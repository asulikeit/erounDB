//! PostgreSQL 문법 → SQLite 실행 문장 변환 및 jamsilDB 규칙 검증.
//!
//! * TEXT 컬럼에 대한 LIKE/ILIKE 거부 (JDB-4102)
//! * TEXT_MATCH / TEXT_SCORE → 검색 후보 임시 테이블 조인으로 재작성
//! * PG 캐스트(`::`), 스키마 한정자, 문자열 리터럴, DDL 타입 매핑
//! * 복제 가능한 DDL 제약 (PRIMARY KEY 필수, PK NOT NULL, DECIMAL 미지원 등)

use std::collections::HashMap;
use std::ops::ControlFlow;

use sqlparser::ast::{
    AlterTableOperation, CastKind, ColumnDef, ColumnOption, ColumnOptionDef, CreateTable, DataType,
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName, ObjectType,
    Query, Statement, TableConstraint, TableFactor, TrimWhereField, Value as SqlValue, Visit,
    VisitMut, Visitor, VisitorMut,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use jamsil_common::DbError;

use crate::classify::{kind_of, parse_statements, StmtKind};
use crate::search_ddl::{parse_search_ddl, strip_search_consistency, SearchDdl};
use crate::scope::Scope;
use crate::types::{decl_kind, oid, type_oid_by_name, ValueKind};

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub decl: String,
    pub pk: bool,
    pub notnull: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchIndexRef {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub pk_columns: Vec<String>,
}

/// 변환기가 참조하는 스키마 정보.
pub trait Catalog {
    fn columns(&self, table: &str) -> Option<Vec<ColumnInfo>>;
    fn search_indexes(&self, table: &str) -> Vec<SearchIndexRef>;
    fn search_index_by_name(&self, name: &str) -> Option<SearchIndexRef>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum TextMatchQuery {
    Literal(String),
    /// 1-based PG 파라미터 번호.
    Param(usize),
}

#[derive(Debug, Clone)]
pub struct TextMatch {
    pub index: SearchIndexRef,
    /// TEXT_MATCH 첫 인자 컬럼 (다중 필드 인덱스에서 이 필드만 검색)
    pub column: String,
    pub query: TextMatchQuery,
    /// 후보 Row PK와 점수를 채울 임시 테이블 이름 (temp 스키마).
    pub temp_table: String,
}

#[derive(Debug, Clone)]
pub struct Translated {
    /// 실행할 SQLite 문장들 (DDL은 여러 개로 나뉠 수 있음).
    pub statements: Vec<String>,
    pub kind: StmtKind,
    /// CommandComplete 태그 접두 (SELECT/INSERT/UPDATE/DELETE) 또는 고정 태그.
    pub tag: String,
    pub text_matches: Vec<TextMatch>,
    pub search_ddl: Option<SearchDdl>,
    /// DROP TABLE 대상 (검색 인덱스 정의 정리용).
    pub dropped_tables: Vec<String>,
    /// (old, new) 테이블 이름 변경.
    pub renamed_tables: Vec<(String, String)>,
    /// 결과 컬럼 타입 힌트 (최상위 SELECT 투영 순서; 와일드카드가 있으면 비움).
    pub column_type_hints: Vec<Option<u32>>,
    pub search_committed: Option<bool>,
    pub notices: Vec<String>,
}

pub struct Translator<'a> {
    pub catalog: &'a dyn Catalog,
    /// 임시 테이블 이름 유일화를 위한 접두.
    pub temp_prefix: String,
    /// 실행 사용자 권한 (None = 내부 호출, 검사 생략)
    pub principal: Option<&'a jamsil_common::acl::Principal>,
}

const RESERVED_PREFIXES: &[&str] = &[
    "_jamsil",
    "sqlite_",
    "pg_",
    "information_schema",
    "jamsil_catalog",
];

fn dialect() -> PostgreSqlDialect {
    PostgreSqlDialect {}
}

fn parse_expr(s: &str) -> Result<Expr, DbError> {
    Parser::new(&dialect())
        .try_with_sql(s)
        .and_then(|mut p| p.parse_expr())
        .map_err(|e| DbError::internal(format!("rewrite failed for `{s}`: {e}")))
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn quote_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// 따옴표 없는 식별자는 PG처럼 소문자로 접는다.
fn fold(i: &Ident) -> String {
    if i.quote_style.is_some() {
        i.value.clone()
    } else {
        i.value.to_ascii_lowercase()
    }
}

fn fold_ident(i: &mut Ident) {
    if i.quote_style.is_none() {
        i.value = i.value.to_ascii_lowercase();
    }
}

fn check_user_name(name: &str, what: &str) -> Result<(), DbError> {
    let l = name.to_ascii_lowercase();
    if RESERVED_PREFIXES.iter().any(|p| l.starts_with(p)) {
        return Err(DbError::new(
            "",
            "42939",
            format!("{what} name \"{name}\" is reserved"),
        ));
    }
    Ok(())
}

/// 스키마 한정자를 제거한 테이블 이름. 지원 스키마: public, pg_catalog, information_schema, jamsil_catalog.
fn normalize_relation(o: &mut ObjectName) -> Result<(), DbError> {
    let parts: Vec<String> = o.0.iter().map(fold).collect();
    let (schema, rel) = match parts.len() {
        1 => return Ok(()),
        2 => (parts[0].clone(), o.0[1].clone()),
        3 => (parts[1].clone(), o.0[2].clone()),
        _ => return Err(DbError::syntax(format!("improper qualified name: {o}"))),
    };
    let new = match schema.as_str() {
        "public" | "pg_catalog" | "main" => rel,
        "information_schema" => Ident::new(format!("information_schema_{}", fold(&rel))),
        "jamsil_catalog" => Ident::new(format!("jamsil_catalog_{}", fold(&rel))),
        "temp" => return Ok(()),
        s => {
            return Err(DbError::new(
                "",
                "3F000",
                format!("schema \"{s}\" does not exist"),
            ))
        }
    };
    o.0 = vec![new];
    Ok(())
}

fn relation_name(o: &ObjectName) -> String {
    o.0.last().map(fold).unwrap_or_default()
}

/// 쿼리에 등장하는 테이블 참조 (이름, 별칭).
#[derive(Default)]
struct TableRefs {
    refs: Vec<(String, Option<String>)>,
}

impl Visitor for TableRefs {
    type Break = ();
    fn pre_visit_table_factor(&mut self, tf: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table { name, alias, .. } = tf {
            let mut n = name.clone();
            let _ = normalize_relation(&mut n);
            self.refs
                .push((relation_name(&n), alias.as_ref().map(|a| fold(&a.name))));
        }
        ControlFlow::Continue(())
    }
}

/// 문장 안에서 정의된 CTE 이름 (권한 검사에서 테이블로 보지 않는다).
fn cte_names(stmt: &Statement) -> Vec<String> {
    struct C(Vec<String>);
    impl Visitor for C {
        type Break = ();
        fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
            if let Some(w) = &q.with {
                for c in &w.cte_tables {
                    self.0.push(fold(&c.alias.name));
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut c = C(vec![]);
    let _ = stmt.visit(&mut c);
    c.0
}

fn func_name(f: &Function) -> String {
    f.name
        .0
        .last()
        .map(|i| i.value.to_ascii_lowercase())
        .unwrap_or_default()
}

fn func_schema(f: &Function) -> Option<String> {
    if f.name.0.len() > 1 {
        Some(f.name.0[0].value.to_ascii_lowercase())
    } else {
        None
    }
}

fn func_args(f: &Function) -> Vec<&Expr> {
    match &f.args {
        FunctionArguments::List(l) => l
            .args
            .iter()
            .filter_map(|a| match a {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(e),
                    ..
                } => Some(e),
                _ => None,
            })
            .collect(),
        _ => vec![],
    }
}

/// 컬럼 참조 (qualifier, column).
fn column_ref(e: &Expr) -> Option<(Option<String>, String)> {
    match e {
        Expr::Identifier(i) => Some((None, fold(i))),
        Expr::CompoundIdentifier(v) if v.len() >= 2 => {
            Some((Some(fold(&v[v.len() - 2])), fold(&v[v.len() - 1])))
        }
        Expr::Nested(inner) => column_ref(inner),
        _ => None,
    }
}

/// 문장 분석 단계에서 찾은 TEXT_MATCH.
struct FoundMatch {
    qualifier: String,
    table: String,
    column: String,
    index: SearchIndexRef,
    query: TextMatchQuery,
    temp_table: String,
}

struct Analyzer<'c> {
    catalog: &'c dyn Catalog,
    scope: Scope,
    matches: Vec<FoundMatch>,
    temp_prefix: String,
    error: Option<DbError>,
}

impl<'c> Analyzer<'c> {
    /// 컬럼 참조를 (qualifier로 쓸 이름, 실제 기반 테이블)로 해석.
    fn resolve(&self, q: &Option<String>, col: &str) -> Vec<(String, String)> {
        self.scope
            .resolve(q, col)
            .into_iter()
            .filter_map(|(r, _)| r.table.clone().map(|t| (r.qual.clone(), t)))
            .collect()
    }

    fn text_match(&mut self, f: &Function) -> Result<(), DbError> {
        let args = func_args(f);
        if args.len() != 2 {
            return Err(DbError::syntax(
                "TEXT_MATCH(column, query) requires 2 arguments",
            ));
        }
        let (q, col) = column_ref(args[0]).ok_or_else(|| {
            DbError::syntax("TEXT_MATCH first argument must be a column reference")
        })?;
        let query = match args[1] {
            Expr::Value(SqlValue::SingleQuotedString(s))
            | Expr::Value(SqlValue::EscapedStringLiteral(s))
            | Expr::Value(SqlValue::DollarQuotedString(sqlparser::ast::DollarQuotedString {
                value: s,
                ..
            })) => TextMatchQuery::Literal(s.clone()),
            Expr::Value(SqlValue::Placeholder(p)) => {
                let n: usize = p
                    .trim_start_matches('$')
                    .parse()
                    .map_err(|_| DbError::syntax("TEXT_MATCH query parameter must be $n"))?;
                TextMatchQuery::Param(n)
            }
            Expr::Cast { expr, .. } => match expr.as_ref() {
                Expr::Value(SqlValue::Placeholder(p)) => TextMatchQuery::Param(
                    p.trim_start_matches('$')
                        .parse()
                        .map_err(|_| DbError::syntax("bad parameter"))?,
                ),
                Expr::Value(SqlValue::SingleQuotedString(s)) => TextMatchQuery::Literal(s.clone()),
                _ => {
                    return Err(DbError::syntax(
                        "TEXT_MATCH query must be a string literal or parameter",
                    ))
                }
            },
            _ => {
                return Err(DbError::syntax(
                    "TEXT_MATCH query must be a string literal or parameter",
                ))
            }
        };
        let candidates = self.resolve(&q, &col);
        let mut found = None;
        for (qual, table) in candidates {
            for idx in self.catalog.search_indexes(&table) {
                if idx.columns.iter().any(|c| c.eq_ignore_ascii_case(&col)) {
                    found = Some((qual.clone(), table.clone(), idx));
                    break;
                }
            }
            if found.is_some() {
                break;
            }
        }
        let Some((qualifier, table, index)) = found else {
            let what = match &q {
                Some(q) => format!("{q}.{col}"),
                None => col.clone(),
            };
            return Err(DbError::search_index_missing(format!(
                "no search index covers column {what}"
            )));
        };
        let temp_table = format!("{}_{}", self.temp_prefix, self.matches.len());
        self.matches.push(FoundMatch {
            qualifier,
            table,
            column: col,
            index,
            query,
            temp_table,
        });
        Ok(())
    }
}

impl<'c> Visitor for Analyzer<'c> {
    type Break = ();
    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        let r: Result<(), DbError> = (|| {
            match e {
                Expr::Like { expr, .. }
                | Expr::ILike { expr, .. }
                | Expr::SimilarTo { expr, .. } => {
                    // TEXT 컬럼을 참조하는 모든 표현식(함수·CAST·별칭·CTE·파생 테이블·뷰)에 적용
                    if self.scope.derives_text(expr) {
                        return Err(DbError::like_on_text());
                    }
                }
                Expr::Function(f) if func_name(f) == "text_match" => self.text_match(f)?,
                _ => {}
            }
            Ok(())
        })();
        if let Err(err) = r {
            self.error = Some(err);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

/// AST 재작성기.
struct Rewriter<'m> {
    matches: &'m [FoundMatch],
    scope: Option<&'m Scope>,
    error: Option<DbError>,
}

fn is_default_kw(e: &Expr) -> bool {
    matches!(e, Expr::Identifier(i) if i.quote_style.is_none() && i.value.eq_ignore_ascii_case("default"))
}

/// 값 표현식을 정규화 함수로 감싼다 (이미 감싸져 있으면 그대로).
fn wrap_norm(x: &mut Expr, f: &str) -> Result<(), DbError> {
    if is_default_kw(x) {
        return Ok(());
    }
    if let Expr::Function(func) = x {
        if func_name(func) == f {
            return Ok(());
        }
    }
    *x = parse_expr(&format!("{f}({x})"))?;
    Ok(())
}

impl<'m> Rewriter<'m> {
    /// 정규화 대상 타입 컬럼과 비교되는 값을 같은 표현으로 바꾼다 (예: TIMESTAMPTZ는 UTC).
    fn normalize_compare(&self, e: &mut Expr) -> Result<(), DbError> {
        let Some(scope) = self.scope else { return Ok(()) };
        let typed = |x: &Expr| scope.ref_kind(x).and_then(|k| k.normalizer());
        use sqlparser::ast::BinaryOperator as B;
        match e {
            Expr::BinaryOp { left, op, right }
                if matches!(op, B::Eq | B::NotEq | B::Lt | B::LtEq | B::Gt | B::GtEq) =>
            {
                if let Some(f) = typed(left) {
                    if scope.ref_kind(right).is_none() {
                        wrap_norm(right, f)?;
                    }
                } else if let Some(f) = typed(right) {
                    if scope.ref_kind(left).is_none() {
                        wrap_norm(left, f)?;
                    }
                }
            }
            Expr::InList { expr, list, .. } => {
                if let Some(f) = typed(expr) {
                    for x in list.iter_mut() {
                        if scope.ref_kind(x).is_none() {
                            wrap_norm(x, f)?;
                        }
                    }
                }
            }
            Expr::Between { expr, low, high, .. } => {
                if let Some(f) = typed(expr) {
                    if scope.ref_kind(low).is_none() {
                        wrap_norm(low, f)?;
                    }
                    if scope.ref_kind(high).is_none() {
                        wrap_norm(high, f)?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn find_match(&self, e: &Expr) -> Option<&FoundMatch> {
        let (q, col) = column_ref(e)?;
        self.matches.iter().find(|m| {
            m.column.eq_ignore_ascii_case(&col)
                && match &q {
                    Some(q) => q == &m.qualifier || q == &m.table,
                    None => true,
                }
        })
    }

    fn pk_expr(m: &FoundMatch) -> String {
        let cols: Vec<String> = m
            .index
            .pk_columns
            .iter()
            .map(|c| format!("{}.{}", quote_ident(&m.qualifier), quote_ident(c)))
            .collect();
        if cols.len() == 1 {
            cols[0].clone()
        } else {
            format!("({})", cols.join(", "))
        }
    }

    fn rewrite(&mut self, e: &mut Expr) -> Result<(), DbError> {
        let is_all = matches!(e, Expr::AllOp { .. });
        let replacement: Option<String> = match e {
            Expr::Cast { kind, expr, data_type, .. } => {
                if *kind == CastKind::TryCast || *kind == CastKind::SafeCast {
                    return Err(DbError::unsupported("TRY_CAST"));
                }
                let t = data_type.to_string().to_ascii_lowercase();
                let base = t.split('(').next().unwrap_or("").trim().to_string();
                let inner = expr.to_string();
                Some(match base.as_str() {
                    "regclass" => format!(
                        "(SELECT oid FROM pg_class WHERE relname = lower(trim(replace(replace({inner}, 'public.', ''), '\"', ''))))"
                    ),
                    "regtype" => format!("_jamsil_regtype({inner})"),
                    "regproc" | "regprocedure" | "regnamespace" | "regrole" => format!("CAST({inner} AS TEXT)"),
                    "oid" | "int" | "integer" | "int4" | "int8" | "bigint" | "smallint" | "int2" => {
                        format!("CAST({inner} AS INTEGER)")
                    }
                    "real" | "float4" | "float8" | "double precision" | "float" | "double" => {
                        format!("CAST({inner} AS REAL)")
                    }
                    "numeric" | "decimal" => format!("CAST({inner} AS NUMERIC)"),
                    "bool" | "boolean" => format!("_jamsil_bool({inner})"),
                    "bytea" | "blob" => format!("CAST({inner} AS BLOB)"),
                    "text" | "varchar" | "character varying" | "char" | "character" | "bpchar" | "name"
                    | "date" | "time" | "timestamp" | "timestamptz" | "timestamp with time zone"
                    | "timestamp without time zone" | "uuid" | "json" | "jsonb" | "\"char\"" => {
                        format!("CAST({inner} AS TEXT)")
                    }
                    "text[]" | "name[]" | "oid[]" | "int[]" | "int2[]" | "int4[]" | "_text" => inner.clone(),
                    other => {
                        if other.ends_with("[]") {
                            inner.clone()
                        } else {
                            return Err(DbError::unsupported(format!("cast to {other}")));
                        }
                    }
                })
            }
            Expr::Function(f) => {
                let name = func_name(f);
                if let Some(s) = func_schema(f) {
                    if s == "pg_catalog" || s == "public" || s == "information_schema" {
                        f.name = ObjectName(vec![f.name.0.last().unwrap().clone()]);
                    }
                }
                let args: Vec<String> = func_args(f).iter().map(|a| a.to_string()).collect();
                // 괄호 없는 SQL 특수 값과 그 함수형: 파서가 다시 키워드 형태로 출력하므로 내부 UDF 이름으로 바꾼다.
                if matches!(
                    name.as_str(),
                    "current_user" | "session_user" | "current_schema" | "current_catalog" | "user" | "current_role"
                ) && args.is_empty()
                {
                    let udf = match name.as_str() {
                        "current_schema" => "_jamsil_current_schema",
                        "current_catalog" => "_jamsil_current_catalog",
                        _ => "_jamsil_user",
                    };
                    return {
                        *e = parse_expr(&format!("{udf}()"))?;
                        Ok(())
                    };
                }
                match name.as_str() {
                    "text_match" => {
                        let m = self
                            .find_match(func_args(f)[0])
                            .ok_or_else(|| DbError::internal("TEXT_MATCH lost during rewrite"))?;
                        let cols: Vec<String> =
                            (0..m.index.pk_columns.len()).map(|i| format!("c{i}")).collect();
                        Some(format!(
                            "{} IN (SELECT {} FROM temp.{})",
                            Self::pk_expr(m),
                            cols.join(", "),
                            quote_ident(&m.temp_table)
                        ))
                    }
                    "text_score" => {
                        let a = func_args(f);
                        if a.len() != 1 {
                            return Err(DbError::syntax("TEXT_SCORE(column) requires 1 argument"));
                        }
                        let m = self.find_match(a[0]).ok_or_else(|| {
                            DbError::syntax("TEXT_SCORE(column) requires a TEXT_MATCH on the same column")
                        })?;
                        let conds: Vec<String> = m
                            .index
                            .pk_columns
                            .iter()
                            .enumerate()
                            .map(|(i, c)| {
                                format!("_s.c{i} = {}.{}", quote_ident(&m.qualifier), quote_ident(c))
                            })
                            .collect();
                        Some(format!(
                            "(SELECT _s.score FROM temp.{} AS _s WHERE {})",
                            quote_ident(&m.temp_table),
                            conds.join(" AND ")
                        ))
                    }
                    "char_length" | "character_length" => Some(format!("length({})", args.join(", "))),
                    "string_agg" if args.len() == 2 => Some(format!("group_concat({}, {})", args[0], args[1])),
                    "strpos" if args.len() == 2 => Some(format!("instr({}, {})", args[0], args[1])),
                    "btrim" => Some(format!("trim({})", args.join(", "))),
                    "array_agg" => Some(format!("_jamsil_array_agg({})", args.join(", "))),
                    "nextval" | "setval" | "currval" => {
                        return Err(DbError::unsupported("sequences (use SERIAL / INTEGER PRIMARY KEY)"))
                    }
                    _ => None,
                }
            }
            Expr::Position { expr, r#in } => Some(format!("instr({}, {})", r#in, expr)),
            Expr::Substring { expr, substring_from, substring_for, .. } => {
                let from = substring_from.as_ref().map(|e| e.to_string()).unwrap_or_else(|| "1".into());
                Some(match substring_for {
                    Some(n) => format!("substr({expr}, {from}, {n})"),
                    None => format!("substr({expr}, {from})"),
                })
            }
            Expr::Trim { expr, trim_where, trim_what, trim_characters } => {
                let chars = if let Some(chars) = trim_characters {
                    Some(chars.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(" || "))
                } else {
                    trim_what.as_ref().map(|w| w.to_string())
                };
                let arg = |f: &str| match &chars {
                    Some(c) => format!("{f}({expr}, {c})"),
                    None => format!("{f}({expr})"),
                };
                Some(match trim_where {
                    Some(TrimWhereField::Leading) => arg("ltrim"),
                    Some(TrimWhereField::Trailing) => arg("rtrim"),
                    _ => match &chars {
                        Some(c) => format!("rtrim(ltrim({expr}, {c}), {c})"),
                        None => format!("rtrim(ltrim({expr}))"),
                    },
                })
            }
            Expr::Extract { field, expr, .. } => {
                Some(format!("_jamsil_extract('{}', {expr})", field.to_string().to_ascii_lowercase()))
            }
            Expr::BinaryOp { left, op, right } => {
                use sqlparser::ast::BinaryOperator as B;
                match op {
                    B::PGRegexMatch => Some(format!("regexp({right}, {left})")),
                    B::PGRegexIMatch => Some(format!("_jamsil_regexp_i({right}, {left})")),
                    B::PGRegexNotMatch => Some(format!("(NOT regexp({right}, {left}))")),
                    B::PGRegexNotIMatch => Some(format!("(NOT _jamsil_regexp_i({right}, {left}))")),
                    _ => None,
                }
            }
            Expr::Subscript { expr, subscript } => match subscript.as_ref() {
                sqlparser::ast::Subscript::Index { index } => Some(format!("_jamsil_arr_get({expr}, {index})")),
                _ => return Err(DbError::unsupported("array slices")),
            },
            Expr::IsDistinctFrom(a, b) => Some(format!("(CASE WHEN {a} IS NULL AND {b} IS NULL THEN 0 WHEN {a} IS NULL OR {b} IS NULL THEN 1 WHEN {a} = {b} THEN 0 ELSE 1 END)")),
            Expr::IsNotDistinctFrom(a, b) => Some(format!("(CASE WHEN {a} IS NULL AND {b} IS NULL THEN 1 WHEN {a} IS NULL OR {b} IS NULL THEN 0 WHEN {a} = {b} THEN 1 ELSE 0 END)")),
            Expr::ILike { negated, expr, pattern, escape_char, .. } => {
                let not = if *negated { "NOT " } else { "" };
                let esc = escape_char.as_ref().map(|c| format!(" ESCAPE {}", quote_lit(c))).unwrap_or_default();
                Some(format!("lower({expr}) {not}LIKE lower({pattern}){esc}"))
            }
            Expr::SimilarTo { .. } => return Err(DbError::unsupported("SIMILAR TO")),
            Expr::AnyOp { left, compare_op, right, .. } | Expr::AllOp { left, compare_op, right } => {
                let items = match right.as_ref() {
                    Expr::Array(a) => a.elem.iter().map(|x| x.to_string()).collect::<Vec<_>>(),
                    Expr::Nested(inner) => match inner.as_ref() {
                        Expr::Array(a) => a.elem.iter().map(|x| x.to_string()).collect(),
                        _ => return Err(DbError::unsupported("ANY/ALL with non-literal array")),
                    },
                    _ => return Err(DbError::unsupported("ANY/ALL with non-literal array")),
                };
                let op = compare_op.to_string();
                Some(match (op.as_str(), is_all) {
                    ("=", false) if !items.is_empty() => format!("{left} IN ({})", items.join(", ")),
                    ("<>", true) | ("!=", true) if !items.is_empty() => {
                        format!("{left} NOT IN ({})", items.join(", "))
                    }
                    (_, _) if items.is_empty() => {
                        if is_all { "1".into() } else { "0".into() }
                    }
                    (op, all) => {
                        let j = if all { " AND " } else { " OR " };
                        format!(
                            "({})",
                            items.iter().map(|i| format!("{left} {op} {i}")).collect::<Vec<_>>().join(j)
                        )
                    }
                })
            }
            Expr::Value(v) => {
                match v {
                    SqlValue::EscapedStringLiteral(s) => *v = SqlValue::SingleQuotedString(s.clone()),
                    SqlValue::DollarQuotedString(d) => *v = SqlValue::SingleQuotedString(d.value.clone()),
                    SqlValue::NationalStringLiteral(s) => *v = SqlValue::SingleQuotedString(s.clone()),
                    SqlValue::HexStringLiteral(h) => {
                        let h = h.clone();
                        return {
                            *e = parse_expr(&format!("X'{h}'"))?;
                            Ok(())
                        };
                    }
                    _ => {}
                }
                None
            }
            Expr::TypedString { data_type, value } => {
                let t = data_type.to_string().to_ascii_lowercase();
                if t.starts_with("interval") {
                    return Err(DbError::unsupported("INTERVAL"));
                }
                Some(quote_lit(value))
            }
            Expr::Interval(_) => return Err(DbError::unsupported("INTERVAL")),
            Expr::AtTimeZone { .. } => return Err(DbError::unsupported("AT TIME ZONE")),
            Expr::Collate { expr, collation } => {
                let c = collation.to_string().trim_matches('"').to_ascii_lowercase();
                let c = match c.as_str() {
                    "c" | "posix" | "binary" | "default" | "ucs_basic" => "BINARY",
                    "nocase" => "NOCASE",
                    _ => return Err(DbError::unsupported(format!("collation {collation}"))),
                };
                Some(format!("{expr} COLLATE {c}"))
            }
            _ => None,
        };
        if let Some(r) = replacement {
            *e = parse_expr(&r)?;
        }
        Ok(())
    }
}

impl<'m> VisitorMut for Rewriter<'m> {
    type Break = ();

    fn post_visit_relation(&mut self, r: &mut ObjectName) -> ControlFlow<()> {
        if let Err(e) = normalize_relation(r) {
            self.error = Some(e);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        if let Err(err) = self.normalize_compare(e).and_then(|_| self.rewrite(e)) {
            self.error = Some(err);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        // FOR UPDATE/SHARE: 단일 Writer 직렬화로 대체.
        q.locks.clear();
        if let Some(f) = q.fetch.take() {
            if f.percent || f.with_ties {
                self.error = Some(DbError::unsupported("FETCH ... PERCENT / WITH TIES"));
                return ControlFlow::Break(());
            }
            if q.limit.is_none() {
                q.limit = Some(
                    f.quantity
                        .unwrap_or(Expr::Value(SqlValue::Number("1".into(), false))),
                );
            }
        }
        if let Some(o) = q.offset.as_mut() {
            o.rows = sqlparser::ast::OffsetRows::None;
        }
        if q.offset.is_some() && q.limit.is_none() {
            q.limit = Some(Expr::Value(SqlValue::Number("-1".into(), false)));
        }
        ControlFlow::Continue(())
    }
}

/// 결과 컬럼 타입 힌트 (와일드카드 없는 최상위 SELECT).
fn projection_hints(stmt: &Statement) -> Vec<Option<u32>> {
    let Statement::Query(q) = stmt else {
        return vec![];
    };
    let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
        return vec![];
    };
    let mut out = vec![];
    for item in &sel.projection {
        let e = match item {
            sqlparser::ast::SelectItem::UnnamedExpr(e) => e,
            sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => return vec![],
        };
        out.push(expr_type_hint(e));
    }
    out
}

fn expr_type_hint(e: &Expr) -> Option<u32> {
    match e {
        Expr::Cast { data_type, .. } => {
            let t = data_type.to_string();
            let base = t.split('(').next().unwrap_or("").trim().to_string();
            type_oid_by_name(&base).or(match base.to_ascii_lowercase().as_str() {
                "regclass" | "regtype" | "regproc" => Some(oid::OID),
                _ => None,
            })
        }
        Expr::Nested(i) => expr_type_hint(i),
        Expr::Function(f) => match func_name(f).as_str() {
            "count" => Some(oid::INT8),
            "now"
            | "current_timestamp"
            | "transaction_timestamp"
            | "statement_timestamp"
            | "clock_timestamp" => Some(oid::TIMESTAMPTZ),
            "text_score" => Some(oid::FLOAT4),
            "version" | "current_database" | "current_schema" | "current_user" | "session_user"
            | "_jamsil_user" | "_jamsil_current_schema" | "_jamsil_current_catalog"
            | "upper" | "lower" | "concat" | "format_type" | "pg_get_expr" | "obj_description"
            | "col_description" => Some(oid::TEXT),
            "length" | "char_length" | "octet_length" | "pg_backend_pid" => Some(oid::INT4),
            "gen_random_uuid" => Some(oid::UUID),
            _ => None,
        },
        Expr::Value(SqlValue::Boolean(_)) => Some(oid::BOOL),
        Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::Exists { .. }
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::Between { .. }
        | Expr::Like { .. } => Some(oid::BOOL),
        Expr::BinaryOp { op, .. } => {
            use sqlparser::ast::BinaryOperator::*;
            match op {
                Eq | NotEq | Lt | LtEq | Gt | GtEq | And | Or => Some(oid::BOOL),
                StringConcat => Some(oid::TEXT),
                _ => None,
            }
        }
        Expr::Value(SqlValue::SingleQuotedString(_)) => Some(oid::TEXT),
        _ => None,
    }
}

impl<'a> Translator<'a> {
    pub fn new(catalog: &'a dyn Catalog, temp_prefix: impl Into<String>) -> Self {
        Translator {
            catalog,
            temp_prefix: temp_prefix.into(),
            principal: None,
        }
    }

    pub fn with_principal(mut self, p: Option<&'a jamsil_common::acl::Principal>) -> Self {
        self.principal = p;
        self
    }

    /// 단일 문장 변환. 입력은 classify()가 분리한 문장 하나.
    pub fn translate(&self, sql: &str) -> Result<Translated, DbError> {
        if let Some(r) = parse_search_ddl(sql) {
            if let Some(p) = self.principal {
                p.check_ddl()?;
            }
            let d = r?;
            return Ok(Translated {
                statements: vec![],
                kind: StmtKind::Ddl,
                tag: match &d {
                    SearchDdl::Create(_) => "CREATE SEARCH INDEX".into(),
                    SearchDdl::Drop(_) => "DROP SEARCH INDEX".into(),
                },
                text_matches: vec![],
                search_ddl: Some(d),
                dropped_tables: vec![],
                renamed_tables: vec![],
                column_type_hints: vec![],
                search_committed: None,
                notices: vec![],
            });
        }
        let (body, committed) = strip_search_consistency(sql);
        let mut stmts = parse_statements(&body)?;
        if stmts.len() != 1 {
            return Err(DbError::syntax("expected exactly one statement"));
        }
        let mut stmt = stmts.pop().unwrap();
        let kind = kind_of(&stmt);
        let mut out = Translated {
            statements: vec![],
            kind: kind.clone(),
            tag: String::new(),
            text_matches: vec![],
            search_ddl: None,
            dropped_tables: vec![],
            renamed_tables: vec![],
            column_type_hints: vec![],
            search_committed: committed,
            notices: vec![],
        };
        if let StmtKind::Unsupported(what) = &kind {
            return Err(DbError::unsupported(what.clone()));
        }

        // 1) 테이블 참조 수집 및 시스템 테이블 쓰기 금지
        let mut refs = TableRefs::default();
        let _ = stmt.visit(&mut refs);
        if kind == StmtKind::Write {
            let mut targets = vec![];
            match &stmt {
                Statement::Insert(i) => {
                    let mut n = i.table_name.clone();
                    normalize_relation(&mut n)?;
                    targets.push(relation_name(&n));
                }
                _ => targets.extend(refs.refs.iter().map(|r| r.0.clone())),
            }
            for t in targets {
                if RESERVED_PREFIXES.iter().any(|p| t.starts_with(p)) {
                    return Err(DbError::new(
                        "",
                        "42501",
                        format!("permission denied for relation {t}"),
                    ));
                }
            }
        }

        // 1-1) 사용자 권한 (RQ-13)
        if let Some(p) = self.principal.filter(|p| !p.is_superuser()) {
            match &kind {
                StmtKind::Ddl => p.check_ddl()?,
                StmtKind::Write => {
                    let target = match &stmt {
                        Statement::Insert(i) => {
                            let mut n = i.table_name.clone();
                            normalize_relation(&mut n)?;
                            Some(relation_name(&n))
                        }
                        Statement::Update { table, .. } => match &table.relation {
                            TableFactor::Table { name, .. } => {
                                let mut n = name.clone();
                                normalize_relation(&mut n)?;
                                Some(relation_name(&n))
                            }
                            _ => None,
                        },
                        _ => None,
                    };
                    match target {
                        Some(t) => p.check_write(&t)?,
                        // DELETE 등: 참조하는 모든 테이블에 쓰기 권한 요구 (보수적)
                        None => {
                            for (t, _) in &refs.refs {
                                p.check_write(t)?;
                            }
                        }
                    }
                }
                _ => {}
            }
            let ctes = cte_names(&stmt);
            for (t, _) in &refs.refs {
                if !ctes.contains(t) {
                    p.check_read(t)?;
                }
            }
        }

        // 2) 분석: TEXT LIKE 금지, TEXT_MATCH 해석
        let scope = Scope::build_statement(&stmt, self.catalog);
        let mut an = Analyzer {
            catalog: self.catalog,
            scope: scope.clone(),
            matches: vec![],
            temp_prefix: self.temp_prefix.clone(),
            error: None,
        };
        let _ = stmt.visit(&mut an);
        if let Some(e) = an.error {
            return Err(e);
        }
        if !an.matches.is_empty() && kind != StmtKind::Read {
            return Err(DbError::unsupported(
                "TEXT_MATCH in data-modifying statements",
            ));
        }

        // 3) DDL 특수 처리
        if kind == StmtKind::Ddl {
            self.translate_ddl(stmt, &mut out)?;
            return Ok(out);
        }

        // 4) 표현식/관계 재작성
        // 괄호 없는 current_user 등은 PG처럼 결과 컬럼 이름을 유지한다.
        struct AliasSessionFns;
        impl VisitorMut for AliasSessionFns {
            type Break = ();
            fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
                if let sqlparser::ast::SetExpr::Select(sel) = q.body.as_mut() {
                    for item in sel.projection.iter_mut() {
                        let replace = match &*item {
                            sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(f)) => {
                                let n = func_name(f);
                                let special = matches!(
                                    n.as_str(),
                                    "current_user" | "session_user" | "current_schema" | "current_catalog" | "user" | "current_role"
                                ) && func_args(f).is_empty();
                                special.then(|| (Expr::Function(f.clone()), n))
                            }
                            _ => None,
                        };
                        if let Some((expr, n)) = replace {
                            *item = sqlparser::ast::SelectItem::ExprWithAlias {
                                expr,
                                alias: Ident::new(n),
                            };
                        }
                    }
                }
                ControlFlow::Continue(())
            }
        }
        let _ = VisitMut::visit(&mut stmt, &mut AliasSessionFns);
        let mut rw = Rewriter {
            matches: &an.matches,
            scope: Some(&scope),
            error: None,
        };
        let _ = VisitMut::visit(&mut stmt, &mut rw);
        if let Some(e) = rw.error {
            return Err(e);
        }
        if kind == StmtKind::Write {
            self.normalize_writes(&mut stmt)?;
        }
        out.column_type_hints = projection_hints(&stmt);
        out.text_matches = an
            .matches
            .into_iter()
            .map(|m| TextMatch {
                column: m.column,
                index: m.index,
                query: m.query,
                temp_table: m.temp_table,
            })
            .collect();
        let (sql, tag) = match &stmt {
            Statement::Explain { statement, .. } => (
                format!("EXPLAIN QUERY PLAN {statement}"),
                "EXPLAIN".to_string(),
            ),
            Statement::Query(_) => (stmt.to_string(), "SELECT".into()),
            Statement::Insert(_) => (stmt.to_string(), "INSERT".into()),
            Statement::Update { .. } => (stmt.to_string(), "UPDATE".into()),
            Statement::Delete(d) => {
                if d.using.is_some() {
                    return Err(DbError::unsupported("DELETE ... USING"));
                }
                (stmt.to_string(), "DELETE".into())
            }
            _ => (stmt.to_string(), "SELECT".into()),
        };
        out.statements.push(sql);
        out.tag = tag;
        Ok(out)
    }

    /// INSERT VALUES / UPDATE SET / ON CONFLICT DO UPDATE 의 대상 컬럼이 정규화 타입이면 값을 감싼다.
    fn normalize_writes(&self, stmt: &mut Statement) -> Result<(), DbError> {
        use sqlparser::ast::{Assignment, AssignmentTarget, OnConflictAction, OnInsert};
        let kinds_of = |table: &str| -> Option<Vec<(String, ValueKind)>> {
            self.catalog.columns(table).map(|cols| {
                cols.iter()
                    .map(|c| (c.name.to_ascii_lowercase(), decl_kind(&c.decl)))
                    .collect()
            })
        };
        let wrap_assignments = |assigns: &mut Vec<Assignment>, kinds: &[(String, ValueKind)]| -> Result<(), DbError> {
            for a in assigns.iter_mut() {
                if let AssignmentTarget::ColumnName(n) = &a.target {
                    let col = relation_name(n);
                    if let Some((_, k)) = kinds.iter().find(|(c, _)| *c == col) {
                        if let Some(f) = k.normalizer() {
                            wrap_norm(&mut a.value, f)?;
                        }
                    }
                }
            }
            Ok(())
        };
        match stmt {
            Statement::Insert(ins) => {
                let table = relation_name(&ins.table_name);
                let Some(kinds) = kinds_of(&table) else { return Ok(()) };
                let targets: Vec<String> = if ins.columns.is_empty() {
                    kinds.iter().map(|(c, _)| c.clone()).collect()
                } else {
                    ins.columns.iter().map(fold).collect()
                };
                let funcs: Vec<Option<&'static str>> = targets
                    .iter()
                    .map(|t| kinds.iter().find(|(c, _)| c == t).and_then(|(_, k)| k.normalizer()))
                    .collect();
                if let Some(src) = ins.source.as_mut() {
                    if let sqlparser::ast::SetExpr::Values(v) = src.body.as_mut() {
                        for row in v.rows.iter_mut() {
                            for (i, x) in row.iter_mut().enumerate() {
                                if let Some(Some(f)) = funcs.get(i).copied() {
                                    wrap_norm(x, f)?;
                                }
                            }
                        }
                    }
                }
                if let Some(OnInsert::OnConflict(oc)) = ins.on.as_mut() {
                    if let OnConflictAction::DoUpdate(du) = &mut oc.action {
                        wrap_assignments(&mut du.assignments, &kinds)?;
                    }
                }
            }
            Statement::Update { table, assignments, .. } => {
                if let TableFactor::Table { name, .. } = &table.relation {
                    if let Some(kinds) = kinds_of(&relation_name(name)) {
                        wrap_assignments(assignments, &kinds)?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn translate_ddl(&self, mut stmt: Statement, out: &mut Translated) -> Result<(), DbError> {
        // 관계 이름/표현식 정규화 (DDL 내부 DEFAULT/CHECK 등)
        let mut rw = Rewriter {
            matches: &[],
            scope: None,
            error: None,
        };
        let _ = VisitMut::visit(&mut stmt, &mut rw);
        if let Some(e) = rw.error {
            return Err(e);
        }
        match stmt {
            Statement::CreateTable(ct) => {
                out.tag = "CREATE TABLE".into();
                out.statements.push(self.create_table(ct)?);
            }
            Statement::CreateIndex(mut ci) => {
                out.tag = "CREATE INDEX".into();
                if !ci.include.is_empty() {
                    return Err(DbError::unsupported("CREATE INDEX ... INCLUDE"));
                }
                if let Some(u) = &ci.using {
                    if !u.value.eq_ignore_ascii_case("btree") {
                        return Err(DbError::unsupported(format!("index method {}", u.value)));
                    }
                }
                ci.using = None;
                ci.concurrently = false;
                ci.nulls_distinct = None;
                ci.with.clear();
                normalize_relation(&mut ci.table_name)?;
                for i in ci.table_name.0.iter_mut() {
                    fold_ident(i);
                }
                let table = relation_name(&ci.table_name);
                if RESERVED_PREFIXES.iter().any(|p| table.starts_with(p)) {
                    return Err(DbError::new(
                        "",
                        "42501",
                        format!("permission denied for relation {table}"),
                    ));
                }
                match ci.name.as_mut() {
                    Some(n) => {
                        for i in n.0.iter_mut() {
                            fold_ident(i);
                        }
                        check_user_name(&relation_name(n), "index")?;
                    }
                    None => {
                        let cols: Vec<String> = ci
                            .columns
                            .iter()
                            .map(|c| c.expr.to_string().replace(['"', ' ', '(', ')', ','], ""))
                            .collect();
                        ci.name = Some(ObjectName(vec![Ident::new(format!(
                            "{}_{}_idx",
                            table,
                            cols.join("_").to_ascii_lowercase()
                        ))]));
                    }
                }
                out.statements.push(Statement::CreateIndex(ci).to_string());
            }
            Statement::CreateView {
                or_replace,
                materialized,
                name,
                columns,
                query,
                if_not_exists,
                temporary,
                ..
            } => {
                if materialized || temporary {
                    return Err(DbError::unsupported("MATERIALIZED/TEMPORARY VIEW"));
                }
                let mut name = name;
                normalize_relation(&mut name)?;
                for i in name.0.iter_mut() {
                    fold_ident(i);
                }
                check_user_name(&relation_name(&name), "view")?;
                out.tag = "CREATE VIEW".into();
                let cols = if columns.is_empty() {
                    String::new()
                } else {
                    format!(
                        " ({})",
                        columns
                            .iter()
                            .map(|c| quote_ident(&fold(&c.name)))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                if or_replace {
                    out.statements.push(format!("DROP VIEW IF EXISTS {name}"));
                }
                let ine = if if_not_exists { "IF NOT EXISTS " } else { "" };
                out.statements
                    .push(format!("CREATE VIEW {ine}{name}{cols} AS {query}"));
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => {
                let (kw, tag) = match object_type {
                    ObjectType::Table => ("TABLE", "DROP TABLE"),
                    ObjectType::Index => ("INDEX", "DROP INDEX"),
                    ObjectType::View => ("VIEW", "DROP VIEW"),
                    ObjectType::Schema => return Err(DbError::unsupported("DROP SCHEMA")),
                    o => return Err(DbError::unsupported(format!("DROP {o}"))),
                };
                out.tag = tag.into();
                for mut n in names {
                    normalize_relation(&mut n)?;
                    for i in n.0.iter_mut() {
                        fold_ident(i);
                    }
                    let rel = relation_name(&n);
                    if RESERVED_PREFIXES.iter().any(|p| rel.starts_with(p)) {
                        return Err(DbError::new(
                            "",
                            "42501",
                            format!("permission denied for relation {rel}"),
                        ));
                    }
                    if object_type == ObjectType::Table {
                        out.dropped_tables.push(rel);
                    }
                    let ie = if if_exists { "IF EXISTS " } else { "" };
                    out.statements.push(format!("DROP {kw} {ie}{n}"));
                }
            }
            Statement::Truncate { table_names, .. } => {
                out.tag = "TRUNCATE TABLE".into();
                for t in table_names {
                    let mut n = t.name.clone();
                    normalize_relation(&mut n)?;
                    let rel = relation_name(&n);
                    if RESERVED_PREFIXES.iter().any(|p| rel.starts_with(p)) {
                        return Err(DbError::new(
                            "",
                            "42501",
                            format!("permission denied for relation {rel}"),
                        ));
                    }
                    out.statements
                        .push(format!("DELETE FROM {}", quote_ident(&rel)));
                }
                // TRUNCATE는 Row 삭제로 복제되므로 DML로 취급한다.
                out.kind = StmtKind::Write;
            }
            Statement::AlterTable {
                name,
                if_exists,
                operations,
                ..
            } => {
                out.tag = "ALTER TABLE".into();
                let mut name = name;
                normalize_relation(&mut name)?;
                for i in name.0.iter_mut() {
                    fold_ident(i);
                }
                let table = relation_name(&name);
                if RESERVED_PREFIXES.iter().any(|p| table.starts_with(p)) {
                    return Err(DbError::new(
                        "",
                        "42501",
                        format!("permission denied for relation {table}"),
                    ));
                }
                if if_exists && self.catalog.columns(&table).is_none() {
                    out.notices
                        .push(format!("relation \"{table}\" does not exist, skipping"));
                    return Ok(());
                }
                let indexes = self.catalog.search_indexes(&table);
                for op in operations {
                    let qt = quote_ident(&table);
                    match op {
                        AlterTableOperation::AddColumn {
                            if_not_exists,
                            column_def,
                            ..
                        } => {
                            let mut cd = column_def;
                            fold_ident(&mut cd.name);
                            if if_not_exists {
                                if let Some(cols) = self.catalog.columns(&table) {
                                    if cols
                                        .iter()
                                        .any(|c| c.name.eq_ignore_ascii_case(&cd.name.value))
                                    {
                                        out.notices.push(format!(
                                            "column \"{}\" of relation \"{table}\" already exists, skipping",
                                            cd.name.value
                                        ));
                                        continue;
                                    }
                                }
                            }
                            if cd.options.iter().any(|o| {
                                matches!(
                                    o.option,
                                    ColumnOption::Unique {
                                        is_primary: true,
                                        ..
                                    }
                                )
                            }) {
                                return Err(DbError::unsupported("ADD COLUMN ... PRIMARY KEY"));
                            }
                            let cd = map_column(cd, false)?;
                            out.statements
                                .push(format!("ALTER TABLE {qt} ADD COLUMN {cd}"));
                        }
                        AlterTableOperation::DropColumn {
                            column_name,
                            if_exists,
                            ..
                        } => {
                            let c = fold(&column_name);
                            if indexes
                                .iter()
                                .any(|i| i.columns.iter().any(|x| x.eq_ignore_ascii_case(&c)))
                            {
                                return Err(DbError::new(
                                    "",
                                    "2BP01",
                                    format!("column {c} is used by a search index; drop the search index first"),
                                ));
                            }
                            if if_exists {
                                if let Some(cols) = self.catalog.columns(&table) {
                                    if !cols.iter().any(|x| x.name.eq_ignore_ascii_case(&c)) {
                                        continue;
                                    }
                                }
                            }
                            out.statements
                                .push(format!("ALTER TABLE {qt} DROP COLUMN {}", quote_ident(&c)));
                        }
                        AlterTableOperation::RenameColumn {
                            old_column_name,
                            new_column_name,
                        } => {
                            let o = fold(&old_column_name);
                            if indexes
                                .iter()
                                .any(|i| i.columns.iter().any(|x| x.eq_ignore_ascii_case(&o)))
                            {
                                return Err(DbError::new(
                                    "",
                                    "2BP01",
                                    format!("column {o} is used by a search index; drop the search index first"),
                                ));
                            }
                            out.statements.push(format!(
                                "ALTER TABLE {qt} RENAME COLUMN {} TO {}",
                                quote_ident(&o),
                                quote_ident(&fold(&new_column_name))
                            ));
                        }
                        AlterTableOperation::RenameTable { table_name } => {
                            let mut tn = table_name;
                            for i in tn.0.iter_mut() {
                                fold_ident(i);
                            }
                            let new = relation_name(&tn);
                            check_user_name(&new, "table")?;
                            out.statements
                                .push(format!("ALTER TABLE {qt} RENAME TO {}", quote_ident(&new)));
                            out.renamed_tables.push((table.clone(), new));
                        }
                        other => return Err(DbError::unsupported(format!("ALTER TABLE {other}"))),
                    }
                }
            }
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
            } => {
                let n = schema_name.to_string().to_ascii_lowercase();
                if n == "public" && if_not_exists {
                    out.tag = "CREATE SCHEMA".into();
                    out.notices
                        .push("schema \"public\" already exists, skipping".into());
                } else {
                    return Err(DbError::unsupported(
                        "CREATE SCHEMA (only the public schema exists)",
                    ));
                }
            }
            Statement::Comment { .. } => {
                out.tag = "COMMENT".into();
                out.notices
                    .push("COMMENT is accepted but not stored in jamsilDB 1.0".into());
            }
            other => {
                return Err(DbError::unsupported(format!(
                    "DDL: {}",
                    first_words(&other.to_string())
                )))
            }
        }
        Ok(())
    }

    fn create_table(&self, mut ct: CreateTable) -> Result<String, DbError> {
        if ct.temporary {
            return Err(DbError::unsupported(
                "CREATE TEMPORARY TABLE (temporary tables are not replicated)",
            ));
        }
        if ct.query.is_some() {
            return Err(DbError::unsupported(
                "CREATE TABLE AS (tables require an explicit PRIMARY KEY)",
            ));
        }
        if ct.like.is_some() || ct.clone.is_some() {
            return Err(DbError::unsupported("CREATE TABLE LIKE/CLONE"));
        }
        if ct.or_replace {
            return Err(DbError::unsupported("CREATE OR REPLACE TABLE"));
        }
        normalize_relation(&mut ct.name)?;
        for i in ct.name.0.iter_mut() {
            fold_ident(i);
        }
        let table = relation_name(&ct.name);
        check_user_name(&table, "table")?;
        for c in ct.columns.iter_mut() {
            fold_ident(&mut c.name);
        }
        // PK 컬럼 수집
        let mut pk_cols: Vec<String> = vec![];
        for c in &ct.columns {
            if c.options.iter().any(|o| {
                matches!(
                    o.option,
                    ColumnOption::Unique {
                        is_primary: true,
                        ..
                    }
                )
            }) {
                pk_cols.push(c.name.value.clone());
            }
        }
        for con in ct.constraints.iter_mut() {
            match con {
                TableConstraint::PrimaryKey { columns, .. } => {
                    for i in columns.iter_mut() {
                        fold_ident(i);
                    }
                    pk_cols.extend(columns.iter().map(|i| i.value.clone()));
                }
                TableConstraint::Unique { columns, .. } => {
                    for i in columns.iter_mut() {
                        fold_ident(i);
                    }
                }
                TableConstraint::ForeignKey {
                    columns,
                    foreign_table,
                    referred_columns,
                    ..
                } => {
                    for i in columns.iter_mut().chain(referred_columns.iter_mut()) {
                        fold_ident(i);
                    }
                    normalize_relation(foreign_table)?;
                    for i in foreign_table.0.iter_mut() {
                        fold_ident(i);
                    }
                }
                TableConstraint::Check { .. } => {}
                other => return Err(DbError::unsupported(format!("table constraint {other}"))),
            }
        }
        if pk_cols.is_empty() {
            return Err(DbError::unsupported(format!(
                "table \"{table}\" has no PRIMARY KEY; jamsilDB replication requires a PRIMARY KEY on every table"
            ))
            .with_hint("add PRIMARY KEY (e.g. id BIGSERIAL PRIMARY KEY or id BIGINT PRIMARY KEY)"));
        }
        let single_pk = if pk_cols.len() == 1 {
            Some(pk_cols[0].clone())
        } else {
            None
        };
        let cols = std::mem::take(&mut ct.columns);
        let mut mapped = Vec::with_capacity(cols.len());
        for mut c in cols {
            let is_pk = pk_cols.iter().any(|p| p == &c.name.value);
            for o in c.options.iter_mut() {
                if let ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                    ..
                } = &mut o.option
                {
                    normalize_relation(foreign_table)?;
                    for i in foreign_table
                        .0
                        .iter_mut()
                        .chain(referred_columns.iter_mut())
                    {
                        fold_ident(i);
                    }
                }
            }
            let rowid = single_pk.as_deref() == Some(c.name.value.as_str());
            let mut c = map_column(c, rowid)?;
            if is_pk
                && !c
                    .options
                    .iter()
                    .any(|o| matches!(o.option, ColumnOption::NotNull))
            {
                c.options.push(ColumnOptionDef {
                    name: None,
                    option: ColumnOption::NotNull,
                });
            }
            mapped.push(c);
        }
        ct.columns = mapped;
        // SQLite 비지원 옵션 제거
        ct.with_options.clear();
        ct.table_properties.clear();
        ct.on_commit = None;
        Ok(Statement::CreateTable(ct).to_string())
    }
}

fn first_words(s: &str) -> String {
    s.split_whitespace().take(3).collect::<Vec<_>>().join(" ")
}

/// DDL 컬럼 타입 매핑과 타입 계약 제약 (설계서 9.2).
///
/// * 정수형은 `typeof = 'integer'` + 범위 검사: 소수·초과값(SQLite가 REAL로 바꾸는 값)·문자열을 COMMIT 전에 거부.
/// * `INTEGER PRIMARY KEY`도 일반 int4 컬럼으로 만든다 (SQLite rowid 별칭이 되면 NULL 자동 생성·64bit 범위가 노출됨).
/// * rowid 별칭(자동 증가)은 SERIAL 계열에만 쓴다. `rowid_pk`: 단일 컬럼 PK인지.
/// * UUID/DATE/TIME/TIMESTAMP/TIMESTAMPTZ는 정규형 검사 (쓰기 값은 변환기가 정규화 함수로 감싼다).
/// * 제약 이름 `_jamsil_<종류>_<컬럼>` 으로 오류를 SQLSTATE에 대응시킨다.
fn map_column(mut c: ColumnDef, rowid_pk: bool) -> Result<ColumnDef, DbError> {
    let t = c.data_type.to_string().to_ascii_uppercase();
    let (base, args) = match t.find('(') {
        Some(p) => (
            t[..p].trim().to_string(),
            Some(t[p + 1..].trim_end_matches(')').trim().to_string()),
        ),
        None => (t.trim().to_string(), None),
    };
    let col = c.name.value.clone();
    let colq = quote_ident(&col);
    let int_check = |lo: Option<(i64, i64)>| -> String {
        match lo {
            Some((lo, hi)) => format!("{colq} IS NULL OR (typeof({colq}) = 'integer' AND {colq} BETWEEN {lo} AND {hi})"),
            None => format!("{colq} IS NULL OR typeof({colq}) = 'integer'"),
        }
    };
    const I2: (i64, i64) = (-32768, 32767);
    const I4: (i64, i64) = (-2147483648, 2147483647);
    let mut checks: Vec<(&'static str, String)> = vec![];
    let mut kind = ValueKind::Other;
    let decl: String = match base.as_str() {
        "SERIAL" | "SERIAL4" | "BIGSERIAL" | "SERIAL8" | "SMALLSERIAL" | "SERIAL2" => {
            if !rowid_pk {
                return Err(DbError::unsupported(
                    "SERIAL is only supported for a single-column PRIMARY KEY",
                ));
            }
            match base.as_str() {
                "SMALLSERIAL" | "SERIAL2" => checks.push(("int2", int_check(Some((1, I2.1))))),
                "BIGSERIAL" | "SERIAL8" => checks.push(("int8", int_check(None))),
                _ => checks.push(("int4", int_check(Some((1, I4.1))))),
            }
            // SQLite rowid 별칭 (자동 증가)
            "INTEGER".into()
        }
        "INTEGER" | "INT" | "INT4" => {
            checks.push(("int4", int_check(Some(I4))));
            "INT".into()
        }
        "SMALLINT" | "INT2" => {
            checks.push(("int2", int_check(Some(I2))));
            "SMALLINT".into()
        }
        "BIGINT" | "INT8" => {
            checks.push(("int8", int_check(None)));
            "BIGINT".into()
        }
        "REAL" | "FLOAT4" => {
            checks.push(("float", format!("{colq} IS NULL OR typeof({colq}) IN ('real', 'integer')")));
            "REAL".into()
        }
        "DOUBLE PRECISION" | "DOUBLE" | "FLOAT8" | "FLOAT" => {
            checks.push(("float", format!("{colq} IS NULL OR typeof({colq}) IN ('real', 'integer')")));
            "DOUBLE PRECISION".into()
        }
        "BOOLEAN" | "BOOL" => {
            checks.push(("bool", format!("{colq} IN (0, 1)")));
            "BOOLEAN".into()
        }
        "VARCHAR" | "CHARACTER VARYING" | "CHAR" | "CHARACTER" | "BPCHAR" | "NVARCHAR" => {
            let fixed = matches!(base.as_str(), "CHAR" | "CHARACTER" | "BPCHAR");
            let n: Option<u64> = args.as_deref().and_then(|a| a.parse().ok());
            let n = if fixed { Some(n.unwrap_or(1)) } else { n };
            let name = if fixed { "CHAR" } else { "VARCHAR" };
            match n {
                Some(n) => {
                    if n == 0 || n > 10_485_760 {
                        return Err(DbError::invalid_param(format!(
                            "length for type {name} must be 1..10485760"
                        )));
                    }
                    checks.push(("len", format!("length({colq}) <= {n}")));
                    format!("{name}({n})")
                }
                None => name.into(),
            }
        }
        "TEXT" => "TEXT".into(),
        "BYTEA" | "BLOB" => "BYTEA".into(),
        "DATE" => {
            kind = ValueKind::Date;
            "DATE".into()
        }
        "TIME" | "TIME WITHOUT TIME ZONE" => {
            kind = ValueKind::Time;
            "TIME".into()
        }
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => {
            kind = ValueKind::Timestamp;
            "TIMESTAMP".into()
        }
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
            kind = ValueKind::TimestampTz;
            "TIMESTAMPTZ".into()
        }
        "UUID" => {
            kind = ValueKind::Uuid;
            "UUID".into()
        }
        "JSON" | "JSONB" => {
            checks.push(("json", format!("json_valid({colq})")));
            base.clone()
        }
        "NUMERIC" | "DECIMAL" | "DEC" => {
            return Err(DbError::unsupported(
                "DECIMAL/NUMERIC (exact fixed-point is not implemented in 1.0; SQLite REAL substitution is forbidden)",
            )
            .with_hint("use BIGINT in minor units, or DOUBLE PRECISION when approximation is acceptable"))
        }
        other => return Err(DbError::unsupported(format!("data type {other}"))),
    };
    let norm = kind.normalizer();
    if let Some(f) = norm {
        let tag = match kind {
            ValueKind::Uuid => "uuid",
            ValueKind::Date => "date",
            ValueKind::Time => "time",
            ValueKind::Timestamp => "ts",
            _ => "tstz",
        };
        let canon = f.replace("_norm_", "_canon_");
        checks.push((tag, format!("{colq} IS NULL OR {canon}({colq})")));
    }
    c.data_type = DataType::Custom(ObjectName(vec![Ident::new(decl)]), vec![]);
    c.collation = None;
    let mut opts = Vec::with_capacity(c.options.len() + checks.len());
    for mut o in std::mem::take(&mut c.options) {
        match &mut o.option {
            ColumnOption::Default(e) => {
                if let Some(f) = norm {
                    // 기본값도 같은 정규형으로 (예: CURRENT_TIMESTAMP → UTC TIMESTAMPTZ)
                    *e = parse_expr(&format!("{f}({e})"))?;
                }
                // SQLite는 함수 DEFAULT를 괄호로 감싸야 한다.
                if !matches!(e, Expr::Value(_) | Expr::UnaryOp { .. } | Expr::Nested(_)) {
                    *e = Expr::Nested(Box::new(e.clone()));
                }
            }
            ColumnOption::Generated { .. } => {
                return Err(DbError::unsupported("GENERATED columns"))
            }
            ColumnOption::Comment(_) | ColumnOption::CharacterSet(_) => continue,
            ColumnOption::DialectSpecific(_) | ColumnOption::OnUpdate(_) => {
                return Err(DbError::unsupported(format!("column option {}", o.option)))
            }
            _ => {}
        }
        opts.push(o);
    }
    for (tag, ch) in checks {
        opts.push(ColumnOptionDef {
            name: Some(Ident::with_quote('"', format!("_jamsil_{tag}_{col}"))),
            option: ColumnOption::Check(parse_expr(&ch)?),
        });
    }
    c.options = opts;
    Ok(c)
}

/// 테스트·디버깅용: 간단한 메모리 Catalog.
#[derive(Default)]
pub struct MemCatalog {
    pub tables: HashMap<String, Vec<ColumnInfo>>,
    pub indexes: Vec<SearchIndexRef>,
}

impl Catalog for MemCatalog {
    fn columns(&self, table: &str) -> Option<Vec<ColumnInfo>> {
        self.tables.get(table).cloned()
    }
    fn search_indexes(&self, table: &str) -> Vec<SearchIndexRef> {
        self.indexes
            .iter()
            .filter(|i| i.table == table)
            .cloned()
            .collect()
    }
    fn search_index_by_name(&self, name: &str) -> Option<SearchIndexRef> {
        self.indexes.iter().find(|i| i.name == name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cat() -> MemCatalog {
        let mut c = MemCatalog::default();
        c.tables.insert(
            "documents".into(),
            vec![
                ColumnInfo {
                    name: "id".into(),
                    decl: "BIGINT".into(),
                    pk: true,
                    notnull: true,
                },
                ColumnInfo {
                    name: "title".into(),
                    decl: "VARCHAR(200)".into(),
                    pk: false,
                    notnull: false,
                },
                ColumnInfo {
                    name: "content".into(),
                    decl: "TEXT".into(),
                    pk: false,
                    notnull: false,
                },
            ],
        );
        c.indexes.push(SearchIndexRef {
            name: "idx_documents_content".into(),
            table: "documents".into(),
            columns: vec!["content".into()],
            pk_columns: vec!["id".into()],
        });
        c
    }

    #[test]
    fn like_on_text_rejected() {
        let c = cat();
        let t = Translator::new(&c, "_jm_t");
        let e = t
            .translate("SELECT id FROM documents WHERE content LIKE '%금융%'")
            .unwrap_err();
        assert_eq!(e.code, "JDB-4102");
        let e = t
            .translate("SELECT id FROM documents d WHERE d.content ILIKE '%x%'")
            .unwrap_err();
        assert_eq!(e.code, "JDB-4102");
        assert!(t
            .translate("SELECT id FROM documents WHERE title LIKE '%금융%'")
            .is_ok());
    }

    #[test]
    fn alter_add_column_gets_type_checks() {
        let c = cat();
        let t = Translator::new(&c, "_jm_t");
        for (ddl, tag) in [
            ("ALTER TABLE documents ADD COLUMN n INTEGER", "_jamsil_int4_n"),
            ("ALTER TABLE documents ADD COLUMN u UUID", "_jamsil_uuid_u"),
            ("ALTER TABLE documents ADD COLUMN d DATE", "_jamsil_date_d"),
        ] {
            let out = t.translate(ddl).unwrap().statements.join(";");
            assert!(out.contains(tag), "{ddl} -> {out}");
        }
    }

    #[test]
    fn bare_session_functions() {
        let c = cat();
        let t = Translator::new(&c, "_jm_t");
        let s = t.translate("SELECT current_user, session_user").unwrap().statements[0].clone();
        assert_eq!(s.matches("_jamsil_user()").count(), 2, "{s}");
        assert!(s.contains("AS current_user") && s.contains("AS session_user"), "{s}");
    }

    #[test]
    fn privileges_enforced_for_non_superusers() {
        use jamsil_common::acl::{Principal, Role};
        let c = cat();
        let rw = Principal { name: "app".into(), role: Role::ReadWrite, tables: Some(vec!["documents".into()]) };
        let ro = Principal { name: "rpt".into(), role: Role::ReadOnly, tables: None };
        let none = Principal { name: "x".into(), role: Role::ReadWrite, tables: Some(vec![]) };
        let tr = |p: &Principal, q: &str| Translator::new(&c, "_jm_t").with_principal(Some(p)).translate(q).map(|_| ());
        assert!(tr(&rw, "SELECT id FROM documents").is_ok());
        assert!(tr(&rw, "WITH d AS (SELECT id FROM documents) SELECT * FROM d").is_ok());
        assert!(tr(&rw, "INSERT INTO documents (id) VALUES (1)").is_ok());
        assert!(tr(&rw, "SELECT relname FROM pg_catalog.pg_class").is_ok());
        for q in [
            "CREATE TABLE t2 (id BIGINT PRIMARY KEY)",
            "DROP TABLE documents",
            "CREATE SEARCH INDEX ix ON documents(content)",
            "SELECT * FROM jamsil_catalog.active_sessions",
            "SELECT * FROM _jamsil_requests",
        ] {
            assert_eq!(tr(&rw, q).unwrap_err().sqlstate, "42501", "{q}");
        }
        assert_eq!(tr(&none, "SELECT id FROM documents").unwrap_err().sqlstate, "42501");
        assert_eq!(tr(&none, "SELECT 1 FROM (SELECT id FROM documents) q").unwrap_err().sqlstate, "42501");
        assert!(tr(&ro, "SELECT id FROM documents").is_ok());
        for q in [
            "INSERT INTO documents (id) VALUES (1)",
            "UPDATE documents SET title = 'x'",
            "DELETE FROM documents",
        ] {
            assert_eq!(tr(&ro, q).unwrap_err().sqlstate, "42501", "{q}");
        }
        assert!(tr(&Principal::superuser("su"), "DROP TABLE documents").is_ok());
    }

    #[test]
    fn like_on_text_derived_expressions_rejected() {
        let c = cat();
        let t = Translator::new(&c, "_jm_t");
        for q in [
            "SELECT id FROM documents WHERE lower(content) LIKE '%a%'",
            "SELECT id FROM documents WHERE content::varchar LIKE '%a%'",
            "SELECT * FROM (SELECT content AS x FROM documents) q WHERE x LIKE '%a%'",
            "WITH q AS (SELECT content FROM documents) SELECT * FROM q WHERE content LIKE '%a%'",
            "WITH q(y) AS (SELECT upper(content) FROM documents) SELECT * FROM q WHERE y ILIKE '%a%'",
            "SELECT id FROM documents d WHERE (d.title || d.content) LIKE '%a%'",
        ] {
            let e = t.translate(q).unwrap_err();
            assert_eq!(e.code, "JDB-4102", "{q}");
        }
        // VARCHAR 는 계속 허용
        t.translate("SELECT * FROM (SELECT title AS x FROM documents) q WHERE x LIKE '%a%'").unwrap();
        t.translate("SELECT id FROM documents WHERE lower(title) LIKE '%a%'").unwrap();
    }

    #[test]
    fn typed_values_are_normalized() {
        let mut c = cat();
        c.tables.insert(
            "ev".into(),
            vec![
                ColumnInfo { name: "id".into(), decl: "BIGINT".into(), pk: true, notnull: true },
                ColumnInfo { name: "ts".into(), decl: "TIMESTAMPTZ".into(), pk: false, notnull: false },
                ColumnInfo { name: "u".into(), decl: "UUID".into(), pk: false, notnull: false },
            ],
        );
        let t = Translator::new(&c, "x");
        let r = t.translate("SELECT count(*) FROM ev WHERE ts = '2026-10-27 00:00:00+00:00'").unwrap();
        assert!(r.statements[0].contains("ts = _jamsil_norm_tstz('2026-10-27 00:00:00+00:00')"), "{}", r.statements[0]);
        let r = t.translate("SELECT 1 FROM ev e WHERE e.u IN ('A', $1) AND ts BETWEEN $2 AND $3").unwrap();
        assert!(r.statements[0].contains("_jamsil_norm_uuid('A')"), "{}", r.statements[0]);
        assert!(r.statements[0].contains("BETWEEN _jamsil_norm_tstz($2) AND _jamsil_norm_tstz($3)"), "{}", r.statements[0]);
        let r = t.translate("INSERT INTO ev VALUES (1, '2026-10-27 09:00:00+09:00', $1), (2, DEFAULT, NULL)").unwrap();
        assert!(r.statements[0].contains("_jamsil_norm_tstz('2026-10-27 09:00:00+09:00'), _jamsil_norm_uuid($1)"), "{}", r.statements[0]);
        assert!(r.statements[0].contains("(2, DEFAULT, _jamsil_norm_uuid(NULL))"), "{}", r.statements[0]);
        let r = t.translate("UPDATE ev SET ts = $1 WHERE id = 1").unwrap();
        assert!(r.statements[0].contains("SET ts = _jamsil_norm_tstz($1)"), "{}", r.statements[0]);
        let r = t.translate("INSERT INTO ev (id, ts) VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET ts = excluded.ts").unwrap();
        assert!(r.statements[0].contains("ts = _jamsil_norm_tstz(excluded.ts)"), "{}", r.statements[0]);
    }

    #[test]
    fn integer_columns_are_strict() {
        let c = cat();
        let t = Translator::new(&c, "x");
        let s = &t.translate("CREATE TABLE z (id INTEGER PRIMARY KEY, s SMALLINT, b BIGINT, d DATE DEFAULT CURRENT_DATE)").unwrap().statements[0];
        assert!(s.contains("id INT NOT NULL") || s.contains("id INT PRIMARY KEY"), "{s}");
        assert!(!s.contains("INTEGER"), "plain INTEGER must not become a rowid alias: {s}");
        assert!(s.contains("CONSTRAINT \"_jamsil_int4_id\" CHECK"), "{s}");
        assert!(s.contains("typeof(\"s\") = 'integer'"), "{s}");
        assert!(s.contains("CONSTRAINT \"_jamsil_int8_b\""), "{s}");
        assert!(s.contains("DEFAULT (_jamsil_norm_date(CURRENT_DATE))"), "{s}");
        assert!(s.contains("_jamsil_canon_date(\"d\")"), "{s}");
    }

    #[test]
    fn text_match_rewrite() {
        let c = cat();
        let t = Translator::new(&c, "_jm_q1");
        let r = t
            .translate(
                "SELECT id, title, TEXT_SCORE(content) FROM documents WHERE TEXT_MATCH(content, '금융 AND (주식 OR 채권)') ORDER BY TEXT_SCORE(content) DESC LIMIT 20",
            )
            .unwrap();
        assert_eq!(r.text_matches.len(), 1);
        assert_eq!(
            r.text_matches[0].query,
            TextMatchQuery::Literal("금융 AND (주식 OR 채권)".into())
        );
        let s = &r.statements[0];
        assert!(
            s.contains("\"documents\".\"id\" IN (SELECT c0 FROM temp.\"_jm_q1_0\")"),
            "{s}"
        );
        assert!(s.contains("SELECT _s.score FROM temp.\"_jm_q1_0\""), "{s}");
        let e = t
            .translate("SELECT id FROM documents WHERE TEXT_MATCH(title, 'x')")
            .unwrap_err();
        assert_eq!(e.code, "JDB-4101");
        let r = t
            .translate("SELECT id FROM documents WHERE TEXT_MATCH(content, $1)")
            .unwrap();
        assert_eq!(r.text_matches[0].query, TextMatchQuery::Param(1));
    }

    #[test]
    fn ddl_rules() {
        let c = cat();
        let t = Translator::new(&c, "x");
        let r = t
            .translate("CREATE TABLE Docs (id BIGSERIAL PRIMARY KEY, title VARCHAR(200), body TEXT, n INTEGER, ok BOOLEAN DEFAULT true, at TIMESTAMPTZ DEFAULT now())")
            .unwrap();
        let s = &r.statements[0];
        assert!(s.starts_with("CREATE TABLE docs"), "{s}");
        assert!(s.contains("id INTEGER PRIMARY KEY CONSTRAINT"), "BIGSERIAL stays a rowid alias: {s}");
        assert!(s.contains("length(\"title\") <= 200"), "{s}");
        assert!(s.contains("DEFAULT (_jamsil_norm_tstz(now()))"), "{s}");
        assert!(t.translate("CREATE TABLE nopk (a INT)").unwrap_err().code == "JDB-5101");
        assert!(
            t.translate("CREATE TABLE d (id INT PRIMARY KEY, p DECIMAL(10,2))")
                .unwrap_err()
                .code
                == "JDB-5101"
        );
        let r = t
            .translate("DROP TABLE IF EXISTS a, public.b CASCADE")
            .unwrap();
        assert_eq!(
            r.statements,
            vec!["DROP TABLE IF EXISTS a", "DROP TABLE IF EXISTS b"]
        );
        assert_eq!(r.dropped_tables, vec!["a", "b"]);
        let r = t.translate("TRUNCATE documents").unwrap();
        assert_eq!(r.kind, StmtKind::Write);
        assert!(t.translate("INSERT INTO _jamsil_state VALUES (1)").is_err());
    }

    #[test]
    fn expression_rewrites() {
        let c = cat();
        let t = Translator::new(&c, "x");
        let r = t
            .translate("SELECT '1'::int, x::text FROM public.documents WHERE a IS DISTINCT FROM b")
            .unwrap();
        let s = &r.statements[0];
        assert!(s.contains("CAST('1' AS INTEGER)"), "{s}");
        assert!(s.contains("FROM documents"), "{s}");
        assert!(s.contains("CASE WHEN a IS NULL"), "{s}");
        assert_eq!(r.column_type_hints[0], Some(oid::INT4));
        let r = t
            .translate(
                "SELECT * FROM pg_catalog.pg_class c WHERE c.relname = E'a\\'b' LIMIT 1 OFFSET 2",
            )
            .unwrap();
        assert!(
            r.statements[0].contains("FROM pg_class AS c"),
            "{}",
            r.statements[0]
        );
        assert!(r.statements[0].contains("'a''b'"), "{}", r.statements[0]);
        let r = t
            .translate("SELECT * FROM information_schema.tables")
            .unwrap();
        assert!(r.statements[0].contains("information_schema_tables"));
        let r = t.translate("SELECT (current_schemas(true))[1]").unwrap();
        assert!(
            r.statements[0].contains("_jamsil_arr_get((current_schemas(true)), 1)"),
            "{}",
            r.statements[0]
        );
        let r = t
            .translate("SELECT 1 FROM documents WHERE id = ANY(ARRAY[1,2,3])")
            .unwrap();
        assert!(
            r.statements[0].contains("id IN (1, 2, 3)"),
            "{}",
            r.statements[0]
        );
    }
}
