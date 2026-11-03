//! SQL 문장 분리·분류 (SQL 프로세스의 라우팅 결정용).

use sqlparser::ast::{
    Expr, ObjectName, OneOrManyWithParens, Query, SetExpr, Statement, Value as SqlValue,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use jamsil_common::DbError;

use crate::search_ddl::{parse_search_ddl, strip_search_consistency};

#[derive(Debug, Clone, PartialEq)]
pub enum StmtKind {
    Empty,
    /// 읽기 전용 (SELECT/VALUES/EXPLAIN).
    Read,
    /// DML (INSERT/UPDATE/DELETE).
    Write,
    /// 스키마 변경 (CREATE/DROP/ALTER/TRUNCATE, SEARCH INDEX).
    Ddl,
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    Release(String),
    RollbackTo(String),
    Set {
        name: String,
        value: String,
        local: bool,
    },
    Reset(String),
    Show(String),
    /// SET TRANSACTION / SET SESSION CHARACTERISTICS (무시하되 성공).
    SetTransaction,
    Discard,
    Deallocate(Option<String>),
    /// 지원하지 않는 문장 (사유).
    Unsupported(String),
}

impl StmtKind {
    pub fn is_write(&self) -> bool {
        matches!(self, StmtKind::Write | StmtKind::Ddl)
    }
}

#[derive(Debug, Clone)]
pub struct Classified {
    /// 저장 엔진으로 전달할 문장 텍스트 (단일 문장).
    pub sql: String,
    pub kind: StmtKind,
    /// `WITH SEARCH CONSISTENCY` 지정값 (Some(true) = committed).
    pub search_committed: Option<bool>,
    /// TEXT_MATCH/TEXT_SCORE 사용 여부 (고정 Snapshot 트랜잭션에서 거부하는 데 쓴다).
    pub uses_search: bool,
}

/// 문장 어디에든(서브쿼리·CTE·DML 조건 포함) TEXT_MATCH/TEXT_SCORE 호출이 있는가.
pub fn uses_search(stmt: &Statement) -> bool {
    sqlparser::ast::visit_expressions(stmt, |e| match e {
        Expr::Function(f)
            if f.name.0.last().is_some_and(|i| {
                i.value.eq_ignore_ascii_case("text_match") || i.value.eq_ignore_ascii_case("text_score")
            }) =>
        {
            std::ops::ControlFlow::Break(())
        }
        _ => std::ops::ControlFlow::Continue(()),
    })
    .is_break()
}

fn obj_name(o: &ObjectName) -> String {
    o.0.iter()
        .map(|i| i.value.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

fn expr_to_setting(e: &Expr) -> String {
    match e {
        Expr::Value(SqlValue::SingleQuotedString(s))
        | Expr::Value(SqlValue::DoubleQuotedString(s))
        | Expr::Value(SqlValue::EscapedStringLiteral(s)) => s.clone(),
        Expr::Value(SqlValue::Number(n, _)) => n.clone(),
        Expr::Value(SqlValue::Boolean(b)) => b.to_string(),
        Expr::Identifier(i) => i.value.clone(),
        Expr::UnaryOp { op, expr } => format!("{op}{}", expr_to_setting(expr)),
        other => other.to_string(),
    }
}

fn query_is_write(q: &Query) -> bool {
    fn set_is_write(s: &SetExpr) -> bool {
        match s {
            SetExpr::Insert(_) | SetExpr::Update(_) => true,
            SetExpr::SetOperation { left, right, .. } => set_is_write(left) || set_is_write(right),
            SetExpr::Query(q) => query_is_write(q),
            _ => false,
        }
    }
    if set_is_write(&q.body) {
        return true;
    }
    if let Some(w) = &q.with {
        if w.cte_tables.iter().any(|c| query_is_write(&c.query)) {
            return true;
        }
    }
    false
}

pub fn kind_of(stmt: &Statement) -> StmtKind {
    match stmt {
        Statement::Query(q) => {
            if query_is_write(q) {
                StmtKind::Write
            } else {
                StmtKind::Read
            }
        }
        Statement::Explain { .. } | Statement::ExplainTable { .. } => StmtKind::Read,
        Statement::Insert(_) | Statement::Update { .. } | Statement::Delete(_) => StmtKind::Write,
        Statement::CreateTable(_)
        | Statement::CreateIndex(_)
        | Statement::CreateView { .. }
        | Statement::AlterTable { .. }
        | Statement::AlterIndex { .. }
        | Statement::AlterView { .. }
        | Statement::Drop { .. }
        | Statement::Truncate { .. }
        | Statement::CreateSchema { .. }
        | Statement::Comment { .. } => StmtKind::Ddl,
        Statement::StartTransaction { .. } => StmtKind::Begin,
        Statement::Commit { .. } => StmtKind::Commit,
        Statement::Rollback {
            savepoint: Some(sp),
            ..
        } => StmtKind::RollbackTo(sp.value.clone()),
        Statement::Rollback { .. } => StmtKind::Rollback,
        Statement::Savepoint { name } => StmtKind::Savepoint(name.value.clone()),
        Statement::ReleaseSavepoint { name } => StmtKind::Release(name.value.clone()),
        Statement::SetVariable {
            local,
            variables,
            value,
            ..
        } => {
            let name = match variables {
                OneOrManyWithParens::One(o) => obj_name(o),
                OneOrManyWithParens::Many(v) => {
                    v.iter().map(obj_name).collect::<Vec<_>>().join(",")
                }
            };
            let value = value
                .iter()
                .map(expr_to_setting)
                .collect::<Vec<_>>()
                .join(", ");
            StmtKind::Set {
                name,
                value,
                local: *local,
            }
        }
        Statement::SetTimeZone { local, value } => StmtKind::Set {
            name: "timezone".into(),
            value: expr_to_setting(value),
            local: *local,
        },
        Statement::SetNames { charset_name, .. } => StmtKind::Set {
            name: "client_encoding".into(),
            value: charset_name.clone(),
            local: false,
        },
        Statement::SetNamesDefault {} => StmtKind::Set {
            name: "client_encoding".into(),
            value: "UTF8".into(),
            local: false,
        },
        Statement::SetTransaction { .. } => StmtKind::SetTransaction,
        Statement::ShowVariable { variable } => StmtKind::Show(
            variable
                .iter()
                .map(|i| i.value.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join(" "),
        ),
        Statement::Discard { .. } => StmtKind::Discard,
        Statement::Deallocate { name, .. } => {
            let n = name.value.clone();
            if n.eq_ignore_ascii_case("all") {
                StmtKind::Deallocate(None)
            } else {
                StmtKind::Deallocate(Some(n))
            }
        }
        other => StmtKind::Unsupported(first_words(&other.to_string())),
    }
}

fn first_words(s: &str) -> String {
    s.split_whitespace().take(3).collect::<Vec<_>>().join(" ")
}

pub fn parse_statements(sql: &str) -> Result<Vec<Statement>, DbError> {
    Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|e| {
        let msg = e.to_string();
        let msg = msg
            .strip_prefix("sql parser error: ")
            .unwrap_or(&msg)
            .to_string();
        DbError::syntax(format!("syntax error: {msg}"))
    })
}

/// 특수 처리되는 문장 (파서가 모르는 PG 문법).
fn special(sql: &str) -> Option<StmtKind> {
    let t = sql.trim().trim_end_matches(';').trim();
    let lower = t.to_ascii_lowercase();
    let words: Vec<&str> = lower.split_whitespace().collect();
    match words.as_slice() {
        ["reset", name] => Some(StmtKind::Reset(name.to_string())),
        ["show", "transaction", "isolation", "level"] => {
            Some(StmtKind::Show("transaction_isolation".into()))
        }
        ["show", rest @ ..] if !rest.is_empty() => Some(StmtKind::Show(rest.join(" "))),
        ["begin"] | ["begin", "transaction"] | ["begin", "work"] | ["start", "transaction"] => {
            Some(StmtKind::Begin)
        }
        ["begin", ..] if lower.contains("isolation level") || lower.contains("read ") => {
            Some(StmtKind::Begin)
        }
        ["end"] | ["end", "transaction"] | ["end", "work"] => Some(StmtKind::Commit),
        ["abort"] => Some(StmtKind::Rollback),
        ["set", "session", "characteristics", ..] => Some(StmtKind::SetTransaction),
        ["listen", ..] | ["unlisten", ..] | ["notify", ..] => {
            Some(StmtKind::Unsupported("LISTEN/NOTIFY".into()))
        }
        _ => None,
    }
}

/// Simple Query 문자열을 문장 단위로 분리·분류한다.
pub fn classify(sql: &str) -> Result<Vec<Classified>, DbError> {
    let rewritten = crate::compat::rewrite(sql);
    let trimmed = rewritten.trim();
    if trimmed.is_empty() || trimmed.chars().all(|c| c == ';' || c.is_whitespace()) {
        return Ok(vec![Classified {
            sql: String::new(),
            kind: StmtKind::Empty,
            search_committed: None,
            uses_search: false,
        }]);
    }
    if let Some(k) = special(trimmed) {
        return Ok(vec![Classified {
            sql: trimmed.to_string(),
            kind: k,
            search_committed: None,
            uses_search: false,
        }]);
    }
    if let Some(r) = parse_search_ddl(trimmed) {
        r?;
        return Ok(vec![Classified {
            sql: trimmed.to_string(),
            kind: StmtKind::Ddl,
            search_committed: None,
            uses_search: false,
        }]);
    }
    let (body, committed) = strip_search_consistency(trimmed);
    let stmts = parse_statements(&body)?;
    if stmts.is_empty() {
        return Ok(vec![Classified {
            sql: String::new(),
            kind: StmtKind::Empty,
            search_committed: None,
            uses_search: false,
        }]);
    }
    let multi = stmts.len() > 1;
    Ok(stmts
        .into_iter()
        .map(|s| {
            let kind = kind_of(&s);
            let uses_search = uses_search(&s);
            // 단일 문장이면 원문을 그대로 넘겨 대형 리터럴의 재직렬화를 피한다.
            let sql = if multi {
                s.to_string()
            } else {
                body.trim().trim_end_matches(';').to_string()
            };
            Classified {
                sql,
                kind,
                search_committed: committed,
                uses_search,
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_search_functions() {
        let uses = |sql: &str| classify(sql).unwrap()[0].uses_search;
        assert!(uses("SELECT id FROM d WHERE TEXT_MATCH(body, 'a') WITH SEARCH CONSISTENCY COMMITTED"));
        assert!(uses("select id, text_score(body) from d where text_match(body, 'a')"));
        assert!(uses("SELECT 1 WHERE EXISTS (SELECT 1 FROM d WHERE TEXT_MATCH(body, 'a'))"));
        assert!(uses("WITH m AS (SELECT id FROM d WHERE TEXT_MATCH(body, $1)) SELECT * FROM m"));
        assert!(uses("UPDATE d SET n = n + 1 WHERE TEXT_MATCH(body, 'a')"));
        assert!(!uses("SELECT 'TEXT_MATCH(body, x)' AS s, text_match FROM d"));
        assert!(!uses("SELECT id FROM d WHERE body = 'alpha'"));
    }

    #[test]
    fn classify_basic() {
        let v = classify("BEGIN; INSERT INTO t VALUES (1); SELECT * FROM t; COMMIT").unwrap();
        let kinds: Vec<_> = v.iter().map(|c| c.kind.clone()).collect();
        assert_eq!(
            kinds,
            vec![
                StmtKind::Begin,
                StmtKind::Write,
                StmtKind::Read,
                StmtKind::Commit
            ]
        );
        assert_eq!(classify("").unwrap()[0].kind, StmtKind::Empty);
        assert_eq!(
            classify("SET extra_float_digits = 3").unwrap()[0].kind,
            StmtKind::Set {
                name: "extra_float_digits".into(),
                value: "3".into(),
                local: false
            }
        );
        assert_eq!(
            classify("SHOW TRANSACTION ISOLATION LEVEL").unwrap()[0].kind,
            StmtKind::Show("transaction_isolation".into())
        );
        assert_eq!(
            classify("create search index i on t(c)").unwrap()[0].kind,
            StmtKind::Ddl
        );
        assert_eq!(
            classify("CREATE TABLE x (id int primary key)").unwrap()[0].kind,
            StmtKind::Ddl
        );
        assert_eq!(
            classify("WITH x AS (INSERT INTO t VALUES (1) RETURNING id) SELECT * FROM x").unwrap()
                [0]
            .kind,
            StmtKind::Write
        );
        let c = &classify("select 1 with search consistency committed").unwrap()[0];
        assert_eq!(c.search_committed, Some(true));
        assert_eq!(c.kind, StmtKind::Read);
        assert!(classify("SELEC 1").is_err());
    }
}
