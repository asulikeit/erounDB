//! 문장 안의 관계(테이블·뷰·파생 테이블·CTE)와 컬럼 값 종류를 해석한다.
//!
//! * TEXT 파생 여부: `lower(body)`, `body AS x`(파생 테이블/CTE), `body::varchar` 등 TEXT 컬럼을 참조하는
//!   모든 표현식은 TEXT로 본다 (TEXT LIKE 금지 규칙, RQ-10).
//! * 정규화 대상 타입(UUID/DATE/TIME/TIMESTAMP/TIMESTAMPTZ) 컬럼과 비교·대입되는 값을 찾는다 (RQ-04/05).
//!
//! 해석은 문장 전체에 대해 평면적(scope 비구분)으로 수행한다. 같은 이름이 여러 관계에 있으면
//! 보수적으로 "TEXT 파생"을 우선한다.

use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Query, SelectItem, SetExpr, Statement, TableFactor, Visit, Visitor};

use crate::translate::Catalog;
use crate::types::{decl_kind, ValueKind};

#[derive(Debug, Clone)]
pub struct RelInfo {
    /// SQL에서 이 관계를 가리키는 이름 (별칭 또는 테이블 이름)
    pub qual: String,
    /// 기반 테이블 이름 (파생 테이블/CTE는 None)
    pub table: Option<String>,
    /// CTE를 이름으로 참조하는 경우 그 CTE 이름 / 파생 테이블이면 별칭
    pub source: Option<String>,
    pub cols: Vec<(String, ValueKind)>,
}

#[derive(Debug, Default, Clone)]
pub struct Scope {
    pub rels: Vec<RelInfo>,
}

fn fold(i: &sqlparser::ast::Ident) -> String {
    if i.quote_style.is_some() {
        i.value.clone()
    } else {
        i.value.to_ascii_lowercase()
    }
}

fn rel_name(o: &sqlparser::ast::ObjectName) -> String {
    // 스키마 한정자 제거 (public.t → t)
    o.0.last().map(fold).unwrap_or_default()
}

/// 컬럼 참조 (qualifier, column).
pub fn column_ref(e: &Expr) -> Option<(Option<String>, String)> {
    match e {
        Expr::Identifier(i) => Some((None, fold(i))),
        Expr::CompoundIdentifier(v) if v.len() >= 2 => {
            Some((Some(fold(&v[v.len() - 2])), fold(&v[v.len() - 1])))
        }
        Expr::Nested(inner) => column_ref(inner),
        _ => None,
    }
}

#[derive(Default)]
struct Collect {
    tables: Vec<(String, Option<String>)>,
    derived: Vec<(String, Query, Vec<String>)>,
    ctes: Vec<(String, Query, Vec<String>)>,
}

impl Visitor for Collect {
    type Break = ();
    fn pre_visit_table_factor(&mut self, tf: &TableFactor) -> ControlFlow<()> {
        match tf {
            TableFactor::Table { name, alias, .. } => {
                self.tables
                    .push((rel_name(name), alias.as_ref().map(|a| fold(&a.name))));
            }
            TableFactor::Derived {
                subquery,
                alias: Some(a),
                ..
            } => {
                self.derived.push((
                    fold(&a.name),
                    (**subquery).clone(),
                    a.columns.iter().map(fold).collect(),
                ));
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        if let Some(w) = &q.with {
            for c in &w.cte_tables {
                self.ctes.push((
                    fold(&c.alias.name),
                    (*c.query).clone(),
                    c.alias.columns.iter().map(fold).collect(),
                ));
            }
        }
        ControlFlow::Continue(())
    }
}

/// 표현식 안의 (하위 질의를 제외한) 컬럼 참조.
#[derive(Default)]
struct Refs {
    depth: usize,
    refs: Vec<(Option<String>, String)>,
}

impl Visitor for Refs {
    type Break = ();
    fn pre_visit_query(&mut self, _q: &Query) -> ControlFlow<()> {
        self.depth += 1;
        ControlFlow::Continue(())
    }
    fn post_visit_query(&mut self, _q: &Query) -> ControlFlow<()> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }
    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        if self.depth == 0 {
            match e {
                Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                    if let Some(r) = column_ref(e) {
                        self.refs.push(r);
                    }
                }
                _ => {}
            }
        }
        ControlFlow::Continue(())
    }
}

impl Scope {
    pub fn build_statement(stmt: &Statement, catalog: &dyn Catalog) -> Scope {
        let mut c = Collect::default();
        let _ = stmt.visit(&mut c);
        Self::from_collect(c, catalog)
    }

    pub fn build_query(q: &Query, catalog: &dyn Catalog) -> Scope {
        let mut c = Collect::default();
        let _ = q.visit(&mut c);
        Self::from_collect(c, catalog)
    }

    fn from_collect(c: Collect, catalog: &dyn Catalog) -> Scope {
        let mut s = Scope::default();
        for (t, alias) in &c.tables {
            let qual = alias.clone().unwrap_or_else(|| t.clone());
            match catalog.columns(t) {
                Some(cols) => s.rels.push(RelInfo {
                    qual,
                    table: Some(t.clone()),
                    source: None,
                    cols: cols
                        .iter()
                        .map(|ci| (ci.name.to_ascii_lowercase(), decl_kind(&ci.decl)))
                        .collect(),
                }),
                // 카탈로그에 없는 이름: CTE 참조일 수 있다
                None => s.rels.push(RelInfo { qual, table: None, source: Some(t.clone()), cols: vec![] }),
            }
        }
        for (alias, _, _) in &c.derived {
            s.rels.push(RelInfo { qual: alias.clone(), table: None, source: Some(alias.clone()), cols: vec![] });
        }
        // 파생 테이블/CTE는 다른 관계를 참조할 수 있으므로 반복해 수렴시킨다.
        for _ in 0..4 {
            let mut changed = false;
            for (name, q, colnames) in c.ctes.iter().chain(c.derived.iter()) {
                let cols = s.projection_kinds(q, colnames);
                for r in s.rels.iter_mut() {
                    if r.source.as_deref() == Some(name.as_str()) && r.cols != cols {
                        r.cols = cols.clone();
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }
        s
    }

    /// 질의 결과 컬럼의 (이름, 종류).
    pub fn projection_kinds(&self, q: &Query, rename: &[String]) -> Vec<(String, ValueKind)> {
        let mut out = vec![];
        let SetExpr::Select(sel) = q.body.as_ref() else {
            return out;
        };
        for item in &sel.projection {
            match item {
                SelectItem::UnnamedExpr(e) => {
                    let name = column_ref(e)
                        .map(|(_, c)| c)
                        .unwrap_or_else(|| "?column?".into());
                    out.push((name, self.expr_kind(e)));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    out.push((fold(alias), self.expr_kind(expr)));
                }
                SelectItem::Wildcard(_) => {
                    // 내부 FROM의 모든 관계 (평면 해석)
                    let mut inner = Collect::default();
                    for f in &sel.from {
                        let _ = f.visit(&mut inner);
                    }
                    for (t, a) in inner.tables {
                        let qual = a.unwrap_or(t);
                        if let Some(r) = self.rels.iter().find(|r| r.qual == qual) {
                            out.extend(r.cols.iter().cloned());
                        }
                    }
                }
                SelectItem::QualifiedWildcard(o, _) => {
                    let qual = rel_name(o);
                    if let Some(r) = self.rels.iter().find(|r| r.qual == qual) {
                        out.extend(r.cols.iter().cloned());
                    }
                }
            }
        }
        for (i, n) in rename.iter().enumerate() {
            if let Some(c) = out.get_mut(i) {
                c.0 = n.clone();
            }
        }
        out
    }

    /// 컬럼 참조 해석: 일치하는 (관계, 종류) 목록.
    pub fn resolve(&self, q: &Option<String>, col: &str) -> Vec<(&RelInfo, ValueKind)> {
        let mut out = vec![];
        for r in &self.rels {
            if let Some(q) = q {
                if q != &r.qual && Some(q) != r.table.as_ref() {
                    continue;
                }
            }
            if let Some((_, k)) = r.cols.iter().find(|(n, _)| n.eq_ignore_ascii_case(col)) {
                out.push((r, *k));
            }
        }
        out
    }

    /// 직접 컬럼 참조의 종류 (여러 관계에 걸리면 TEXT 우선, 그다음 첫 번째).
    pub fn ref_kind(&self, e: &Expr) -> Option<ValueKind> {
        let (q, c) = column_ref(e)?;
        let hits = self.resolve(&q, &c);
        if hits.iter().any(|(_, k)| *k == ValueKind::Text) {
            return Some(ValueKind::Text);
        }
        hits.first().map(|(_, k)| *k)
    }

    /// 표현식이 TEXT 컬럼에서 파생되었는가 (하위 질의 제외).
    pub fn derives_text(&self, e: &Expr) -> bool {
        let mut r = Refs::default();
        let _ = e.visit(&mut r);
        r.refs.iter().any(|(q, c)| {
            self.resolve(q, c)
                .iter()
                .any(|(_, k)| *k == ValueKind::Text)
        })
    }

    /// 표현식 결과의 종류: 직접 참조면 그 컬럼 종류, TEXT를 참조하면 Text, 아니면 Other.
    pub fn expr_kind(&self, e: &Expr) -> ValueKind {
        if let Some(k) = self.ref_kind(e) {
            return k;
        }
        if self.derives_text(e) {
            ValueKind::Text
        } else {
            ValueKind::Other
        }
    }
}

/// 뷰 정의(`CREATE VIEW ... AS <query>`)에서 TEXT 파생 컬럼 이름 목록.
pub fn view_text_columns(view_sql: &str, catalog: &dyn Catalog) -> Vec<String> {
    let Ok(mut stmts) = crate::classify::parse_statements(view_sql) else {
        return vec![];
    };
    let Some(Statement::CreateView { query, columns, .. }) = stmts.pop() else {
        return vec![];
    };
    let scope = Scope::build_query(&query, catalog);
    let rename: Vec<String> = columns.iter().map(|c| fold(&c.name)).collect();
    scope
        .projection_kinds(&query, &rename)
        .into_iter()
        .filter(|(_, k)| *k == ValueKind::Text)
        .map(|(n, _)| n)
        .collect()
}
