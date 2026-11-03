//! jamsilDB 확장 SQL (설계서 10.2, 10.5):
//!
//! ```sql
//! CREATE SEARCH INDEX [IF NOT EXISTS] name ON table(col[, col...]) [WITH (analyzer = 'korean', scoring = 'bm25')];
//! DROP SEARCH INDEX [IF EXISTS] name;
//! SELECT ... WITH SEARCH CONSISTENCY COMMITTED|EVENTUAL;
//! ```

use std::sync::OnceLock;

use regex::Regex;

use jamsil_common::DbError;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateSearchIndex {
    pub name: String,
    pub if_not_exists: bool,
    pub table: String,
    pub columns: Vec<String>,
    pub analyzer: String,
    pub scoring: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropSearchIndex {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SearchDdl {
    Create(CreateSearchIndex),
    Drop(DropSearchIndex),
}

fn ident(s: &str) -> String {
    let s = s.trim();
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        s[1..s.len() - 1].replace("\"\"", "\"")
    } else {
        s.to_ascii_lowercase()
    }
}

fn strip_schema(s: &str) -> String {
    let s = s.trim();
    let lower = s.to_ascii_lowercase();
    if let Some(rest) = lower.strip_prefix("public.") {
        let _ = rest;
        return ident(&s[7..]);
    }
    ident(s)
}

pub fn parse_search_ddl(sql: &str) -> Option<Result<SearchDdl, DbError>> {
    static CREATE: OnceLock<Regex> = OnceLock::new();
    static DROP: OnceLock<Regex> = OnceLock::new();
    static PREFIX: OnceLock<Regex> = OnceLock::new();
    let prefix =
        PREFIX.get_or_init(|| Regex::new(r"(?is)^\s*(create|drop)\s+search\s+index\b").unwrap());
    if !prefix.is_match(sql) {
        return None;
    }
    let create = CREATE.get_or_init(|| {
        Regex::new(
            r#"(?is)^\s*create\s+search\s+index\s+(if\s+not\s+exists\s+)?("(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)\s+on\s+((?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)(?:\.(?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*))?)\s*\(([^)]*)\)\s*(?:with\s*\(([^)]*)\))?\s*;?\s*$"#,
        )
        .unwrap()
    });
    let drop = DROP.get_or_init(|| {
        Regex::new(
            r#"(?is)^\s*drop\s+search\s+index\s+(if\s+exists\s+)?("(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)\s*;?\s*$"#,
        )
        .unwrap()
    });
    if let Some(c) = create.captures(sql) {
        let name = ident(&c[2]);
        let table = strip_schema(&c[3]);
        let columns: Vec<String> = c[4]
            .split(',')
            .map(ident)
            .filter(|s| !s.is_empty())
            .collect();
        if columns.is_empty() {
            return Some(Err(DbError::syntax(
                "CREATE SEARCH INDEX requires at least one column",
            )));
        }
        let mut analyzer = "korean".to_string();
        let mut scoring = "bm25".to_string();
        if let Some(opts) = c.get(5) {
            for kv in opts.as_str().split(',') {
                let kv = kv.trim();
                if kv.is_empty() {
                    continue;
                }
                let Some((k, v)) = kv.split_once('=') else {
                    return Some(Err(DbError::syntax(format!(
                        "invalid search index option: {kv}"
                    ))));
                };
                let k = k.trim().to_ascii_lowercase();
                let v = v.trim().trim_matches('\'').to_ascii_lowercase();
                match k.as_str() {
                    "analyzer" => match v.as_str() {
                        "korean" | "english" | "standard" | "simple" => analyzer = v,
                        _ => {
                            return Some(Err(DbError::invalid_param(format!(
                                "unknown analyzer '{v}' (korean, english, standard, simple)"
                            ))))
                        }
                    },
                    "scoring" => {
                        if v != "bm25" {
                            return Some(Err(DbError::invalid_param(
                                "only scoring = 'bm25' is supported",
                            )));
                        }
                        scoring = v;
                    }
                    _ => {
                        return Some(Err(DbError::invalid_param(format!(
                            "unknown search index option {k}"
                        ))))
                    }
                }
            }
        }
        return Some(Ok(SearchDdl::Create(CreateSearchIndex {
            name,
            if_not_exists: c.get(1).is_some(),
            table,
            columns,
            analyzer,
            scoring,
        })));
    }
    if let Some(c) = drop.captures(sql) {
        return Some(Ok(SearchDdl::Drop(DropSearchIndex {
            name: ident(&c[2]),
            if_exists: c.get(1).is_some(),
        })));
    }
    Some(Err(DbError::syntax("invalid SEARCH INDEX statement")))
}

/// `... WITH SEARCH CONSISTENCY COMMITTED` 접미 구문 제거. 반환: (나머지 SQL, Some(committed?)).
pub fn strip_search_consistency(sql: &str) -> (String, Option<bool>) {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r"(?is)\s+with\s+search\s+consistency\s+(committed|eventual)\s*;?\s*$").unwrap()
    });
    if let Some(c) = re.captures(sql) {
        let committed = c[1].eq_ignore_ascii_case("committed");
        let m = c.get(0).unwrap();
        return (sql[..m.start()].to_string(), Some(committed));
    }
    (sql.to_string(), None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create() {
        let d = parse_search_ddl(
            "CREATE SEARCH INDEX idx_documents_content\nON documents(content)\nWITH (analyzer = 'korean', scoring = 'bm25');",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            d,
            SearchDdl::Create(CreateSearchIndex {
                name: "idx_documents_content".into(),
                if_not_exists: false,
                table: "documents".into(),
                columns: vec!["content".into()],
                analyzer: "korean".into(),
                scoring: "bm25".into()
            })
        );
        let d = parse_search_ddl(
            "create search index if not exists i on public.Docs (Title, \"Body\")",
        )
        .unwrap()
        .unwrap();
        match d {
            SearchDdl::Create(c) => {
                assert!(c.if_not_exists);
                assert_eq!(c.table, "docs");
                assert_eq!(c.columns, vec!["title".to_string(), "Body".to_string()]);
            }
            _ => panic!(),
        }
        assert!(parse_search_ddl("select 1").is_none());
        assert!(matches!(
            parse_search_ddl("DROP SEARCH INDEX IF EXISTS x"),
            Some(Ok(SearchDdl::Drop(_)))
        ));
    }

    #[test]
    fn strip_consistency() {
        let (s, c) = strip_search_consistency(
            "SELECT id FROM d WHERE TEXT_MATCH(content, '인공지능')\nWITH SEARCH CONSISTENCY COMMITTED;",
        );
        assert_eq!(c, Some(true));
        assert_eq!(s, "SELECT id FROM d WHERE TEXT_MATCH(content, '인공지능')");
    }
}
