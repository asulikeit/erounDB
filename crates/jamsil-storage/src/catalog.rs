//! 시스템 테이블과 스키마 캐시.

use std::collections::HashMap;

use rusqlite::{params, Connection, OptionalExtension};

use jamsil_common::DbError;
use jamsil_ipc::msg::SearchIndexDef;
use jamsil_sql::{Catalog, ColumnInfo, SearchIndexRef};

use crate::convert::map_err;

/// 복제되는 시스템 테이블(_jamsil_state, _jamsil_search_indexes, _jamsil_requests)과
/// 노드 로컬 테이블(_jamsil_local, _jamsil_outbox). 모두 Session 추적 대상에서 제외된다.
pub const SYSTEM_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS _jamsil_state (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    applied_index BIGINT NOT NULL,
    applied_term BIGINT NOT NULL,
    schema_version BIGINT NOT NULL,
    format_version INTEGER NOT NULL,
    cluster_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS _jamsil_search_indexes (
    name TEXT PRIMARY KEY NOT NULL,
    table_name TEXT NOT NULL,
    columns TEXT NOT NULL,
    analyzer TEXT NOT NULL,
    scoring TEXT NOT NULL,
    created_index BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS _jamsil_requests (
    request_id TEXT PRIMARY KEY NOT NULL,
    raft_index BIGINT NOT NULL,
    tag TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS _jamsil_local (
    k TEXT PRIMARY KEY NOT NULL,
    v
);
CREATE TABLE IF NOT EXISTS _jamsil_outbox (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    raft_index BIGINT NOT NULL,
    index_name TEXT NOT NULL,
    op INTEGER NOT NULL,
    pk TEXT NOT NULL
);
"#;

pub const OP_UPSERT: i64 = 0;
pub const OP_DELETE: i64 = 1;
pub const OP_INDEX_CREATED: i64 = 2;
pub const OP_INDEX_DROPPED: i64 = 3;

pub fn is_system_table(name: &str) -> bool {
    name.starts_with("_jamsil") || name.starts_with("sqlite_")
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StateRow {
    pub applied_index: u64,
    pub applied_term: u64,
    pub schema_version: u64,
}

pub fn read_state(conn: &Connection) -> Result<StateRow, DbError> {
    conn.query_row(
        "SELECT applied_index, applied_term, schema_version FROM _jamsil_state WHERE singleton_id = 1",
        [],
        |r| {
            Ok(StateRow {
                applied_index: r.get::<_, i64>(0)? as u64,
                applied_term: r.get::<_, i64>(1)? as u64,
                schema_version: r.get::<_, i64>(2)? as u64,
            })
        },
    )
    .map_err(map_err)
}

pub fn init_system_tables(conn: &Connection, cluster_id: &str) -> Result<(), DbError> {
    conn.execute_batch(SYSTEM_DDL).map_err(map_err)?;
    let existing: Option<String> = conn
        .query_row(
            "SELECT cluster_id FROM _jamsil_state WHERE singleton_id = 1",
            [],
            |r| r.get(0),
        )
        .optional()
        .map_err(map_err)?;
    match existing {
        None => {
            conn.execute(
                "INSERT INTO _jamsil_state VALUES (1, 0, 0, 1, ?1, ?2)",
                params![jamsil_common::FORMAT_VERSION, cluster_id],
            )
            .map_err(map_err)?;
        }
        Some(c) if c != cluster_id => {
            return Err(DbError::store_recovery_required(format!(
                "cluster id mismatch: data belongs to '{c}', configured '{cluster_id}'"
            ))
            .fatal());
        }
        Some(_) => {}
    }
    // 0.1.0-3: 멱등 요청 이력에 지문·결과 컬럼 추가 (모든 노드에서 같은 결정적 변경)
    let has_fp: bool = conn
        .query_row(
            "SELECT count(*) FROM pragma_table_info('_jamsil_requests') WHERE name = 'fingerprint'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .map_err(map_err)?
        > 0;
    if !has_fp {
        conn.execute_batch(
            "ALTER TABLE _jamsil_requests ADD COLUMN fingerprint TEXT; ALTER TABLE _jamsil_requests ADD COLUMN result BLOB;",
        )
        .map_err(map_err)?;
    }
    let fv: i64 = conn
        .query_row(
            "SELECT format_version FROM _jamsil_state WHERE singleton_id = 1",
            [],
            |r| r.get(0),
        )
        .map_err(map_err)?;
    if fv as u32 != jamsil_common::FORMAT_VERSION {
        return Err(DbError::store_recovery_required(format!(
            "data format version {fv} is not supported (expected {})",
            jamsil_common::FORMAT_VERSION
        ))
        .fatal());
    }
    Ok(())
}

pub fn local_get(conn: &Connection, k: &str) -> Result<Option<i64>, DbError> {
    conn.query_row("SELECT v FROM _jamsil_local WHERE k = ?1", [k], |r| {
        r.get::<_, i64>(0)
    })
    .optional()
    .map_err(map_err)
}

pub fn local_set(conn: &Connection, k: &str, v: i64) -> Result<(), DbError> {
    conn.execute("INSERT INTO _jamsil_local(k, v) VALUES (?1, ?2) ON CONFLICT(k) DO UPDATE SET v = excluded.v", params![k, v])
        .map_err(map_err)?;
    Ok(())
}

/// 사용자 테이블/뷰 스키마와 검색 인덱스 정의 캐시.
#[derive(Debug, Clone, Default)]
pub struct CatalogCache {
    pub schema_version: u64,
    pub tables: HashMap<String, Vec<ColumnInfo>>,
    pub search: Vec<SearchIndexDef>,
}

impl CatalogCache {
    pub fn load(conn: &Connection) -> Result<CatalogCache, DbError> {
        let schema_version = read_state(conn).map(|s| s.schema_version).unwrap_or(0);
        let mut tables = HashMap::new();
        let names: Vec<String> = {
            let mut st = conn
                .prepare("SELECT name FROM sqlite_schema WHERE type IN ('table','view') AND substr(name,1,7) <> '_jamsil' AND substr(name,1,7) <> 'sqlite_'")
                .map_err(map_err)?;
            let rows = st
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(map_err)?;
            rows.collect::<Result<_, _>>().map_err(map_err)?
        };
        for n in names {
            match table_columns(conn, &n) {
                Ok(cols) => {
                    tables.insert(n.to_ascii_lowercase(), cols);
                }
                // 기반 테이블이 사라진 뷰 등은 건너뛴다.
                Err(e) => tracing::warn!("catalog: skipping relation {n}: {e}"),
            }
        }
        // 뷰의 표현식 컬럼 중 TEXT에서 파생된 것은 TEXT로 표시 (TEXT LIKE 금지 규칙이 뷰를 통해 우회되지 않도록)
        let views: Vec<(String, String)> = {
            let mut st = conn
                .prepare("SELECT name, sql FROM sqlite_schema WHERE type = 'view' AND sql IS NOT NULL AND substr(name,1,7) <> '_jamsil'")
                .map_err(map_err)?;
            let rows = st
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .map_err(map_err)?;
            rows.collect::<Result<_, _>>().map_err(map_err)?
        };
        if !views.is_empty() {
            let partial = CatalogCache {
                schema_version,
                tables: tables.clone(),
                search: vec![],
            };
            for (name, sql) in views {
                let text_cols = jamsil_sql::scope::view_text_columns(&sql, &partial);
                if let Some(cols) = tables.get_mut(&name.to_ascii_lowercase()) {
                    for c in cols.iter_mut() {
                        if c.decl.is_empty() && text_cols.iter().any(|t| t.eq_ignore_ascii_case(&c.name)) {
                            c.decl = "TEXT".into();
                        }
                    }
                }
            }
        }
        let search = load_search_indexes(conn, &tables)?;
        Ok(CatalogCache {
            schema_version,
            tables,
            search,
        })
    }

    pub fn search_def(&self, name: &str) -> Option<&SearchIndexDef> {
        self.search.iter().find(|s| s.name == name)
    }

    pub fn search_for_table(&self, table: &str) -> Vec<&SearchIndexDef> {
        self.search
            .iter()
            .filter(|s| s.table.eq_ignore_ascii_case(table))
            .collect()
    }
}

pub fn table_columns(conn: &Connection, table: &str) -> Result<Vec<ColumnInfo>, DbError> {
    let mut st = conn
        .prepare("SELECT name, type, pk, \"notnull\" FROM pragma_table_info(?1) ORDER BY cid")
        .map_err(map_err)?;
    let rows = st
        .query_map([table], |r| {
            Ok(ColumnInfo {
                name: r.get(0)?,
                decl: r.get::<_, Option<String>>(1)?.unwrap_or_default(),
                pk: r.get::<_, i64>(2)? > 0,
                notnull: r.get::<_, i64>(3)? != 0,
            })
        })
        .map_err(map_err)?;
    rows.collect::<Result<_, _>>().map_err(map_err)
}

fn load_search_indexes(
    conn: &Connection,
    tables: &HashMap<String, Vec<ColumnInfo>>,
) -> Result<Vec<SearchIndexDef>, DbError> {
    let mut st = conn
        .prepare("SELECT name, table_name, columns, analyzer, scoring, created_index FROM _jamsil_search_indexes ORDER BY name")
        .map_err(map_err)?;
    let rows = st
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, i64>(5)?,
            ))
        })
        .map_err(map_err)?;
    let mut out = vec![];
    for r in rows {
        let (name, table, cols, analyzer, scoring, created) = r.map_err(map_err)?;
        let columns: Vec<String> = serde_json::from_str(&cols).unwrap_or_default();
        let pk_columns = tables
            .get(&table)
            .map(|cs| cs.iter().filter(|c| c.pk).map(|c| c.name.clone()).collect())
            .unwrap_or_default();
        out.push(SearchIndexDef {
            name,
            table,
            columns,
            pk_columns,
            analyzer,
            scoring,
            created_index: created as u64,
        });
    }
    Ok(out)
}

impl Catalog for CatalogCache {
    fn columns(&self, table: &str) -> Option<Vec<ColumnInfo>> {
        self.tables.get(&table.to_ascii_lowercase()).cloned()
    }
    fn search_indexes(&self, table: &str) -> Vec<SearchIndexRef> {
        self.search_for_table(table)
            .into_iter()
            .map(|s| SearchIndexRef {
                name: s.name.clone(),
                table: s.table.clone(),
                columns: s.columns.clone(),
                pk_columns: s.pk_columns.clone(),
            })
            .collect()
    }
    fn search_index_by_name(&self, name: &str) -> Option<SearchIndexRef> {
        self.search_def(name).map(|s| SearchIndexRef {
            name: s.name.clone(),
            table: s.table.clone(),
            columns: s.columns.clone(),
            pk_columns: s.pk_columns.clone(),
        })
    }
}

/// 테이블을 참조하는 뷰 (PostgreSQL처럼 의존 객체가 있으면 DROP을 거부하기 위함).
pub fn dependent_views(conn: &Connection, table: &str) -> Result<Vec<String>, DbError> {
    let mut st = conn
        .prepare("SELECT name, sql FROM sqlite_schema WHERE type = 'view'")
        .map_err(map_err)?;
    let rows = st
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?.unwrap_or_default())))
        .map_err(map_err)?;
    let t = table.to_ascii_lowercase();
    let mut out = vec![];
    for r in rows {
        let (name, sql) = r.map_err(map_err)?;
        let lower = sql.to_ascii_lowercase();
        // 식별자 경계로 테이블 이름 등장 여부 확인
        let mut found = false;
        let bytes = lower.as_bytes();
        let mut from = 0;
        while let Some(pos) = lower[from..].find(&t) {
            let a = from + pos;
            let b = a + t.len();
            let before = a == 0 || !(bytes[a - 1].is_ascii_alphanumeric() || bytes[a - 1] == b'_');
            let after = b >= bytes.len() || !(bytes[b].is_ascii_alphanumeric() || bytes[b] == b'_');
            if before && after {
                found = true;
                break;
            }
            from = b;
        }
        if found && !name.eq_ignore_ascii_case(table) {
            out.push(name);
        }
    }
    Ok(out)
}
