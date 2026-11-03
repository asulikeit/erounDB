//! 단일 SQLite 문장 실행과 결과 컬럼 타입 결정 (Writer/Reader 공용).

use rusqlite::Connection;

use jamsil_common::{ColumnDesc, DbError, QueryResult, Value};
use jamsil_sql::types::{decl_to_oid, oid};

use crate::convert::{bind_params, map_err, value_from_ref};

/// 결과 집합 하나의 최대 바이트 (클라이언트로 보내기 전 메모리 상한).
pub const MAX_RESULT_BYTES: usize = 512 * 1024 * 1024;

pub struct ExecOutput {
    pub columns: Option<Vec<ColumnDesc>>,
    pub rows: Vec<Vec<Value>>,
    pub changes: u64,
}

pub fn infer_oid(rows: &[Vec<Value>], col: usize) -> u32 {
    for r in rows {
        match r.get(col) {
            Some(Value::Int(_)) => return oid::INT8,
            Some(Value::Real(_)) => return oid::FLOAT8,
            Some(Value::Text(_)) => return oid::TEXT,
            Some(Value::Blob(_)) => return oid::BYTEA,
            _ => continue,
        }
    }
    oid::TEXT
}

pub fn column_descs(
    names: &[String],
    decls: &[Option<String>],
    hints: &[Option<u32>],
    rows: Option<&[Vec<Value>]>,
) -> Vec<ColumnDesc> {
    names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let (type_oid, typmod) = match decls.get(i).and_then(|d| d.as_ref()) {
                Some(d) if !d.is_empty() => decl_to_oid(d),
                _ => match hints.get(i).copied().flatten() {
                    Some(h) => (h, -1),
                    None => (rows.map(|r| infer_oid(r, i)).unwrap_or(oid::TEXT), -1),
                },
            };
            ColumnDesc {
                name: name.clone(),
                type_oid,
                type_len: jamsil_common::value::type_len(type_oid),
                type_mod: typmod,
                table_oid: 0,
                column_attr: 0,
            }
        })
        .collect()
}

/// 문장을 실행한다. 결과 컬럼이 있으면 모든 Row를 수집한다.
pub fn exec(
    conn: &Connection,
    sql: &str,
    params: &[Value],
    hints: &[Option<u32>],
) -> Result<ExecOutput, DbError> {
    let mut stmt = conn.prepare(sql).map_err(map_err)?;
    bind_params(&mut stmt, params)?;
    let ncol = stmt.column_count();
    if ncol == 0 {
        let changes = stmt.raw_execute().map_err(map_err)? as u64;
        return Ok(ExecOutput {
            columns: None,
            rows: vec![],
            changes,
        });
    }
    let names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let decls: Vec<Option<String>> = stmt
        .columns()
        .iter()
        .map(|c| c.decl_type().map(|s| s.to_string()))
        .collect();
    let readonly = stmt.readonly();
    let mut rows_out = Vec::new();
    let mut bytes = 0usize;
    {
        let mut rows = stmt.raw_query();
        while let Some(row) = rows.next().map_err(map_err)? {
            let mut r = Vec::with_capacity(ncol);
            for i in 0..ncol {
                let v = value_from_ref(row.get_ref(i).map_err(map_err)?)?;
                bytes += v.approx_size();
                r.push(v);
            }
            if bytes > MAX_RESULT_BYTES {
                return Err(DbError::new(
                    "",
                    "54000",
                    "result set exceeds server memory limit (512MiB); use LIMIT or fewer columns",
                ));
            }
            rows_out.push(r);
        }
    }
    let changes = if readonly { 0 } else { conn.changes() };
    let columns = column_descs(&names, &decls, hints, Some(&rows_out));
    Ok(ExecOutput {
        columns: Some(columns),
        rows: rows_out,
        changes,
    })
}

/// 실행하지 않고 파라미터 수와 결과 컬럼을 기술한다.
pub fn describe(
    conn: &Connection,
    sql: &str,
    hints: &[Option<u32>],
) -> Result<(usize, Option<Vec<ColumnDesc>>), DbError> {
    let stmt = conn.prepare(sql).map_err(map_err)?;
    let mut max_param = 0usize;
    for i in 1..=stmt.parameter_count() {
        let k = match stmt.parameter_name(i) {
            Some(n) => n
                .trim_start_matches(['$', '?', ':', '@'])
                .parse::<usize>()
                .unwrap_or(i),
            None => i,
        };
        max_param = max_param.max(k);
    }
    if stmt.column_count() == 0 {
        return Ok((max_param, None));
    }
    let names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let decls: Vec<Option<String>> = stmt
        .columns()
        .iter()
        .map(|c| c.decl_type().map(|s| s.to_string()))
        .collect();
    Ok((max_param, Some(column_descs(&names, &decls, hints, None))))
}

pub fn make_tag(prefix: &str, changes: u64, rows: usize, has_columns: bool) -> String {
    match prefix {
        "INSERT" => format!("INSERT 0 {changes}"),
        "UPDATE" => format!("UPDATE {changes}"),
        "DELETE" => format!("DELETE {changes}"),
        "SELECT" => format!("SELECT {rows}"),
        "EXPLAIN" => "EXPLAIN".into(),
        p if has_columns && p.is_empty() => format!("SELECT {rows}"),
        p => p.to_string(),
    }
}

pub fn to_result(tag_prefix: &str, out: ExecOutput) -> QueryResult {
    let has = out.columns.is_some();
    let tag = make_tag(tag_prefix, out.changes, out.rows.len(), has);
    QueryResult {
        columns: out.columns,
        rows_affected: if has && tag_prefix == "SELECT" {
            out.rows.len() as u64
        } else {
            out.changes
        },
        rows: out.rows,
        tag,
        notices: vec![],
    }
}
