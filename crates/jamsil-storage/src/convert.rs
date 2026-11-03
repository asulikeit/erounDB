//! rusqlite ↔ jamsilDB 값/오류 변환.

use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::{ffi, Statement};

use jamsil_common::{DbError, Value};

pub fn value_from_ref(v: ValueRef<'_>) -> Result<Value, DbError> {
    Ok(match v {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Int(i),
        ValueRef::Real(f) => Value::Real(f),
        ValueRef::Text(t) => {
            Value::Text(String::from_utf8(t.to_vec()).map_err(|_| DbError::invalid_utf8())?)
        }
        ValueRef::Blob(b) => Value::Blob(b.to_vec()),
    })
}

pub fn to_sql(v: &Value) -> ToSqlOutput<'_> {
    match v {
        Value::Null => ToSqlOutput::Borrowed(ValueRef::Null),
        Value::Int(i) => ToSqlOutput::Borrowed(ValueRef::Integer(*i)),
        Value::Real(f) => ToSqlOutput::Borrowed(ValueRef::Real(*f)),
        Value::Text(s) => ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes())),
        Value::Blob(b) => ToSqlOutput::Borrowed(ValueRef::Blob(b)),
    }
}

/// `$n` 이름 파라미터 또는 위치 파라미터 바인딩.
pub fn bind_params(stmt: &mut Statement<'_>, params: &[Value]) -> Result<(), DbError> {
    let n = stmt.parameter_count();
    for i in 1..=n {
        let idx = match stmt.parameter_name(i) {
            Some(name) => {
                let k = name.trim_start_matches(['$', '?', ':', '@']);
                match k.parse::<usize>() {
                    Ok(k) if k >= 1 => k - 1,
                    _ => {
                        return Err(DbError::new(
                            "",
                            "42P02",
                            format!("there is no parameter {name}"),
                        ));
                    }
                }
            }
            None => i - 1,
        };
        let v = params.get(idx).ok_or_else(|| {
            DbError::new(
                "",
                "08P01",
                format!(
                    "bind message supplies {} parameters, but statement requires more",
                    params.len()
                ),
            )
        })?;
        stmt.raw_bind_parameter(i, to_sql(v)).map_err(map_err)?;
    }
    Ok(())
}

/// SQLite 오류 → PG SQLSTATE.
pub fn map_err(e: rusqlite::Error) -> DbError {
    match &e {
        rusqlite::Error::SqliteFailure(fe, msg) => {
            let msg = msg.clone().unwrap_or_else(|| fe.to_string());
            map_sqlite(fe.extended_code, &msg)
        }
        rusqlite::Error::InvalidParameterCount(a, b) => DbError::new(
            "",
            "08P01",
            format!("bind message supplies {a} parameters, but statement requires {b}"),
        ),
        rusqlite::Error::Utf8Error(_) | rusqlite::Error::FromSqlConversionFailure(..) => {
            DbError::invalid_utf8()
        }
        other => DbError::internal(other.to_string()),
    }
}

pub fn map_sqlite(ext: i32, msg: &str) -> DbError {
    let primary = ext & 0xff;
    // 사용자 함수가 낸 "[SQLSTATE] 메시지" 형식 오류
    if let Some(rest) = msg.strip_prefix('[') {
        if let Some((state, text)) = rest.split_once("] ") {
            if state.len() == 5 && state.bytes().all(|b| b.is_ascii_alphanumeric()) {
                return DbError::new("", state, text.to_string());
            }
        }
    }
    let lower = msg.to_ascii_lowercase();
    match ext {
        ffi::SQLITE_CONSTRAINT_UNIQUE
        | ffi::SQLITE_CONSTRAINT_PRIMARYKEY
        | ffi::SQLITE_CONSTRAINT_ROWID => {
            return DbError::new(
                "",
                "23505",
                format!("duplicate key value violates unique constraint ({msg})"),
            );
        }
        ffi::SQLITE_CONSTRAINT_NOTNULL => {
            return DbError::new(
                "",
                "23502",
                format!("null value violates not-null constraint ({msg})"),
            );
        }
        ffi::SQLITE_CONSTRAINT_FOREIGNKEY => {
            return DbError::new(
                "",
                "23503",
                "insert or update violates foreign key constraint",
            );
        }
        ffi::SQLITE_CONSTRAINT_CHECK => {
            // jamsilDB 타입 계약 제약: CONSTRAINT "_jamsil_<종류>_<컬럼>"
            if let Some(name) = msg.split("CHECK constraint failed: ").nth(1) {
                let name = name.trim().trim_matches('"');
                if let Some(rest) = name.strip_prefix("_jamsil_") {
                    if let Some((kind, col)) = rest.split_once('_') {
                        let (state, what) = match kind {
                            "int2" => ("22003", "smallint out of range or not an integer"),
                            "int4" => ("22003", "integer out of range or not an integer"),
                            "int8" => ("22003", "bigint out of range or not an integer"),
                            "float" => ("22P02", "invalid input syntax for type double precision"),
                            "bool" => ("22P02", "invalid input syntax for type boolean"),
                            "len" => ("22001", "value too long for type character varying"),
                            "json" => ("22P02", "invalid input syntax for type json"),
                            "uuid" => ("22P02", "invalid input syntax for type uuid"),
                            "date" => ("22007", "invalid value for type date"),
                            "time" => ("22007", "invalid value for type time"),
                            "ts" => ("22007", "invalid value for type timestamp"),
                            "tstz" => ("22007", "invalid value for type timestamp with time zone"),
                            _ => ("23514", "new row violates check constraint"),
                        };
                        return DbError::new("", state, format!("{what} (column \"{col}\")"));
                    }
                }
            }
            if lower.contains("length(") {
                return DbError::new(
                    "",
                    "22001",
                    format!("value too long for type character varying ({msg})"),
                );
            }
            if lower.contains("between -2147483648") || lower.contains("between -32768") {
                return DbError::new("", "22003", format!("integer out of range ({msg})"));
            }
            if lower.contains("in (0, 1)") {
                return DbError::new(
                    "",
                    "22P02",
                    format!("invalid input syntax for type boolean ({msg})"),
                );
            }
            if lower.contains("json_valid") {
                return DbError::new(
                    "",
                    "22P02",
                    format!("invalid input syntax for type json ({msg})"),
                );
            }
            return DbError::new(
                "",
                "23514",
                format!("new row violates check constraint ({msg})"),
            );
        }
        _ => {}
    }
    match primary {
        ffi::SQLITE_TOOBIG => DbError::row_too_large(msg.to_string()),
        ffi::SQLITE_FULL => {
            DbError::new("", "53100", format!("could not write: disk full ({msg})"))
        }
        ffi::SQLITE_IOERR => DbError::new("", "58030", format!("I/O error: {msg}")),
        ffi::SQLITE_BUSY | ffi::SQLITE_LOCKED => {
            DbError::new("", "55P03", format!("lock not available: {msg}"))
        }
        ffi::SQLITE_CORRUPT | ffi::SQLITE_NOTADB => {
            DbError::new("", "XX001", format!("data corrupted: {msg}"))
        }
        ffi::SQLITE_READONLY => DbError::read_only(msg.to_string()),
        ffi::SQLITE_INTERRUPT => {
            DbError::new("", "57014", "canceling statement due to user request")
        }
        ffi::SQLITE_RANGE => DbError::new("", "08P01", msg.to_string()),
        ffi::SQLITE_MISMATCH => DbError::new("", "42804", format!("datatype mismatch: {msg}")),
        ffi::SQLITE_CONSTRAINT => DbError::new("", "23000", msg.to_string()),
        _ => {
            let (state, m) = if lower.starts_with("no such table") {
                (
                    "42P01",
                    msg.replace("no such table", "relation does not exist"),
                )
            } else if lower.starts_with("no such column") {
                (
                    "42703",
                    msg.replace("no such column", "column does not exist"),
                )
            } else if lower.starts_with("no such function") {
                (
                    "42883",
                    msg.replace("no such function", "function does not exist"),
                )
            } else if lower.starts_with("no such index") {
                ("42704", msg.to_string())
            } else if lower.contains("already exists") {
                ("42P07", msg.to_string())
            } else if lower.contains("syntax error")
                || lower.starts_with("near ")
                || lower.contains("incomplete input")
            {
                ("42601", msg.to_string())
            } else if lower.contains("ambiguous column") {
                ("42702", msg.to_string())
            } else if lower.contains("misuse of aggregate")
                || lower.contains("wrong number of arguments")
            {
                ("42803", msg.to_string())
            } else if lower.contains("too many sql variables") {
                ("54000", msg.to_string())
            } else if lower.contains("string or blob too big") {
                return DbError::row_too_large(msg.to_string());
            } else {
                ("XX000", msg.to_string())
            };
            DbError::new("", state, m)
        }
    }
}

pub use jamsil_common::value::{pk_from_json, pk_to_json};
