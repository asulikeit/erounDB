use serde::{Deserialize, Serialize};

/// SQLite 저장 클래스에 대응하는 SQL 값.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
    Blob(#[serde(with = "serde_bytes")] Vec<u8>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
    pub fn approx_size(&self) -> usize {
        match self {
            Value::Null => 1,
            Value::Int(_) | Value::Real(_) => 8,
            Value::Text(s) => s.len(),
            Value::Blob(b) => b.len(),
        }
    }
}

/// 결과 컬럼 메타데이터.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDesc {
    pub name: String,
    /// PostgreSQL type OID.
    pub type_oid: u32,
    /// pg typlen (가변이면 -1).
    pub type_len: i16,
    /// typmod (VARCHAR(n) -> n+4, 없으면 -1).
    pub type_mod: i32,
    /// 원본 테이블 OID / 컬럼 번호 (알 수 없으면 0).
    pub table_oid: u32,
    pub column_attr: i16,
}

impl ColumnDesc {
    pub fn simple(name: impl Into<String>, type_oid: u32) -> Self {
        ColumnDesc {
            name: name.into(),
            type_oid,
            type_len: crate::value::type_len(type_oid),
            type_mod: -1,
            table_oid: 0,
            column_attr: 0,
        }
    }
}

/// PG 타입 길이 (고정 길이 타입).
pub fn type_len(oid: u32) -> i16 {
    match oid {
        16 => 1,
        21 => 2,
        23 | 26 | 700 | 1082 => 4,
        20 | 701 | 1083 | 1114 | 1184 => 8,
        2950 => 16,
        _ => -1,
    }
}

/// 문장 실행 결과.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryResult {
    /// 결과 집합이 있는 문장(SELECT 등)이면 Some.
    pub columns: Option<Vec<ColumnDesc>>,
    pub rows: Vec<Vec<Value>>,
    /// PG CommandComplete 태그 (예: "INSERT 0 1", "SELECT 3").
    pub tag: String,
    pub rows_affected: u64,
    /// 경고/알림 메시지 (NoticeResponse).
    pub notices: Vec<String>,
}

/// PK 값 목록 ↔ JSON (Outbox·검색 문서 키 공용 포맷).
pub fn pk_to_json(pk: &[Value]) -> String {
    let arr: Vec<serde_json::Value> = pk
        .iter()
        .map(|v| match v {
            Value::Null => serde_json::Value::Null,
            Value::Int(i) => serde_json::json!({"i": i}),
            Value::Real(f) => serde_json::json!({"r": f}),
            Value::Text(s) => serde_json::json!({"t": s}),
            Value::Blob(b) => serde_json::json!({"b": hex::encode(b)}),
        })
        .collect();
    serde_json::Value::Array(arr).to_string()
}

pub fn pk_from_json(s: &str) -> Result<Vec<Value>, crate::DbError> {
    let bad = |m: &str| crate::DbError::internal(format!("bad pk json: {m}"));
    let v: serde_json::Value = serde_json::from_str(s).map_err(|e| bad(&e.to_string()))?;
    let arr = v.as_array().ok_or_else(|| bad("not an array"))?;
    arr.iter()
        .map(|x| {
            if x.is_null() {
                return Ok(Value::Null);
            }
            if let Some(i) = x.get("i").and_then(|i| i.as_i64()) {
                return Ok(Value::Int(i));
            }
            if let Some(f) = x.get("r").and_then(|i| i.as_f64()) {
                return Ok(Value::Real(f));
            }
            if let Some(t) = x.get("t").and_then(|i| i.as_str()) {
                return Ok(Value::Text(t.to_string()));
            }
            if let Some(b) = x.get("b").and_then(|i| i.as_str()) {
                return Ok(Value::Blob(
                    hex::decode(b).map_err(|e| bad(&e.to_string()))?,
                ));
            }
            Err(bad("element"))
        })
        .collect()
}
