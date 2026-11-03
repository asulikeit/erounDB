//! PostgreSQL 타입 OID 매핑.

pub mod oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const CHAR: u32 = 18;
    pub const NAME: u32 = 19;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT2VECTOR: u32 = 22;
    pub const INT4: u32 = 23;
    pub const REGPROC: u32 = 24;
    pub const TEXT: u32 = 25;
    pub const OID: u32 = 26;
    pub const OIDVECTOR: u32 = 30;
    pub const JSON: u32 = 114;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const UNKNOWN: u32 = 705;
    pub const BOOL_ARRAY: u32 = 1000;
    pub const INT2_ARRAY: u32 = 1005;
    pub const INT4_ARRAY: u32 = 1007;
    pub const TEXT_ARRAY: u32 = 1009;
    pub const VARCHAR_ARRAY: u32 = 1015;
    pub const INT8_ARRAY: u32 = 1016;
    pub const FLOAT8_ARRAY: u32 = 1022;
    pub const OID_ARRAY: u32 = 1028;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const NUMERIC: u32 = 1700;
    pub const REGCLASS: u32 = 2205;
    pub const VOID: u32 = 2278;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;
}

/// pg_type 기본 정보: (oid, typname, typlen, typcategory, array oid).
pub const PG_TYPES: &[(u32, &str, i16, char, u32)] = &[
    (oid::BOOL, "bool", 1, 'B', oid::BOOL_ARRAY),
    (oid::BYTEA, "bytea", -1, 'U', 1001),
    (oid::CHAR, "char", 1, 'Z', 1002),
    (oid::NAME, "name", 64, 'S', 1003),
    (oid::INT8, "int8", 8, 'N', oid::INT8_ARRAY),
    (oid::INT2, "int2", 2, 'N', oid::INT2_ARRAY),
    (oid::INT2VECTOR, "int2vector", -1, 'A', 1006),
    (oid::INT4, "int4", 4, 'N', oid::INT4_ARRAY),
    (oid::REGPROC, "regproc", 4, 'N', 1008),
    (oid::TEXT, "text", -1, 'S', oid::TEXT_ARRAY),
    (oid::OID, "oid", 4, 'N', oid::OID_ARRAY),
    (oid::OIDVECTOR, "oidvector", -1, 'A', 1013),
    (oid::JSON, "json", -1, 'U', 199),
    (oid::FLOAT4, "float4", 4, 'N', 1021),
    (oid::FLOAT8, "float8", 8, 'N', oid::FLOAT8_ARRAY),
    (oid::UNKNOWN, "unknown", -2, 'X', 0),
    (oid::BOOL_ARRAY, "_bool", -1, 'A', 0),
    (oid::INT2_ARRAY, "_int2", -1, 'A', 0),
    (oid::INT4_ARRAY, "_int4", -1, 'A', 0),
    (oid::TEXT_ARRAY, "_text", -1, 'A', 0),
    (oid::VARCHAR_ARRAY, "_varchar", -1, 'A', 0),
    (oid::INT8_ARRAY, "_int8", -1, 'A', 0),
    (oid::FLOAT8_ARRAY, "_float8", -1, 'A', 0),
    (oid::OID_ARRAY, "_oid", -1, 'A', 0),
    (oid::BPCHAR, "bpchar", -1, 'S', 1014),
    (oid::VARCHAR, "varchar", -1, 'S', oid::VARCHAR_ARRAY),
    (oid::DATE, "date", 4, 'D', 1182),
    (oid::TIME, "time", 8, 'D', 1183),
    (oid::TIMESTAMP, "timestamp", 8, 'D', 1115),
    (oid::TIMESTAMPTZ, "timestamptz", 8, 'D', 1185),
    (oid::NUMERIC, "numeric", -1, 'N', 1231),
    (oid::REGCLASS, "regclass", 4, 'N', 2210),
    (oid::VOID, "void", 4, 'P', 0),
    (oid::UUID, "uuid", 16, 'U', 2951),
    (oid::JSONB, "jsonb", -1, 'U', 3807),
];

pub fn type_name(o: u32) -> &'static str {
    PG_TYPES
        .iter()
        .find(|t| t.0 == o)
        .map(|t| t.1)
        .unwrap_or("unknown")
}

pub fn type_oid_by_name(name: &str) -> Option<u32> {
    let n = name.trim().trim_matches('"').to_ascii_lowercase();
    let n = n.strip_prefix("pg_catalog.").unwrap_or(&n);
    let canon = match n {
        "integer" | "int" => "int4",
        "bigint" => "int8",
        "smallint" => "int2",
        "boolean" => "bool",
        "real" => "float4",
        "double precision" | "float" => "float8",
        "character varying" => "varchar",
        "character" => "bpchar",
        "timestamp without time zone" => "timestamp",
        "timestamp with time zone" => "timestamptz",
        "time without time zone" => "time",
        "decimal" => "numeric",
        o => o,
    };
    PG_TYPES.iter().find(|t| t.1 == canon).map(|t| t.0)
}

/// 파라미터 하나를 PG 타입 이름으로 표기 (format_type 용도).
pub fn format_type(o: u32, typmod: i32) -> String {
    match o {
        oid::VARCHAR if typmod > 4 => format!("character varying({})", typmod - 4),
        oid::VARCHAR => "character varying".into(),
        oid::BPCHAR if typmod > 4 => format!("character({})", typmod - 4),
        oid::BPCHAR => "character".into(),
        oid::INT4 => "integer".into(),
        oid::INT8 => "bigint".into(),
        oid::INT2 => "smallint".into(),
        oid::BOOL => "boolean".into(),
        oid::FLOAT4 => "real".into(),
        oid::FLOAT8 => "double precision".into(),
        oid::TIMESTAMP => "timestamp without time zone".into(),
        oid::TIMESTAMPTZ => "timestamp with time zone".into(),
        oid::TIME => "time without time zone".into(),
        o => type_name(o).into(),
    }
}

/// SQLite 선언 타입 문자열 → (PG OID, typmod).
///
/// jamsilDB DDL 변환 규칙상 `INTEGER`는 rowid alias(=SERIAL/INTEGER PRIMARY KEY)에만 쓰이므로 int8,
/// 일반 INTEGER 컬럼은 `INT`(int4)로 저장된다.
pub fn decl_to_oid(decl: &str) -> (u32, i32) {
    let up = decl.trim().to_ascii_uppercase();
    let (base, args) = match up.find('(') {
        Some(p) => (
            up[..p].trim().to_string(),
            up[p + 1..].trim_end_matches(')').trim().to_string(),
        ),
        None => (up.clone(), String::new()),
    };
    let n: i32 = args
        .split(',')
        .next()
        .and_then(|a| a.trim().parse().ok())
        .unwrap_or(-1);
    let typmod = if n >= 0 { n + 4 } else { -1 };
    let o = match base.as_str() {
        "INTEGER" => oid::INT8,
        "INT" | "INT4" | "MEDIUMINT" => oid::INT4,
        "SMALLINT" | "INT2" | "TINYINT" => oid::INT2,
        "BIGINT" | "INT8" => oid::INT8,
        "REAL" | "FLOAT4" => oid::FLOAT4,
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" | "FLOAT" => oid::FLOAT8,
        "BOOLEAN" | "BOOL" => oid::BOOL,
        "VARCHAR" | "CHARACTER VARYING" | "NVARCHAR" => return (oid::VARCHAR, typmod),
        "CHAR" | "CHARACTER" | "BPCHAR" | "NCHAR" => return (oid::BPCHAR, typmod),
        "TEXT" | "CLOB" => oid::TEXT,
        "BYTEA" | "BLOB" => oid::BYTEA,
        "DATE" => oid::DATE,
        "TIME" | "TIME WITHOUT TIME ZONE" => oid::TIME,
        "TIMESTAMP" | "DATETIME" | "TIMESTAMP WITHOUT TIME ZONE" => oid::TIMESTAMP,
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => oid::TIMESTAMPTZ,
        "UUID" => oid::UUID,
        "JSON" => oid::JSON,
        "JSONB" => oid::JSONB,
        "NUMERIC" | "DECIMAL" => oid::NUMERIC,
        "NAME" => oid::NAME,
        "OID" => oid::OID,
        "" => return (oid::TEXT, -1),
        b => {
            if b.contains("INT") {
                oid::INT8
            } else if b.contains("CHAR") || b.contains("CLOB") || b.contains("TEXT") {
                oid::TEXT
            } else if b.contains("BLOB") {
                oid::BYTEA
            } else if b.contains("REAL") || b.contains("FLOA") || b.contains("DOUB") {
                oid::FLOAT8
            } else {
                oid::TEXT
            }
        }
    };
    (o, -1)
}

/// 값 정규화·LIKE 규칙에 쓰는 선언 타입 분류.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    /// TEXT (32MiB, LIKE 금지)
    Text,
    Uuid,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Other,
}

impl ValueKind {
    /// 쓰기·비교 시 값을 정규화하는 SQLite 함수 이름.
    pub fn normalizer(&self) -> Option<&'static str> {
        Some(match self {
            ValueKind::Uuid => "_jamsil_norm_uuid",
            ValueKind::Date => "_jamsil_norm_date",
            ValueKind::Time => "_jamsil_norm_time",
            ValueKind::Timestamp => "_jamsil_norm_ts",
            ValueKind::TimestampTz => "_jamsil_norm_tstz",
            _ => return None,
        })
    }
}

pub fn decl_kind(decl: &str) -> ValueKind {
    let up = decl.trim().to_ascii_uppercase();
    let base = up.split('(').next().unwrap_or("").trim();
    match base {
        "TEXT" | "CLOB" => ValueKind::Text,
        "UUID" => ValueKind::Uuid,
        "DATE" => ValueKind::Date,
        "TIME" | "TIME WITHOUT TIME ZONE" => ValueKind::Time,
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" | "DATETIME" => ValueKind::Timestamp,
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => ValueKind::TimestampTz,
        _ => ValueKind::Other,
    }
}

/// TEXT 타입(32MiB 상한, LIKE 금지 대상)인지.
pub fn decl_is_text(decl: &str) -> bool {
    let up = decl.trim().to_ascii_uppercase();
    up == "TEXT" || up == "CLOB"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decl_mapping() {
        assert_eq!(decl_to_oid("VARCHAR(200)"), (oid::VARCHAR, 204));
        assert_eq!(decl_to_oid("INT").0, oid::INT4);
        assert_eq!(decl_to_oid("INTEGER").0, oid::INT8);
        assert_eq!(decl_to_oid("timestamp with time zone").0, oid::TIMESTAMPTZ);
        assert_eq!(decl_to_oid("TEXT").0, oid::TEXT);
        assert!(decl_is_text("text"));
        assert!(!decl_is_text("VARCHAR(10)"));
        assert_eq!(type_oid_by_name("pg_catalog.int4"), Some(oid::INT4));
    }
}
