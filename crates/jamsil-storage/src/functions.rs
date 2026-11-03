//! SQLite에 등록하는 PostgreSQL 호환 함수.

use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::functions::{Aggregate, Context, FunctionFlags};
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{Connection, Result};

use jamsil_sql::types::{format_type, type_oid_by_name};

thread_local! {
    /// 현재 Reader 스레드에서 실행 중인 질의의 취소 플래그 (pg_sleep 등 장시간 함수가 확인).
    pub static CANCEL_FLAG: std::cell::RefCell<Option<std::sync::Arc<std::sync::atomic::AtomicBool>>> =
        const { std::cell::RefCell::new(None) };
}

thread_local! {
    /// 현재 실행 중인 문장의 사용자와 권한, 카탈로그 테이블 이름 (current_user / has_table_privilege).
    pub static ACL_CTX: std::cell::RefCell<Option<AclCtx>> = const { std::cell::RefCell::new(None) };
}

#[derive(Clone)]
pub struct AclCtx {
    pub user: String,
    pub principal: Option<jamsil_common::acl::Principal>,
    pub tables: std::sync::Arc<Vec<String>>,
}

/// 문장 실행 동안 ACL 문맥을 설정한다 (drop 시 해제).
pub struct AclScope;

impl AclScope {
    pub fn enter(ctx: AclCtx) -> AclScope {
        ACL_CTX.with(|c| *c.borrow_mut() = Some(ctx));
        AclScope
    }
}

impl Drop for AclScope {
    fn drop(&mut self) {
        ACL_CTX.with(|c| *c.borrow_mut() = None);
    }
}

fn acl_user(default: &str) -> String {
    ACL_CTX.with(|c| {
        c.borrow()
            .as_ref()
            .map(|a| a.user.clone())
            .filter(|u| !u.is_empty())
            .unwrap_or_else(|| default.to_string())
    })
}

/// has_*_privilege: 대상 테이블(이름 또는 OID)과 권한 문자열로 판정.
fn acl_table_privilege(table: Option<SqlValue>, privs: &str) -> bool {
    ACL_CTX.with(|c| {
        let c = c.borrow();
        let Some(ctx) = c.as_ref() else { return true };
        let Some(p) = &ctx.principal else { return true };
        let name = match table {
            Some(SqlValue::Text(t)) => {
                let t = t.trim().trim_matches('"');
                t.rsplit('.').next().unwrap_or(t).trim_matches('"').to_ascii_lowercase()
            }
            Some(SqlValue::Integer(oid)) => {
                match ctx.tables.iter().find(|n| stable_oid("r", n) == oid || stable_oid("v", n) == oid) {
                    Some(n) => n.to_ascii_lowercase(),
                    // 사용자 테이블이 아닌 OID (시스템 카탈로그 등)
                    None => return privs.split(',').all(|x| x.trim().eq_ignore_ascii_case("SELECT")),
                }
            }
            _ => return false,
        };
        p.has_table_privilege(&name, privs)
    })
}

fn canceled() -> bool {
    CANCEL_FLAG.with(|f| f.borrow().as_ref().map(|a| a.load(std::sync::atomic::Ordering::Relaxed)).unwrap_or(false))
}

pub const NS_PG_CATALOG: i64 = 11;
pub const NS_PUBLIC: i64 = 2200;
pub const NS_INFORMATION_SCHEMA: i64 = 13000;
pub const NS_JAMSIL_CATALOG: i64 = 13100;

/// 테이블·인덱스·컬럼 등의 안정적 OID (모든 노드에서 같은 값).
pub fn stable_oid(kind: &str, name: &str) -> i64 {
    let mut h = crc32fast::Hasher::new();
    h.update(kind.as_bytes());
    h.update(b":");
    h.update(name.to_ascii_lowercase().as_bytes());
    16384 + (h.finalize() as i64 % 1_000_000_000)
}

pub fn now_timestamptz() -> String {
    let now = time::OffsetDateTime::now_utc();
    // TIMESTAMPTZ 정규형 (소수점 이하 끝자리 0 제거)
    let mut s = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        now.year(),
        now.month() as u8,
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    );
    if now.microsecond() != 0 {
        s.push_str(format!(".{:06}", now.microsecond()).trim_end_matches('0'));
    }
    s.push_str("+00");
    s
}

fn text_arg(ctx: &Context<'_>, i: usize) -> Option<String> {
    match ctx.get_raw(i) {
        ValueRef::Null => None,
        ValueRef::Text(t) => Some(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Integer(n) => Some(n.to_string()),
        ValueRef::Real(f) => Some(f.to_string()),
        ValueRef::Blob(b) => Some(String::from_utf8_lossy(b).into_owned()),
    }
}

fn int_arg(ctx: &Context<'_>, i: usize) -> Option<i64> {
    match ctx.get_raw(i) {
        ValueRef::Integer(n) => Some(n),
        ValueRef::Real(f) => Some(f as i64),
        ValueRef::Text(t) => std::str::from_utf8(t)
            .ok()
            .and_then(|s| s.trim().parse().ok()),
        _ => None,
    }
}

/// PG 불리언 입력 해석.
pub fn parse_bool(v: ValueRef<'_>) -> Option<i64> {
    match v {
        ValueRef::Null => None,
        ValueRef::Integer(i) => Some((i != 0) as i64),
        ValueRef::Real(f) => Some((f != 0.0) as i64),
        ValueRef::Text(t) | ValueRef::Blob(t) => {
            let s = String::from_utf8_lossy(t).trim().to_ascii_lowercase();
            match s.as_str() {
                "t" | "true" | "y" | "yes" | "on" | "1" => Some(1),
                "f" | "false" | "n" | "no" | "off" | "0" => Some(0),
                _ => None,
            }
        }
    }
}

/// "YYYY-MM-DD[ HH:MM:SS[.ffffff]][+TZ]" 파싱 → (y, m, d, H, M, S(float), tz_offset_secs)
pub fn parse_ts(s: &str) -> Option<(i64, i64, i64, i64, i64, f64, i64)> {
    let s = s.trim();
    let (date, rest) = match s.find([' ', 'T']) {
        Some(p) => (&s[..p], s[p + 1..].trim()),
        None => (s, ""),
    };
    let mut dp = date.split('-');
    let y: i64 = dp.next()?.parse().ok()?;
    let m: i64 = dp.next()?.parse().ok()?;
    let d: i64 = dp.next()?.parse().ok()?;
    if rest.is_empty() {
        return Some((y, m, d, 0, 0, 0.0, 0));
    }
    let (time_part, tz) = match rest.find(['+', '-', 'Z']) {
        Some(p) => (&rest[..p], &rest[p..]),
        None => (rest, ""),
    };
    let mut tp = time_part.split(':');
    let hh: i64 = tp.next()?.trim().parse().ok()?;
    let mi: i64 = tp.next().unwrap_or("0").trim().parse().ok()?;
    let ss: f64 = tp.next().unwrap_or("0").trim().parse().ok()?;
    let tzs = if tz.is_empty() || tz == "Z" {
        0
    } else {
        let sign = if tz.starts_with('-') { -1 } else { 1 };
        let body = &tz[1..];
        let mut parts = body.split(':');
        let h: i64 = parts.next().unwrap_or("0").parse().unwrap_or(0);
        let mm: i64 = parts.next().unwrap_or("0").parse().unwrap_or(0);
        sign * (h * 3600 + mm * 60)
    };
    Some((y, m, d, hh, mi, ss, tzs))
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn extract(field: &str, ts: &str) -> Option<f64> {
    let (y, m, d, hh, mi, ss, tz) = parse_ts(ts)?;
    let days = days_from_civil(y, m, d);
    Some(match field {
        "year" => y as f64,
        "month" => m as f64,
        "day" => d as f64,
        "hour" => hh as f64,
        "minute" => mi as f64,
        "second" => ss,
        "milliseconds" | "millisecond" => ss * 1000.0,
        "microseconds" | "microsecond" => ss * 1_000_000.0,
        "quarter" => ((m - 1) / 3 + 1) as f64,
        "dow" => ((days + 4).rem_euclid(7)) as f64,
        "isodow" => {
            let w = (days + 4).rem_euclid(7);
            if w == 0 {
                7.0
            } else {
                w as f64
            }
        }
        "doy" => (days - days_from_civil(y, 1, 1) + 1) as f64,
        "epoch" => (days * 86400 + hh * 3600 + mi * 60 - tz) as f64 + ss,
        "century" => ((y - 1) / 100 + 1) as f64,
        "decade" => (y / 10) as f64,
        _ => return None,
    })
}

struct ArrayAgg;

impl Aggregate<Vec<String>, Option<String>> for ArrayAgg {
    fn init(&self, _: &mut Context<'_>) -> Result<Vec<String>> {
        Ok(vec![])
    }
    fn step(&self, ctx: &mut Context<'_>, acc: &mut Vec<String>) -> Result<()> {
        acc.push(match text_arg(ctx, 0) {
            None => "NULL".into(),
            Some(s) => {
                if s.is_empty() || s.contains([',', '{', '}', '"', ' ', '\\']) {
                    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
                } else {
                    s
                }
            }
        });
        Ok(())
    }
    fn finalize(&self, _: &mut Context<'_>, acc: Option<Vec<String>>) -> Result<Option<String>> {
        Ok(acc.map(|a| format!("{{{}}}", a.join(","))))
    }
}

pub struct FunctionEnv {
    pub database: String,
    pub user: String,
    pub start_time: String,
}

pub fn register(conn: &Connection, env: &FunctionEnv) -> Result<()> {
    let det = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
    let nondet = FunctionFlags::SQLITE_UTF8;

    for name in [
        "now",
        "transaction_timestamp",
        "statement_timestamp",
        "clock_timestamp",
    ] {
        conn.create_scalar_function(name, 0, nondet, |_| Ok(now_timestamptz()))?;
    }
    conn.create_scalar_function("version", 0, det, |_| {
        Ok(format!(
            "PostgreSQL 14.0 (jamsilDB {} on SQLite {}, Rust)",
            jamsil_common::build_info(),
            rusqlite::version()
        ))
    })?;
    let db = env.database.clone();
    conn.create_scalar_function("current_database", 0, det, move |_| Ok(db.clone()))?;
    let db = env.database.clone();
    conn.create_scalar_function("current_catalog", 0, det, move |_| Ok(db.clone()))?;
    conn.create_scalar_function("current_schema", 0, det, |_| Ok("public".to_string()))?;
    conn.create_scalar_function("_jamsil_current_schema", 0, det, |_| Ok("public".to_string()))?;
    let db = env.database.clone();
    conn.create_scalar_function("_jamsil_current_catalog", 0, det, move |_| Ok(db.clone()))?;
    // 사용자 함수는 문장마다 값이 다르므로 deterministic이 아니다.
    for name in ["current_user", "session_user", "user", "current_role", "_jamsil_user"] {
        let u = env.user.clone();
        conn.create_scalar_function(name, 0, FunctionFlags::SQLITE_UTF8, move |_| Ok(acl_user(&u)))?;
    }
    conn.create_scalar_function("current_schemas", 1, det, |ctx| {
        let implicit = parse_bool(ctx.get_raw(0)).unwrap_or(0) == 1;
        Ok(if implicit {
            "{pg_catalog,public}".to_string()
        } else {
            "{public}".to_string()
        })
    })?;
    conn.create_scalar_function("pg_backend_pid", 0, det, |_| Ok(std::process::id() as i64))?;
    let st = env.start_time.clone();
    conn.create_scalar_function("pg_postmaster_start_time", 0, det, move |_| Ok(st.clone()))?;
    conn.create_scalar_function("pg_is_in_recovery", 0, det, |_| Ok(0i64))?;
    for (name, n) in [
        ("obj_description", 1),
        ("obj_description", 2),
        ("col_description", 2),
        ("shobj_description", 2),
        ("pg_get_constraintdef", 1),
        ("pg_get_constraintdef", 2),
        ("pg_get_viewdef", 1),
        ("pg_get_viewdef", 2),
        ("pg_get_triggerdef", 1),
        ("pg_get_function_identity_arguments", 1),
        ("inet_server_addr", 0),
        ("inet_server_port", 0),
        ("pg_get_serial_sequence", 2),
        ("pg_get_partkeydef", 1),
        ("pg_get_ruledef", 1),
        ("pg_get_ruledef", 2),
        ("pg_get_function_arguments", 1),
        ("pg_get_function_result", 1),
        ("pg_get_functiondef", 1),
        ("pg_get_statisticsobjdef", 1),
    ] {
        conn.create_scalar_function(name, n, det, |_| Ok(None::<String>))?;
    }
    conn.create_scalar_function("pg_get_expr", -1, det, |ctx| Ok(text_arg(ctx, 0)))?;
    conn.create_scalar_function("format_type", 2, det, |ctx| {
        let o = int_arg(ctx, 0);
        let m = int_arg(ctx, 1).unwrap_or(-1);
        Ok(o.map(|o| format_type(o as u32, m as i32)))
    })?;
    let u = env.user.clone();
    conn.create_scalar_function("pg_get_userbyid", 1, det, move |_| Ok(u.clone()))?;
    // has_table_privilege([user,] table, privilege) / has_column_privilege([user,] table, column, privilege)
    conn.create_scalar_function("has_table_privilege", -1, FunctionFlags::SQLITE_UTF8, |ctx| {
        let n = ctx.len();
        if n < 2 {
            return Ok(0i64);
        }
        let table: Option<SqlValue> = ctx.get(n - 2).ok();
        let privs = text_arg(ctx, n - 1).unwrap_or_default();
        Ok(acl_table_privilege(table, &privs) as i64)
    })?;
    conn.create_scalar_function("has_column_privilege", -1, FunctionFlags::SQLITE_UTF8, |ctx| {
        let n = ctx.len();
        if n < 3 {
            return Ok(0i64);
        }
        let table: Option<SqlValue> = ctx.get(n - 3).ok();
        let privs = text_arg(ctx, n - 1).unwrap_or_default();
        Ok(acl_table_privilege(table, &privs) as i64)
    })?;
    for name in [
        "has_schema_privilege",
        "has_database_privilege",
        "has_function_privilege",
        "has_sequence_privilege",
        "pg_table_is_visible",
        "pg_type_is_visible",
        "pg_function_is_visible",
    ] {
        conn.create_scalar_function(name, -1, det, |_| Ok(1i64))?;
    }
    conn.create_scalar_function("pg_encoding_to_char", 1, det, |_| Ok("UTF8".to_string()))?;
    for name in [
        "pg_total_relation_size",
        "pg_relation_size",
        "pg_table_size",
        "pg_indexes_size",
        "pg_database_size",
        "pg_stat_get_numscans",
        "pg_stat_get_tuples_returned",
        "pg_stat_get_live_tuples",
    ] {
        conn.create_scalar_function(name, 1, det, |_| Ok(0i64))?;
    }
    conn.create_scalar_function("pg_size_pretty", 1, det, |ctx| {
        Ok(int_arg(ctx, 0).map(|n| {
            if n < 10 * 1024 {
                format!("{n} bytes")
            } else if n < 10 * 1024 * 1024 {
                format!("{} kB", n / 1024)
            } else if n < 10 * 1024 * 1024 * 1024 {
                format!("{} MB", n / (1024 * 1024))
            } else {
                format!("{} GB", n / (1024 * 1024 * 1024))
            }
        }))
    })?;
    conn.create_scalar_function("quote_ident", 1, det, |ctx| {
        Ok(text_arg(ctx, 0).map(|s| {
            let simple = !s.is_empty()
                && s.chars()
                    .next()
                    .map(|c| c.is_ascii_lowercase() || c == '_')
                    .unwrap_or(false)
                && s.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            if simple {
                s
            } else {
                format!("\"{}\"", s.replace('"', "\"\""))
            }
        }))
    })?;
    conn.create_scalar_function("array_to_string", 2, det, |ctx| {
        let a = text_arg(ctx, 0);
        let sep = text_arg(ctx, 1).unwrap_or_default();
        Ok(a.map(|a| {
            let inner = a.trim().trim_start_matches('{').trim_end_matches('}');
            inner
                .split(',')
                .map(|x| x.trim_matches('"'))
                .filter(|x| *x != "NULL")
                .collect::<Vec<_>>()
                .join(&sep)
        }))
    })?;
    conn.create_scalar_function("_jamsil_bool", 1, det, |ctx| Ok(parse_bool(ctx.get_raw(0))))?;
    conn.create_scalar_function("_jamsil_regtype", 1, det, |ctx| {
        Ok(match ctx.get_raw(0) {
            ValueRef::Integer(i) => Some(i),
            _ => text_arg(ctx, 0)
                .and_then(|s| type_oid_by_name(&s))
                .map(|o| o as i64),
        })
    })?;
    conn.create_scalar_function("_jamsil_extract", 2, det, |ctx| {
        let f = text_arg(ctx, 0).unwrap_or_default();
        Ok(text_arg(ctx, 1).and_then(|ts| extract(&f, &ts)))
    })?;
    conn.create_scalar_function("date_part", 2, det, |ctx| {
        let f = text_arg(ctx, 0).unwrap_or_default().to_ascii_lowercase();
        Ok(text_arg(ctx, 1).and_then(|ts| extract(&f, &ts)))
    })?;
    conn.create_scalar_function("_jamsil_oid", 2, det, |ctx| {
        let k = text_arg(ctx, 0).unwrap_or_default();
        let n = text_arg(ctx, 1).unwrap_or_default();
        Ok(stable_oid(&k, &n))
    })?;
    conn.create_scalar_function("_jamsil_decl_oid", 1, det, |ctx| {
        Ok(jamsil_sql::types::decl_to_oid(&text_arg(ctx, 0).unwrap_or_default()).0 as i64)
    })?;
    conn.create_scalar_function("_jamsil_decl_typmod", 1, det, |ctx| {
        Ok(jamsil_sql::types::decl_to_oid(&text_arg(ctx, 0).unwrap_or_default()).1 as i64)
    })?;
    conn.create_scalar_function("_jamsil_type_name", 1, det, |ctx| {
        Ok(jamsil_sql::types::type_name(int_arg(ctx, 0).unwrap_or(0) as u32).to_string())
    })?;
    conn.create_scalar_function("_jamsil_format_decl", 1, det, |ctx| {
        let (o, m) = jamsil_sql::types::decl_to_oid(&text_arg(ctx, 0).unwrap_or_default());
        Ok(format_type(o, m))
    })?;
    // PG 정규식 연산자 (~, ~*, !~, !~*) 지원: regexp(pattern, value), regexp_i(pattern, value)
    for (name, ci) in [("regexp", false), ("_jamsil_regexp_i", true)] {
        conn.create_scalar_function(name, 2, det, move |ctx| {
            let (Some(p), Some(v)) = (text_arg(ctx, 0), text_arg(ctx, 1)) else {
                return Ok(None);
            };
            let re = regex::RegexBuilder::new(&p)
                .case_insensitive(ci)
                .size_limit(1 << 20)
                .build()
                .map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))?;
            Ok(Some(re.is_match(&v) as i64))
        })?;
    }
    // PG 배열/int2vector 텍스트 ('1 2' 또는 '{1,2}') 보조 함수
    fn arr_items(s: &str) -> Vec<String> {
        s.trim()
            .trim_start_matches('{')
            .trim_end_matches('}')
            .split([',', ' '])
            .filter(|x| !x.is_empty())
            .map(|x| x.trim_matches('"').to_string())
            .collect()
    }
    conn.create_scalar_function("_jamsil_array_pos", 2, det, |ctx| {
        let (Some(a), Some(v)) = (text_arg(ctx, 0), text_arg(ctx, 1)) else {
            return Ok(None);
        };
        Ok(arr_items(&a)
            .iter()
            .position(|x| *x == v)
            .map(|p| p as i64 + 1))
    })?;
    conn.create_scalar_function("_jamsil_arr_get", 2, det, |ctx| {
        let (Some(a), Some(i)) = (text_arg(ctx, 0), int_arg(ctx, 1)) else {
            return Ok(None);
        };
        Ok(arr_items(&a)
            .get((i - 1).max(0) as usize)
            .map(|x| match x.parse::<i64>() {
                Ok(n) => SqlValue::Integer(n),
                Err(_) => SqlValue::Text(x.clone()),
            }))
    })?;
    conn.create_scalar_function("_jamsil_series_json", -1, det, |ctx| {
        let (a, b) = if ctx.len() >= 2 {
            (int_arg(ctx, 0).unwrap_or(1), int_arg(ctx, 1).unwrap_or(0))
        } else {
            (1, int_arg(ctx, 0).unwrap_or(0))
        };
        let b = b.min(a.saturating_add(100_000));
        Ok(format!(
            "[{}]",
            (a..=b).map(|i| i.to_string()).collect::<Vec<_>>().join(",")
        ))
    })?;
    for name in ["array_upper", "array_length", "cardinality"] {
        conn.create_scalar_function(name, -1, det, |ctx| {
            Ok(text_arg(ctx, 0).map(|a| arr_items(&a).len() as i64))
        })?;
    }
    // 타입 정규화 (쓰기·비교 값) 와 정규형 검사 (CHECK 제약)
    for kind in ["uuid", "date", "time", "ts", "tstz"] {
        conn.create_scalar_function(&format!("_jamsil_norm_{kind}"), 1, det, move |ctx| {
            crate::typeval::normalize(kind, ctx.get_raw(0))
                .map_err(|e| rusqlite::Error::UserFunctionError(e.into()))
        })?;
        conn.create_scalar_function(&format!("_jamsil_canon_{kind}"), 1, det, move |ctx| {
            Ok(crate::typeval::is_canonical(kind, ctx.get_raw(0)) as i64)
        })?;
    }
    conn.create_aggregate_function("_jamsil_array_agg", 1, det, ArrayAgg)?;
    for name in ["gen_random_uuid", "uuid_generate_v4"] {
        conn.create_scalar_function(name, 0, nondet, |_| Ok(uuid::Uuid::new_v4().to_string()))?;
    }
    conn.create_scalar_function("left", 2, det, |ctx| {
        let s = text_arg(ctx, 0);
        let n = int_arg(ctx, 1).unwrap_or(0);
        Ok(s.map(|s| {
            let len = s.chars().count() as i64;
            let take = if n >= 0 { n.min(len) } else { (len + n).max(0) };
            s.chars().take(take as usize).collect::<String>()
        }))
    })?;
    conn.create_scalar_function("right", 2, det, |ctx| {
        let s = text_arg(ctx, 0);
        let n = int_arg(ctx, 1).unwrap_or(0);
        Ok(s.map(|s| {
            let len = s.chars().count() as i64;
            let take = if n >= 0 { n.min(len) } else { (len + n).max(0) };
            s.chars().skip((len - take) as usize).collect::<String>()
        }))
    })?;
    conn.create_scalar_function("repeat", 2, det, |ctx| {
        let s = text_arg(ctx, 0);
        let n = int_arg(ctx, 1).unwrap_or(0).max(0) as usize;
        match s {
            None => Ok(None),
            Some(s) => {
                if s.len().saturating_mul(n) > 64 * 1024 * 1024 {
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_TOOBIG),
                        Some("string or blob too big".into()),
                    ));
                }
                Ok(Some(s.repeat(n)))
            }
        }
    })?;
    for (name, left) in [("lpad", true), ("rpad", false)] {
        conn.create_scalar_function(name, -1, det, move |ctx| {
            let s = text_arg(ctx, 0);
            let n = int_arg(ctx, 1).unwrap_or(0).max(0) as usize;
            let fill = if ctx.len() > 2 {
                text_arg(ctx, 2).unwrap_or_default()
            } else {
                " ".into()
            };
            Ok(s.map(|s| {
                let len = s.chars().count();
                if len >= n || fill.is_empty() {
                    return s.chars().take(n).collect::<String>();
                }
                let pad: String = fill.chars().cycle().take(n - len).collect();
                if left {
                    format!("{pad}{s}")
                } else {
                    format!("{s}{pad}")
                }
            }))
        })?;
    }
    conn.create_scalar_function("split_part", 3, det, |ctx| {
        let s = text_arg(ctx, 0);
        let d = text_arg(ctx, 1).unwrap_or_default();
        let n = int_arg(ctx, 2).unwrap_or(1);
        Ok(s.map(|s| {
            if d.is_empty() {
                return if n == 1 { s } else { String::new() };
            }
            let parts: Vec<&str> = s.split(d.as_str()).collect();
            let idx = if n > 0 { n - 1 } else { parts.len() as i64 + n };
            parts
                .get(idx as usize)
                .map(|x| x.to_string())
                .unwrap_or_default()
        }))
    })?;
    conn.create_scalar_function("initcap", 1, det, |ctx| {
        Ok(text_arg(ctx, 0).map(|s| {
            let mut out = String::with_capacity(s.len());
            let mut start = true;
            for c in s.chars() {
                if c.is_alphanumeric() {
                    if start {
                        out.extend(c.to_uppercase());
                    } else {
                        out.extend(c.to_lowercase());
                    }
                    start = false;
                } else {
                    out.push(c);
                    start = true;
                }
            }
            out
        }))
    })?;
    conn.create_scalar_function("reverse", 1, det, |ctx| {
        Ok(text_arg(ctx, 0).map(|s| s.chars().rev().collect::<String>()))
    })?;
    conn.create_scalar_function("to_hex", 1, det, |ctx| {
        Ok(int_arg(ctx, 0).map(|n| format!("{n:x}")))
    })?;
    conn.create_scalar_function("ascii", 1, det, |ctx| {
        Ok(text_arg(ctx, 0).and_then(|s| s.chars().next().map(|c| c as i64)))
    })?;
    conn.create_scalar_function("chr", 1, det, |ctx| {
        Ok(int_arg(ctx, 0)
            .and_then(|n| char::from_u32(n as u32))
            .map(|c| c.to_string()))
    })?;
    conn.create_scalar_function("pg_sleep", 1, nondet, |ctx| {
        let secs = match ctx.get_raw(0) {
            ValueRef::Integer(i) => i as f64,
            ValueRef::Real(f) => f,
            _ => 0.0,
        };
        let until = std::time::Instant::now() + std::time::Duration::from_secs_f64(secs.clamp(0.0, 300.0));
        while std::time::Instant::now() < until {
            if canceled() {
                return Err(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_INTERRUPT),
                    Some("interrupted".into()),
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        Ok(SqlValue::Null)
    })?;
    conn.create_scalar_function("set_config", 3, nondet, |ctx| Ok(text_arg(ctx, 1)))?;
    conn.create_scalar_function("current_setting", -1, det, |ctx| {
        let n = text_arg(ctx, 0).unwrap_or_default().to_ascii_lowercase();
        Ok(match n.as_str() {
            "server_version" => Some("14.0".to_string()),
            "server_version_num" => Some("140000".to_string()),
            "server_encoding" | "client_encoding" => Some("UTF8".to_string()),
            "standard_conforming_strings" => Some("on".to_string()),
            "datestyle" => Some("ISO, MDY".to_string()),
            "timezone" => Some("UTC".to_string()),
            "search_path" => Some("public".to_string()),
            "max_identifier_length" => Some("63".to_string()),
            "transaction_isolation" | "default_transaction_isolation" => {
                Some("serializable".to_string())
            }
            _ => None,
        })
    })?;
    Ok(())
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_fields() {
        assert_eq!(extract("year", "2024-03-05 10:20:30.5+00"), Some(2024.0));
        assert_eq!(extract("second", "2024-03-05 10:20:30.5"), Some(30.5));
        assert_eq!(extract("epoch", "1970-01-02"), Some(86400.0));
        assert_eq!(extract("dow", "2024-03-05"), Some(2.0));
    }
}
