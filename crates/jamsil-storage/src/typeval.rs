//! UUID·날짜·시각 값의 검증과 정규형 (설계서 9.2, RQ-04/05).
//!
//! 정규형(같은 값 = 같은 문자열, 문자열 순서 = 시간 순서):
//! * UUID: 소문자 8-4-4-4-12
//! * DATE: `YYYY-MM-DD`
//! * TIME: `HH:MM:SS[.ffffff]` (소수점 이하 끝자리 0 제거)
//! * TIMESTAMP: `YYYY-MM-DD HH:MM:SS[.ffffff]` (입력의 시간대 표기는 무시 — PostgreSQL과 동일)
//! * TIMESTAMPTZ: UTC로 변환한 `YYYY-MM-DD HH:MM:SS[.ffffff]+00` (시간대 없는 입력은 세션 시간대 UTC)
//!
//! 오류 메시지는 `[SQLSTATE] 내용` 형식이며 convert.rs가 SQLSTATE로 바꾼다.

use rusqlite::types::ValueRef;

pub type TvResult<T> = Result<T, String>;

fn syntax(ty: &str, v: &str) -> String {
    format!("[22007] invalid input syntax for type {ty}: \"{}\"", v.chars().take(100).collect::<String>())
}

fn range(ty: &str, v: &str) -> String {
    format!("[22008] {ty} field value out of range: \"{}\"", v.chars().take(100).collect::<String>())
}

pub fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

pub fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn digits(s: &str) -> Option<i64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) || s.len() > 9 {
        return None;
    }
    s.parse().ok()
}

/// "YYYY-MM-DD" → (y, m, d) 검증 포함.
fn parse_date_part(s: &str, ty: &str, orig: &str) -> TvResult<(i64, i64, i64)> {
    let mut it = s.split('-');
    let (Some(y), Some(m), Some(d), None) = (it.next(), it.next(), it.next(), it.next()) else {
        return Err(syntax(ty, orig));
    };
    if y.len() != 4 {
        return Err(syntax(ty, orig));
    }
    let (Some(y), Some(m), Some(d)) = (digits(y), digits(m), digits(d)) else {
        return Err(syntax(ty, orig));
    };
    if !(1..=9999).contains(&y) || !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m) {
        return Err(range(ty, orig));
    }
    Ok((y, m, d))
}

/// "HH:MM[:SS[.frac]]" → 하루 기준 마이크로초. 24:00:00 허용.
fn parse_time_part(s: &str, ty: &str, orig: &str) -> TvResult<i64> {
    let mut it = s.split(':');
    let (Some(h), Some(mi)) = (it.next(), it.next()) else {
        return Err(syntax(ty, orig));
    };
    let sec = it.next().unwrap_or("00");
    if it.next().is_some() || h.len() > 2 || mi.len() != 2 {
        return Err(syntax(ty, orig));
    }
    let (sec_i, frac) = match sec.split_once('.') {
        Some((a, b)) => (a, b),
        None => (sec, ""),
    };
    if sec_i.len() != 2 {
        return Err(syntax(ty, orig));
    }
    let (Some(h), Some(mi), Some(se)) = (digits(h), digits(mi), digits(sec_i)) else {
        return Err(syntax(ty, orig));
    };
    let mut us = 0i64;
    if !frac.is_empty() {
        if !frac.bytes().all(|b| b.is_ascii_digit()) || frac.len() > 9 {
            return Err(syntax(ty, orig));
        }
        // 마이크로초로 반올림
        let mut f: String = frac.chars().take(7).collect();
        while f.len() < 7 {
            f.push('0');
        }
        let v: i64 = f.parse().unwrap_or(0);
        us = (v + 5) / 10;
    }
    if mi > 59 || se > 59 || h > 24 || (h == 24 && (mi != 0 || se != 0 || us != 0)) {
        return Err(range(ty, orig));
    }
    Ok(((h * 60 + mi) * 60 + se) * 1_000_000 + us)
}

/// 시간대 표기 "Z", "+09", "+0900", "+09:00", "-03:30" → 초.
fn parse_offset(s: &str, ty: &str, orig: &str) -> TvResult<i64> {
    if s.eq_ignore_ascii_case("z") || s.eq_ignore_ascii_case("utc") {
        return Ok(0);
    }
    let (sign, body) = match s.as_bytes().first() {
        Some(b'+') => (1, &s[1..]),
        Some(b'-') => (-1, &s[1..]),
        _ => return Err(syntax(ty, orig)),
    };
    let parts: Vec<&str> = if body.contains(':') {
        body.split(':').collect()
    } else if body.len() == 4 {
        vec![&body[..2], &body[2..]]
    } else {
        vec![body]
    };
    if parts.is_empty() || parts.len() > 3 || parts[0].is_empty() || parts[0].len() > 2 {
        return Err(syntax(ty, orig));
    }
    let mut secs = 0;
    for (i, p) in parts.iter().enumerate() {
        let v = digits(p).ok_or_else(|| syntax(ty, orig))?;
        if (i > 0 && (p.len() != 2 || v > 59)) || (i == 0 && v > 15) {
            return Err(range(ty, orig));
        }
        secs += v * [3600, 60, 1][i];
    }
    Ok(sign * secs)
}

fn fmt_time(us: i64) -> String {
    let s = us / 1_000_000;
    let mut out = format!("{:02}:{:02}:{:02}", s / 3600, (s / 60) % 60, s % 60);
    let frac = us % 1_000_000;
    if frac != 0 {
        let f = format!(".{frac:06}");
        out.push_str(f.trim_end_matches('0'));
    }
    out
}

fn split_datetime(t: &str) -> (&str, Option<&str>) {
    match t.find([' ', 'T', 't']) {
        Some(p) => (&t[..p], Some(t[p + 1..].trim())),
        None => (t, None),
    }
}

/// 시각 문자열에서 시간대 부분 분리 ("10:00:00+09:00" → ("10:00:00", Some("+09:00"))).
fn split_tz(t: &str) -> (&str, Option<&str>) {
    let t = t.trim();
    if let Some(p) = t.find(['+', '-', 'Z', 'z']) {
        return (t[..p].trim(), Some(t[p..].trim()));
    }
    if let Some(stripped) = t.strip_suffix(" UTC").or_else(|| t.strip_suffix(" utc")) {
        return (stripped.trim(), Some("Z"));
    }
    (t, None)
}

pub fn norm_date(v: &str) -> TvResult<String> {
    let t = v.trim();
    let (d, _) = split_datetime(t);
    let (y, m, dd) = parse_date_part(d, "date", v)?;
    Ok(format!("{y:04}-{m:02}-{dd:02}"))
}

pub fn norm_time(v: &str) -> TvResult<String> {
    let (t, _) = split_tz(v.trim());
    Ok(fmt_time(parse_time_part(t, "time", v)?))
}

pub fn norm_timestamp(v: &str) -> TvResult<String> {
    let t = v.trim();
    let (d, rest) = split_datetime(t);
    let (y, m, dd) = parse_date_part(d, "timestamp", v)?;
    let us = match rest {
        None | Some("") => 0,
        Some(r) => {
            let (tm, _tz) = split_tz(r);
            parse_time_part(tm, "timestamp", v)?
        }
    };
    let days = days_from_civil(y, m, dd) + us / 86_400_000_000;
    let (y, m, dd) = civil_from_days(days);
    Ok(format!("{y:04}-{m:02}-{dd:02} {}", fmt_time(us % 86_400_000_000)))
}

pub fn norm_timestamptz(v: &str) -> TvResult<String> {
    let t = v.trim();
    let (d, rest) = split_datetime(t);
    let (y, m, dd) = parse_date_part(d, "timestamp with time zone", v)?;
    let (us, off) = match rest {
        None | Some("") => (0, 0),
        Some(r) => {
            let (tm, tz) = split_tz(r);
            let us = parse_time_part(tm, "timestamp with time zone", v)?;
            let off = match tz {
                Some(z) => parse_offset(z, "timestamp with time zone", v)?,
                None => 0,
            };
            (us, off)
        }
    };
    let total = days_from_civil(y, m, dd) * 86_400_000_000 + us - off * 1_000_000;
    let days = total.div_euclid(86_400_000_000);
    let rem = total.rem_euclid(86_400_000_000);
    let (y, m, dd) = civil_from_days(days);
    if !(1..=9999).contains(&y) {
        return Err(range("timestamp with time zone", v));
    }
    Ok(format!("{y:04}-{m:02}-{dd:02} {}+00", fmt_time(rem)))
}

pub fn norm_uuid_text(v: &str) -> TvResult<String> {
    let t = v.trim();
    let t = t.strip_prefix('{').and_then(|x| x.strip_suffix('}')).unwrap_or(t);
    let hex: String = if t.len() == 36 {
        let b = t.as_bytes();
        if b[8] != b'-' || b[13] != b'-' || b[18] != b'-' || b[23] != b'-' {
            return Err(format!("[22P02] invalid input syntax for type uuid: \"{v}\""));
        }
        t.chars().filter(|c| *c != '-').collect()
    } else {
        t.to_string()
    };
    if hex.len() != 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(format!(
            "[22P02] invalid input syntax for type uuid: \"{}\"",
            v.chars().take(100).collect::<String>()
        ));
    }
    let h = hex.to_ascii_lowercase();
    Ok(format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32]))
}

/// SQLite 값 → 정규형. NULL은 NULL.
pub fn normalize(kind: &str, v: ValueRef<'_>) -> TvResult<Option<String>> {
    let text = match v {
        ValueRef::Null => return Ok(None),
        ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned(),
        ValueRef::Blob(b) if kind == "uuid" && b.len() == 16 => hex_uuid(b),
        ValueRef::Integer(i) => {
            return Err(format!("[42804] {kind} value expected, got integer {i}"));
        }
        ValueRef::Real(f) => return Err(format!("[42804] {kind} value expected, got number {f}")),
        ValueRef::Blob(_) => return Err(format!("[42804] {kind} value expected, got bytea")),
    };
    let r = match kind {
        "uuid" => norm_uuid_text(&text)?,
        "date" => norm_date(&text)?,
        "time" => norm_time(&text)?,
        "ts" => norm_timestamp(&text)?,
        "tstz" => norm_timestamptz(&text)?,
        _ => return Err(format!("[XX000] unknown normalizer {kind}")),
    };
    Ok(Some(r))
}

fn hex_uuid(b: &[u8]) -> String {
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

/// 이미 정규형인가 (CHECK 제약용, 오류 대신 false).
pub fn is_canonical(kind: &str, v: ValueRef<'_>) -> bool {
    match v {
        ValueRef::Text(t) => {
            let s = String::from_utf8_lossy(t);
            matches!(normalize(kind, v), Ok(Some(n)) if n == s)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporal_normalization() {
        assert_eq!(norm_timestamptz("2026-10-27 09:00:00+09:00").unwrap(), "2026-10-27 00:00:00+00");
        assert_eq!(norm_timestamptz("2026-10-27 00:00:00+00:00").unwrap(), "2026-10-27 00:00:00+00");
        assert_eq!(norm_timestamptz("2026-10-27T00:00:00Z").unwrap(), "2026-10-27 00:00:00+00");
        assert_eq!(norm_timestamptz("2026-01-01 01:30:00.500+0300").unwrap(), "2025-12-31 22:30:00.5+00");
        assert_eq!(norm_timestamptz("2026-10-27").unwrap(), "2026-10-27 00:00:00+00");
        assert_eq!(norm_timestamp("2026-10-27 09:00:00+09:00").unwrap(), "2026-10-27 09:00:00");
        assert_eq!(norm_date("2024-02-29").unwrap(), "2024-02-29");
        assert!(norm_date("2026-02-30").unwrap_err().starts_with("[22008]"));
        assert!(norm_date("2023-02-29").is_err());
        assert!(norm_date("hello").unwrap_err().starts_with("[22007]"));
        assert!(norm_time("25:99:99").is_err());
        assert_eq!(norm_time("9:05").unwrap(), "09:05:00");
        assert_eq!(norm_time("24:00:00").unwrap(), "24:00:00");
        assert_eq!(norm_time("10:00:00.1234567").unwrap(), "10:00:00.123457");
        assert_eq!(norm_uuid_text("{A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11}").unwrap(), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        assert_eq!(norm_uuid_text("a0eebc999c0b4ef8bb6d6bb9bd380a11").unwrap(), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
        assert!(norm_uuid_text("not-a-uuid").unwrap_err().starts_with("[22P02]"));
        // 정렬 = 시간 순서
        let a = norm_timestamptz("2026-10-27 00:00:00+00").unwrap();
        let b = norm_timestamptz("2026-10-27 00:00:00.5+00").unwrap();
        let c = norm_timestamptz("2026-10-27 00:00:01+00").unwrap();
        assert!(a < b && b < c);
    }
}
