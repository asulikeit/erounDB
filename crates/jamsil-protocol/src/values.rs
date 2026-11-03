//! SQL 값 ↔ PG text/binary 포맷 변환.

use jamsil_common::{DbError, Value};
use jamsil_sql::types::oid;

const PG_EPOCH_DAYS: i64 = 10957; // 2000-01-01 - 1970-01-01

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

/// "YYYY-MM-DD[ HH:MM:SS[.ffffff]][+HH[:MM]]" → (unix micros UTC, has_time)
pub fn parse_timestamp(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date, rest) = match s.find([' ', 'T']) {
        Some(p) => (&s[..p], s[p + 1..].trim()),
        None => (s, ""),
    };
    let mut dp = date.splitn(3, '-');
    let y: i64 = dp.next()?.parse().ok()?;
    let m: i64 = dp.next()?.parse().ok()?;
    let d: i64 = dp.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let days = days_from_civil(y, m, d);
    if rest.is_empty() {
        return Some(days * 86_400_000_000);
    }
    let (time, tz) = match rest.find(['+', '-', 'Z']) {
        Some(p) => (&rest[..p], &rest[p..]),
        None => (rest, ""),
    };
    let micros = parse_time(time.trim())?;
    let tzs: i64 = if tz.is_empty() || tz == "Z" {
        0
    } else {
        let sign = if tz.starts_with('-') { -1 } else { 1 };
        let body = &tz[1..];
        let (h, mi) = match body.split_once(':') {
            Some((h, m)) => (h.parse::<i64>().ok()?, m.parse::<i64>().ok()?),
            None if body.len() == 4 => (body[..2].parse().ok()?, body[2..].parse().ok()?),
            None => (body.parse().ok()?, 0),
        };
        sign * (h * 3600 + mi * 60)
    };
    Some(days * 86_400_000_000 + micros - tzs * 1_000_000)
}

pub fn parse_time(t: &str) -> Option<i64> {
    let mut p = t.split(':');
    let h: i64 = p.next()?.parse().ok()?;
    let m: i64 = p.next().unwrap_or("0").parse().ok()?;
    let sec = p.next().unwrap_or("0");
    let (s, frac) = match sec.split_once('.') {
        Some((s, f)) => (s, f),
        None => (sec, ""),
    };
    let s: i64 = s.parse().ok()?;
    let mut us: i64 = 0;
    if !frac.is_empty() {
        let f: String = frac.chars().take(6).collect();
        us = f.parse::<i64>().ok()? * 10i64.pow(6 - f.len() as u32);
    }
    Some(((h * 60 + m) * 60 + s) * 1_000_000 + us)
}

pub fn format_timestamp(unix_micros: i64, with_tz: bool) -> String {
    let days = unix_micros.div_euclid(86_400_000_000);
    let rem = unix_micros.rem_euclid(86_400_000_000);
    let (y, m, d) = civil_from_days(days);
    let secs = rem / 1_000_000;
    let us = rem % 1_000_000;
    let mut s = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        y,
        m,
        d,
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    );
    if us != 0 {
        let f = format!(".{us:06}");
        s.push_str(f.trim_end_matches('0'));
    }
    if with_tz {
        s.push_str("+00");
    }
    s
}

pub fn format_float(f: f64) -> String {
    if f.is_nan() {
        return "NaN".into();
    }
    if f.is_infinite() {
        return if f > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        };
    }
    if f.fract() == 0.0 && f.abs() < 1e15 {
        return format!("{}", f as i64);
    }
    let s = format!("{f:?}");
    // Rust: 1e300 / 1e-7 → PG: 1e+300 / 1e-07
    if let Some((m, e)) = s.split_once('e') {
        let (sign, digits) = if let Some(d) = e.strip_prefix('-') {
            ("-", d)
        } else {
            ("+", e)
        };
        let digits = if digits.len() < 2 {
            format!("0{digits}")
        } else {
            digits.to_string()
        };
        return format!("{m}e{sign}{digits}");
    }
    s
}

fn bool_text(v: &Value) -> Option<bool> {
    match v {
        Value::Int(i) => Some(*i != 0),
        Value::Real(f) => Some(*f != 0.0),
        Value::Text(s) => match s.trim().to_ascii_lowercase().as_str() {
            "t" | "true" | "1" | "yes" | "y" | "on" => Some(true),
            "f" | "false" | "0" | "no" | "n" | "off" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn plain_text(v: &Value) -> Vec<u8> {
    match v {
        Value::Null => vec![],
        Value::Int(i) => i.to_string().into_bytes(),
        Value::Real(f) => format_float(*f).into_bytes(),
        Value::Text(s) => s.as_bytes().to_vec(),
        Value::Blob(b) => {
            let mut s = String::with_capacity(2 + b.len() * 2);
            s.push_str("\\x");
            s.push_str(&hex::encode(b));
            s.into_bytes()
        }
    }
}

/// 값 → text 포맷 바이트.
pub fn encode_text(v: &Value, type_oid: u32) -> Option<Vec<u8>> {
    if v.is_null() {
        return None;
    }
    Some(match type_oid {
        oid::BOOL => match bool_text(v) {
            Some(true) => b"t".to_vec(),
            Some(false) => b"f".to_vec(),
            None => plain_text(v),
        },
        oid::BYTEA => match v {
            Value::Text(s) => format!("\\x{}", hex::encode(s.as_bytes())).into_bytes(),
            other => plain_text(other),
        },
        _ => plain_text(v),
    })
}

fn invalid(ty: &str, v: &Value) -> DbError {
    DbError::new(
        "",
        "22P02",
        format!("cannot encode value {v:?} as binary {ty}"),
    )
}

fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Int(i) => Some(*i),
        Value::Real(f) if f.fract() == 0.0 => Some(*f as i64),
        Value::Text(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Real(f) => Some(*f),
        Value::Text(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// PG numeric binary 인코딩 (10진 문자열에서).
pub fn numeric_binary(s: &str) -> Option<Vec<u8>> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("nan") {
        let mut out = vec![];
        out.extend_from_slice(&0i16.to_be_bytes());
        out.extend_from_slice(&0i16.to_be_bytes());
        out.extend_from_slice(&0xC000u16.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        return Some(out);
    }
    let (neg, body) = match s.strip_prefix('-') {
        Some(b) => (true, b),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    // 지수 표기는 f64 경유
    if body.contains(['e', 'E']) {
        let f: f64 = s.parse().ok()?;
        return numeric_binary(&format!("{f}"));
    }
    let (int_part, frac_part) = match body.split_once('.') {
        Some((a, b)) => (a, b),
        None => (body, ""),
    };
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let dscale = frac_part.len() as u16;
    let int_part = int_part.trim_start_matches('0');
    // 4자리 그룹으로 정렬
    let pad_int = (4 - int_part.len() % 4) % 4;
    let int_s = format!("{}{}", "0".repeat(pad_int), int_part);
    let pad_frac = (4 - frac_part.len() % 4) % 4;
    let frac_s = format!("{}{}", frac_part, "0".repeat(pad_frac));
    let mut digits: Vec<i16> = vec![];
    for c in int_s.as_bytes().chunks(4) {
        digits.push(std::str::from_utf8(c).ok()?.parse().ok()?);
    }
    let int_groups = digits.len() as i16;
    for c in frac_s.as_bytes().chunks(4) {
        digits.push(std::str::from_utf8(c).ok()?.parse().ok()?);
    }
    let mut weight = int_groups - 1;
    // 앞쪽 0 그룹 제거
    while digits.first() == Some(&0) {
        digits.remove(0);
        weight -= 1;
    }
    while digits.last() == Some(&0) {
        digits.pop();
    }
    if digits.is_empty() {
        weight = 0;
    }
    let mut out = vec![];
    out.extend_from_slice(&(digits.len() as i16).to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(
        &(if neg && !digits.is_empty() {
            0x4000u16
        } else {
            0
        })
        .to_be_bytes(),
    );
    out.extend_from_slice(&dscale.to_be_bytes());
    for d in digits {
        out.extend_from_slice(&d.to_be_bytes());
    }
    Some(out)
}

pub fn numeric_from_binary(b: &[u8]) -> Option<String> {
    if b.len() < 8 {
        return None;
    }
    let n = i16::from_be_bytes([b[0], b[1]]) as usize;
    let weight = i16::from_be_bytes([b[2], b[3]]) as i32;
    let sign = u16::from_be_bytes([b[4], b[5]]);
    let dscale = u16::from_be_bytes([b[6], b[7]]) as usize;
    if sign == 0xC000 {
        return Some("NaN".into());
    }
    if b.len() < 8 + n * 2 {
        return None;
    }
    let digits: Vec<i32> = (0..n)
        .map(|i| i16::from_be_bytes([b[8 + i * 2], b[9 + i * 2]]) as i32)
        .collect();
    let mut int_s = String::new();
    for w in 0..=weight.max(-1) {
        let d = digits.get(w as usize).copied().unwrap_or(0);
        if int_s.is_empty() {
            int_s = d.to_string();
        } else {
            int_s.push_str(&format!("{d:04}"));
        }
    }
    if int_s.is_empty() {
        int_s = "0".into();
    }
    let mut frac = String::new();
    let mut idx = weight + 1;
    if idx < 0 {
        frac.push_str(&"0000".repeat((-idx) as usize));
        idx = 0;
    }
    for d in digits.iter().skip(idx as usize) {
        frac.push_str(&format!("{d:04}"));
    }
    while frac.len() < dscale {
        frac.push('0');
    }
    frac.truncate(dscale);
    let mut s = if sign == 0x4000 {
        format!("-{int_s}")
    } else {
        int_s
    };
    if dscale > 0 {
        s.push('.');
        s.push_str(&frac);
    }
    Some(s)
}

/// 값 → binary 포맷 바이트.
pub fn encode_binary(v: &Value, type_oid: u32) -> Result<Option<Vec<u8>>, DbError> {
    if v.is_null() {
        return Ok(None);
    }
    Ok(Some(match type_oid {
        oid::BOOL => vec![bool_text(v).ok_or_else(|| invalid("bool", v))? as u8],
        oid::INT2 => (as_i64(v).ok_or_else(|| invalid("int2", v))? as i16)
            .to_be_bytes()
            .to_vec(),
        oid::INT4 => (as_i64(v).ok_or_else(|| invalid("int4", v))? as i32)
            .to_be_bytes()
            .to_vec(),
        oid::INT8 => as_i64(v)
            .ok_or_else(|| invalid("int8", v))?
            .to_be_bytes()
            .to_vec(),
        oid::OID | oid::REGCLASS | oid::REGPROC => (as_i64(v).ok_or_else(|| invalid("oid", v))?
            as u32)
            .to_be_bytes()
            .to_vec(),
        oid::FLOAT4 => (as_f64(v).ok_or_else(|| invalid("float4", v))? as f32)
            .to_be_bytes()
            .to_vec(),
        oid::FLOAT8 => as_f64(v)
            .ok_or_else(|| invalid("float8", v))?
            .to_be_bytes()
            .to_vec(),
        oid::BYTEA => match v {
            Value::Blob(b) => b.clone(),
            Value::Text(s) => s.as_bytes().to_vec(),
            other => plain_text(other),
        },
        oid::UUID => {
            let s = String::from_utf8(plain_text(v)).unwrap_or_default();
            let h: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
            let b = hex::decode(&h).map_err(|_| invalid("uuid", v))?;
            if b.len() != 16 {
                return Err(invalid("uuid", v));
            }
            b
        }
        oid::DATE => {
            let s = String::from_utf8(plain_text(v)).unwrap_or_default();
            let us = parse_timestamp(&s).ok_or_else(|| invalid("date", v))?;
            ((us.div_euclid(86_400_000_000) - PG_EPOCH_DAYS) as i32)
                .to_be_bytes()
                .to_vec()
        }
        oid::TIMESTAMP | oid::TIMESTAMPTZ => {
            let s = String::from_utf8(plain_text(v)).unwrap_or_default();
            let us = parse_timestamp(&s).ok_or_else(|| invalid("timestamp", v))?;
            (us - PG_EPOCH_DAYS * 86_400_000_000).to_be_bytes().to_vec()
        }
        oid::TIME => {
            let s = String::from_utf8(plain_text(v)).unwrap_or_default();
            parse_time(s.trim())
                .ok_or_else(|| invalid("time", v))?
                .to_be_bytes()
                .to_vec()
        }
        oid::NUMERIC => {
            let s = String::from_utf8(plain_text(v)).unwrap_or_default();
            numeric_binary(&s).ok_or_else(|| invalid("numeric", v))?
        }
        oid::JSONB => {
            let mut b = vec![1u8];
            b.extend_from_slice(&plain_text(v));
            b
        }
        _ => plain_text(v),
    }))
}

fn utf8(b: Vec<u8>) -> Result<String, DbError> {
    String::from_utf8(b).map_err(|_| DbError::invalid_utf8())
}

/// Bind 파라미터 디코딩.
pub fn decode_param(raw: Option<Vec<u8>>, type_oid: u32, binary: bool) -> Result<Value, DbError> {
    let Some(raw) = raw else {
        return Ok(Value::Null);
    };
    if !binary {
        let s = utf8(raw)?;
        return Ok(match type_oid {
            oid::BOOL => match bool_text(&Value::Text(s.clone())) {
                Some(b) => Value::Int(b as i64),
                None => {
                    return Err(DbError::new(
                        "",
                        "22P02",
                        format!("invalid input syntax for type boolean: \"{s}\""),
                    ))
                }
            },
            oid::INT2 | oid::INT4 | oid::INT8 | oid::OID => match s.trim().parse::<i64>() {
                Ok(i) => Value::Int(i),
                Err(_) => {
                    return Err(DbError::new(
                        "",
                        "22P02",
                        format!("invalid input syntax for type integer: \"{s}\""),
                    ))
                }
            },
            oid::FLOAT4 | oid::FLOAT8 => match s.trim().to_ascii_lowercase().as_str() {
                "nan" => Value::Real(f64::NAN),
                "infinity" | "+infinity" => Value::Real(f64::INFINITY),
                "-infinity" => Value::Real(f64::NEG_INFINITY),
                t => Value::Real(t.parse().map_err(|_| {
                    DbError::new(
                        "",
                        "22P02",
                        format!("invalid input syntax for type double precision: \"{s}\""),
                    )
                })?),
            },
            oid::NUMERIC => match s.trim().parse::<i64>() {
                Ok(i) => Value::Int(i),
                Err(_) => Value::Real(s.trim().parse().map_err(|_| {
                    DbError::new(
                        "",
                        "22P02",
                        format!("invalid input syntax for type numeric: \"{s}\""),
                    )
                })?),
            },
            oid::BYTEA => {
                if let Some(h) = s.strip_prefix("\\x") {
                    Value::Blob(
                        hex::decode(h)
                            .map_err(|_| DbError::new("", "22P02", "invalid hexadecimal data"))?,
                    )
                } else {
                    Value::Blob(unescape_bytea(&s))
                }
            }
            _ => Value::Text(s),
        });
    }
    let b = raw;
    let need = |n: usize| -> Result<(), DbError> {
        if b.len() != n {
            Err(DbError::new(
                "",
                "22P03",
                format!(
                    "incorrect binary data format (expected {n} bytes, got {})",
                    b.len()
                ),
            ))
        } else {
            Ok(())
        }
    };
    Ok(match type_oid {
        oid::BOOL => {
            need(1)?;
            Value::Int((b[0] != 0) as i64)
        }
        oid::INT2 => {
            need(2)?;
            Value::Int(i16::from_be_bytes([b[0], b[1]]) as i64)
        }
        oid::INT4 => {
            need(4)?;
            Value::Int(i32::from_be_bytes(b[..4].try_into().unwrap()) as i64)
        }
        oid::OID => {
            need(4)?;
            Value::Int(u32::from_be_bytes(b[..4].try_into().unwrap()) as i64)
        }
        oid::INT8 => {
            need(8)?;
            Value::Int(i64::from_be_bytes(b[..8].try_into().unwrap()))
        }
        oid::FLOAT4 => {
            need(4)?;
            Value::Real(f32::from_be_bytes(b[..4].try_into().unwrap()) as f64)
        }
        oid::FLOAT8 => {
            need(8)?;
            Value::Real(f64::from_be_bytes(b[..8].try_into().unwrap()))
        }
        oid::BYTEA => Value::Blob(b),
        oid::UUID => {
            need(16)?;
            let h = hex::encode(&b);
            Value::Text(format!(
                "{}-{}-{}-{}-{}",
                &h[0..8],
                &h[8..12],
                &h[12..16],
                &h[16..20],
                &h[20..32]
            ))
        }
        oid::DATE => {
            need(4)?;
            let days = i32::from_be_bytes(b[..4].try_into().unwrap()) as i64 + PG_EPOCH_DAYS;
            let (y, m, d) = civil_from_days(days);
            Value::Text(format!("{y:04}-{m:02}-{d:02}"))
        }
        oid::TIMESTAMP | oid::TIMESTAMPTZ => {
            need(8)?;
            let us =
                i64::from_be_bytes(b[..8].try_into().unwrap()) + PG_EPOCH_DAYS * 86_400_000_000;
            Value::Text(format_timestamp(us, type_oid == oid::TIMESTAMPTZ))
        }
        oid::TIME => {
            need(8)?;
            let us = i64::from_be_bytes(b[..8].try_into().unwrap());
            let s = us / 1_000_000;
            let mut t = format!("{:02}:{:02}:{:02}", s / 3600, (s / 60) % 60, s % 60);
            if us % 1_000_000 != 0 {
                t.push_str(format!(".{:06}", us % 1_000_000).trim_end_matches('0'));
            }
            Value::Text(t)
        }
        oid::NUMERIC => {
            let s = numeric_from_binary(&b)
                .ok_or_else(|| DbError::new("", "22P03", "invalid numeric binary"))?;
            match s.parse::<i64>() {
                Ok(i) => Value::Int(i),
                Err(_) => Value::Real(s.parse().unwrap_or(f64::NAN)),
            }
        }
        oid::JSONB => {
            if b.first() != Some(&1) {
                return Err(DbError::new("", "22P03", "unsupported jsonb version"));
            }
            Value::Text(utf8(b[1..].to_vec())?)
        }
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::JSON | oid::UNKNOWN | 0 => {
            Value::Text(utf8(b)?)
        }
        o => {
            return Err(DbError::new(
                "",
                "22P03",
                format!("binary format for type oid {o} is not supported"),
            ))
        }
    })
}

fn unescape_bytea(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' && i + 1 < b.len() && b[i + 1] == b'\\' {
            out.push(b'\\');
            i += 2;
        } else if b[i] == b'\\'
            && i + 3 < b.len()
            && b[i + 1..i + 4].iter().all(|c| (b'0'..=b'7').contains(c))
        {
            out.push(((b[i + 1] - b'0') << 6) | ((b[i + 2] - b'0') << 3) | (b[i + 3] - b'0'));
            i += 4;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_roundtrip() {
        let us = parse_timestamp("2024-02-29 13:45:01.25+09").unwrap();
        assert_eq!(format_timestamp(us, true), "2024-02-29 04:45:01.25+00");
        let v = Value::Text("2000-01-01 00:00:00".into());
        assert_eq!(
            encode_binary(&v, oid::TIMESTAMP).unwrap().unwrap(),
            0i64.to_be_bytes().to_vec()
        );
        let back = decode_param(Some(0i32.to_be_bytes().to_vec()), oid::DATE, true).unwrap();
        assert_eq!(back, Value::Text("2000-01-01".into()));
    }

    #[test]
    fn numeric_roundtrip() {
        for s in ["0", "1", "-12345.678", "0.0001", "100000000", "3.14159"] {
            let b = numeric_binary(s).unwrap();
            let back = numeric_from_binary(&b).unwrap();
            assert_eq!(
                back.parse::<f64>().unwrap(),
                s.parse::<f64>().unwrap(),
                "{s} -> {back}"
            );
        }
    }

    #[test]
    fn floats() {
        assert_eq!(format_float(1.0), "1");
        assert_eq!(format_float(0.1), "0.1");
        assert_eq!(format_float(1e300), "1e+300");
        assert_eq!(format_float(1.5e-7), "1.5e-07");
        assert_eq!(encode_text(&Value::Int(1), oid::BOOL).unwrap(), b"t");
    }
}
