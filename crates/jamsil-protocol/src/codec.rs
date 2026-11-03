//! 메시지 인코딩/디코딩.

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

use jamsil_common::{ColumnDesc, DbError};

pub const SSL_REQUEST: i32 = 80877103;
pub const GSSENC_REQUEST: i32 = 80877104;
pub const CANCEL_REQUEST: i32 = 80877102;
pub const PROTOCOL_V3: i32 = 196608;

#[derive(Debug)]
pub enum Startup {
    Ssl,
    GssEnc,
    Cancel {
        pid: i32,
        key: i32,
    },
    Startup {
        version: i32,
        params: Vec<(String, String)>,
    },
}

fn cstr(buf: &mut &[u8]) -> Result<String, DbError> {
    let pos = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| DbError::protocol("missing string terminator"))?;
    let s = std::str::from_utf8(&buf[..pos])
        .map_err(|_| DbError::invalid_utf8())?
        .to_string();
    buf.advance(pos + 1);
    Ok(s)
}

fn need(buf: &[u8], n: usize) -> Result<(), DbError> {
    if buf.len() < n {
        Err(DbError::protocol("message too short"))
    } else {
        Ok(())
    }
}

pub async fn read_startup<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<Startup>, DbError> {
    let len = match r.read_i32().await {
        Ok(l) => l,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if !(8..=10_000).contains(&len) {
        return Err(DbError::protocol("invalid startup packet length"));
    }
    let mut body = vec![0u8; len as usize - 4];
    r.read_exact(&mut body).await?;
    let mut b: &[u8] = &body;
    let code = b.get_i32();
    Ok(Some(match code {
        SSL_REQUEST => Startup::Ssl,
        GSSENC_REQUEST => Startup::GssEnc,
        CANCEL_REQUEST => {
            need(b, 8)?;
            Startup::Cancel {
                pid: b.get_i32(),
                key: b.get_i32(),
            }
        }
        v => {
            let mut params = vec![];
            while !b.is_empty() && b[0] != 0 {
                let k = cstr(&mut b)?;
                let val = cstr(&mut b)?;
                params.push((k, val));
            }
            Startup::Startup { version: v, params }
        }
    }))
}

#[derive(Debug)]
pub enum FrontendMessage {
    Query(String),
    Parse {
        name: String,
        sql: String,
        param_types: Vec<u32>,
    },
    Bind {
        portal: String,
        statement: String,
        param_formats: Vec<i16>,
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    },
    Describe {
        kind: u8,
        name: String,
    },
    Execute {
        portal: String,
        max_rows: i32,
    },
    Sync,
    Flush,
    Close {
        kind: u8,
        name: String,
    },
    Terminate,
    /// 'p' 메시지 원문 (Password/SASL)
    Password(Vec<u8>),
    CopyData,
    CopyDone,
    CopyFail,
    FunctionCall,
}

/// 일반 메시지 하나 읽기. max_len 초과 시 오류.
pub async fn read_message<R: AsyncRead + Unpin>(
    r: &mut R,
    max_len: usize,
) -> Result<Option<FrontendMessage>, DbError> {
    let tag = match r.read_u8().await {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let len = r.read_i32().await?;
    if len < 4 || len as usize - 4 > max_len {
        return Err(DbError::new("", "54000", format!("message too large ({len} bytes)")).fatal());
    }
    let mut body = vec![0u8; len as usize - 4];
    r.read_exact(&mut body).await?;
    parse_message(tag, body).map(Some)
}

/// 메시지 하나 읽기 + 대형 메시지 메모리 예산 예약 (RQ-11).
///
/// 본문을 읽기 전에 길이만큼 예약하고, 예산이 `wait` 안에 확보되지 않으면 본문을 버리고
/// `Ok(Some(Err((tag, 오류))))`를 돌려준다 (연결은 유지, 프로토콜 동기화 유지).
pub async fn read_message_budgeted<R: AsyncRead + Unpin>(
    r: &mut R,
    max_len: usize,
    wait: std::time::Duration,
) -> Result<Option<Result<(FrontendMessage, jamsil_common::budget::BudgetGuard), (u8, DbError)>>, DbError> {
    let tag = match r.read_u8().await {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let len = r.read_i32().await?;
    if len < 4 || len as usize - 4 > max_len {
        return Err(DbError::new("", "54000", format!("message too large ({len} bytes)")).fatal());
    }
    let n = len as usize - 4;
    let guard = match jamsil_common::budget::global().reserve(n, wait).await {
        Ok(g) => g,
        Err(e) => {
            let mut left = n;
            let mut scratch = [0u8; 8192];
            while left > 0 {
                let k = left.min(scratch.len());
                r.read_exact(&mut scratch[..k]).await?;
                left -= k;
            }
            return Ok(Some(Err((tag, e))));
        }
    };
    let mut body = vec![0u8; n];
    r.read_exact(&mut body).await?;
    Ok(Some(Ok((parse_message(tag, body)?, guard))))
}

fn parse_message(tag: u8, body: Vec<u8>) -> Result<FrontendMessage, DbError> {
    let mut b: &[u8] = &body;
    Ok(match tag {
        b'Q' => FrontendMessage::Query(cstr(&mut b)?),
        b'P' => {
            let name = cstr(&mut b)?;
            let sql = cstr(&mut b)?;
            need(b, 2)?;
            let n = b.get_i16().max(0) as usize;
            need(b, n * 4)?;
            let param_types = (0..n).map(|_| b.get_u32()).collect();
            FrontendMessage::Parse {
                name,
                sql,
                param_types,
            }
        }
        b'B' => {
            let portal = cstr(&mut b)?;
            let statement = cstr(&mut b)?;
            need(b, 2)?;
            let nf = b.get_i16().max(0) as usize;
            need(b, nf * 2)?;
            let param_formats = (0..nf).map(|_| b.get_i16()).collect();
            need(b, 2)?;
            let np = b.get_i16().max(0) as usize;
            let mut params = Vec::with_capacity(np);
            for _ in 0..np {
                need(b, 4)?;
                let l = b.get_i32();
                if l < 0 {
                    params.push(None);
                } else {
                    need(b, l as usize)?;
                    params.push(Some(b[..l as usize].to_vec()));
                    b.advance(l as usize);
                }
            }
            need(b, 2)?;
            let nr = b.get_i16().max(0) as usize;
            need(b, nr * 2)?;
            let result_formats = (0..nr).map(|_| b.get_i16()).collect();
            FrontendMessage::Bind {
                portal,
                statement,
                param_formats,
                params,
                result_formats,
            }
        }
        b'D' => {
            need(b, 1)?;
            let kind = b.get_u8();
            FrontendMessage::Describe {
                kind,
                name: cstr(&mut b)?,
            }
        }
        b'E' => {
            let portal = cstr(&mut b)?;
            need(b, 4)?;
            FrontendMessage::Execute {
                portal,
                max_rows: b.get_i32(),
            }
        }
        b'S' => FrontendMessage::Sync,
        b'H' => FrontendMessage::Flush,
        b'C' => {
            need(b, 1)?;
            let kind = b.get_u8();
            FrontendMessage::Close {
                kind,
                name: cstr(&mut b)?,
            }
        }
        b'X' => FrontendMessage::Terminate,
        b'p' => FrontendMessage::Password(body),
        b'd' => FrontendMessage::CopyData,
        b'c' => FrontendMessage::CopyDone,
        b'f' => FrontendMessage::CopyFail,
        b'F' => FrontendMessage::FunctionCall,
        t => {
            return Err(
                DbError::protocol(format!("invalid frontend message type {}", t as char)).fatal(),
            )
        }
    })
}

#[derive(Debug, Clone)]
pub enum BackendMessage<'a> {
    AuthOk,
    AuthCleartext,
    AuthSasl(&'a [&'a str]),
    AuthSaslContinue(&'a [u8]),
    AuthSaslFinal(&'a [u8]),
    ParameterStatus(&'a str, &'a str),
    BackendKeyData(i32, i32),
    ReadyForQuery(u8),
    RowDescription(&'a [ColumnDesc], &'a [i16]),
    CommandComplete(&'a str),
    EmptyQuery,
    ParseComplete,
    BindComplete,
    CloseComplete,
    NoData,
    PortalSuspended,
    ParameterDescription(&'a [u32]),
    Error(&'a DbError),
    Notice(&'a str),
}

fn put_cstr(out: &mut BytesMut, s: &str) {
    out.put_slice(s.as_bytes());
    out.put_u8(0);
}

/// 메시지 시작: 태그 + 길이 자리. 반환값은 길이 위치.
fn begin(out: &mut BytesMut, tag: u8) -> usize {
    out.put_u8(tag);
    let p = out.len();
    out.put_i32(0);
    p
}

fn finish(out: &mut BytesMut, p: usize) {
    let len = (out.len() - p) as i32;
    out[p..p + 4].copy_from_slice(&len.to_be_bytes());
}

fn format_for(formats: &[i16], i: usize) -> i16 {
    match formats.len() {
        0 => 0,
        1 => formats[0],
        _ => formats.get(i).copied().unwrap_or(0),
    }
}

pub fn encode(out: &mut BytesMut, m: &BackendMessage<'_>) {
    match m {
        BackendMessage::AuthOk => {
            let p = begin(out, b'R');
            out.put_i32(0);
            finish(out, p);
        }
        BackendMessage::AuthCleartext => {
            let p = begin(out, b'R');
            out.put_i32(3);
            finish(out, p);
        }
        BackendMessage::AuthSasl(mechs) => {
            let p = begin(out, b'R');
            out.put_i32(10);
            for m in mechs.iter() {
                put_cstr(out, m);
            }
            out.put_u8(0);
            finish(out, p);
        }
        BackendMessage::AuthSaslContinue(d) => {
            let p = begin(out, b'R');
            out.put_i32(11);
            out.put_slice(d);
            finish(out, p);
        }
        BackendMessage::AuthSaslFinal(d) => {
            let p = begin(out, b'R');
            out.put_i32(12);
            out.put_slice(d);
            finish(out, p);
        }
        BackendMessage::ParameterStatus(k, v) => {
            let p = begin(out, b'S');
            put_cstr(out, k);
            put_cstr(out, v);
            finish(out, p);
        }
        BackendMessage::BackendKeyData(pid, key) => {
            let p = begin(out, b'K');
            out.put_i32(*pid);
            out.put_i32(*key);
            finish(out, p);
        }
        BackendMessage::ReadyForQuery(s) => {
            let p = begin(out, b'Z');
            out.put_u8(*s);
            finish(out, p);
        }
        BackendMessage::RowDescription(cols, formats) => {
            let p = begin(out, b'T');
            out.put_i16(cols.len() as i16);
            for (i, c) in cols.iter().enumerate() {
                put_cstr(out, &c.name);
                out.put_u32(c.table_oid);
                out.put_i16(c.column_attr);
                out.put_u32(c.type_oid);
                out.put_i16(c.type_len);
                out.put_i32(c.type_mod);
                out.put_i16(format_for(formats, i));
            }
            finish(out, p);
        }
        BackendMessage::CommandComplete(tag) => {
            let p = begin(out, b'C');
            put_cstr(out, tag);
            finish(out, p);
        }
        BackendMessage::EmptyQuery => {
            let p = begin(out, b'I');
            finish(out, p);
        }
        BackendMessage::ParseComplete => {
            let p = begin(out, b'1');
            finish(out, p);
        }
        BackendMessage::BindComplete => {
            let p = begin(out, b'2');
            finish(out, p);
        }
        BackendMessage::CloseComplete => {
            let p = begin(out, b'3');
            finish(out, p);
        }
        BackendMessage::NoData => {
            let p = begin(out, b'n');
            finish(out, p);
        }
        BackendMessage::PortalSuspended => {
            let p = begin(out, b's');
            finish(out, p);
        }
        BackendMessage::ParameterDescription(types) => {
            let p = begin(out, b't');
            out.put_i16(types.len() as i16);
            for t in types.iter() {
                out.put_u32(*t);
            }
            finish(out, p);
        }
        BackendMessage::Error(e) => {
            let p = begin(out, b'E');
            error_fields(out, e, if e.fatal { "FATAL" } else { "ERROR" });
            finish(out, p);
        }
        BackendMessage::Notice(msg) => {
            let p = begin(out, b'N');
            out.put_u8(b'S');
            put_cstr(out, "NOTICE");
            out.put_u8(b'V');
            put_cstr(out, "NOTICE");
            out.put_u8(b'C');
            put_cstr(out, "00000");
            out.put_u8(b'M');
            put_cstr(out, msg);
            out.put_u8(0);
            finish(out, p);
        }
    }
}

fn error_fields(out: &mut BytesMut, e: &DbError, sev: &str) {
    out.put_u8(b'S');
    put_cstr(out, sev);
    out.put_u8(b'V');
    put_cstr(out, sev);
    out.put_u8(b'C');
    put_cstr(out, &e.sqlstate);
    out.put_u8(b'M');
    put_cstr(out, &e.display_message());
    if let Some(d) = &e.detail {
        out.put_u8(b'D');
        put_cstr(out, d);
    }
    if let Some(h) = &e.hint {
        out.put_u8(b'H');
        put_cstr(out, h);
    }
    if !e.code.is_empty() {
        // 벤더 코드: 'R' (routine) 필드에 노출
        out.put_u8(b'R');
        put_cstr(out, &e.code);
    }
    out.put_u8(0);
}

/// DataRow: 이미 인코딩된 컬럼 값들.
pub fn encode_data_row(out: &mut BytesMut, cols: &[Option<Vec<u8>>]) {
    let p = begin(out, b'D');
    out.put_i16(cols.len() as i16);
    for c in cols {
        match c {
            None => out.put_i32(-1),
            Some(v) => {
                out.put_i32(v.len() as i32);
                out.put_slice(v);
            }
        }
    }
    finish(out, p);
}

/// SASLInitialResponse 파싱: (mechanism, data)
pub fn parse_sasl_initial(body: &[u8]) -> Result<(String, Vec<u8>), DbError> {
    let mut b = body;
    let mech = cstr(&mut b)?;
    need(b, 4)?;
    let l = b.get_i32();
    let data = if l < 0 {
        vec![]
    } else {
        b[..(l as usize).min(b.len())].to_vec()
    };
    Ok((mech, data))
}

pub fn parse_password(body: &[u8]) -> Result<String, DbError> {
    let mut b = body;
    cstr(&mut b)
}
