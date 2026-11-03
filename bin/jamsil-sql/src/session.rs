//! 클라이언트 세션: 시작/인증, Simple/Extended Query, 트랜잭션 상태, 라우팅.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Notify;

use jamsil_common::limits::MAX_PG_MESSAGE_BYTES;
use jamsil_common::{new_id, ColumnDesc, DbError, Id128, QueryResult, Value};
use jamsil_ipc::msg::{
    DescribeResult, SearchConsistency, Statement, StoreRequest, StoreResponse, SyncRequest,
    SyncResponse,
};
use jamsil_protocol::codec::{
    encode, encode_data_row, parse_password, parse_sasl_initial, read_message,
    read_message_budgeted, read_startup,
    BackendMessage, FrontendMessage, Startup, PROTOCOL_V3,
};
use jamsil_protocol::scram::ScramServer;
use jamsil_protocol::values::{decode_param, encode_binary, encode_text};
use jamsil_sql::types::oid;
use jamsil_sql::{classify, Classified, StmtKind};

use crate::{ServerCtx, SessionInfo};

pub fn now_text() -> String {
    let n = time::OffsetDateTime::now_utc();
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}+00",
        n.year(),
        n.month() as u8,
        n.day(),
        n.hour(),
        n.minute(),
        n.second()
    )
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + Sync> Io for T {}

#[derive(Debug, Clone)]
enum Tx {
    Idle,
    Block {
        txn: Id128,
        writes: bool,
        failed: bool,
        iso: Iso,
        read_only: bool,
        /// 고정된 읽기 Snapshot (id, applied_index) — REPEATABLE READ/SERIALIZABLE
        snap: Option<(Id128, u64)>,
        /// 첫 문장이 실행되었는가 (SET TRANSACTION 허용 여부)
        started: bool,
    },
}

/// 트랜잭션 격리 수준 (RQ-06).
///
/// * READ COMMITTED: 문장마다 최신 확정 Snapshot.
/// * REPEATABLE READ / SERIALIZABLE: 첫 문장에서 Snapshot을 고정한다. 쓰기는 클러스터 전체에서
///   직렬화되므로, 첫 쓰기 시점에 Snapshot 이후 다른 COMMIT이 있었다면 40001로 거부한다.
///   (첫 쓰기 이후에는 Writer를 독점하므로 트랜잭션 전체가 직렬 실행과 같다.)
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Iso {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl Iso {
    fn parse(v: &str) -> Result<Iso, DbError> {
        let v = v
            .trim()
            .trim_matches('\'')
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        match v.as_str() {
            // PostgreSQL과 같이 READ UNCOMMITTED는 READ COMMITTED로 동작한다.
            "read uncommitted" | "read committed" | "default" => Ok(Iso::ReadCommitted),
            "repeatable read" => Ok(Iso::RepeatableRead),
            "serializable" => Ok(Iso::Serializable),
            _ => Err(DbError::new(
                "",
                "22023",
                format!("invalid value for parameter \"transaction_isolation\": \"{v}\""),
            )
            .with_hint("available values: serializable, repeatable read, read committed, read uncommitted")),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Iso::ReadCommitted => "read committed",
            Iso::RepeatableRead => "repeatable read",
            Iso::Serializable => "serializable",
        }
    }
}

/// BEGIN/START TRANSACTION/SET TRANSACTION 의 트랜잭션 모드.
#[derive(Default, Debug)]
struct TxModes {
    iso: Option<Iso>,
    read_only: Option<bool>,
}

fn parse_tx_modes(sql: &str) -> Result<TxModes, DbError> {
    let up = sql
        .trim()
        .trim_end_matches(';')
        .replace(',', " ")
        .split_whitespace()
        .map(|w| w.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let mut m = TxModes::default();
    let mut i = 0;
    while i < up.len() {
        match up[i].as_str() {
            "ISOLATION" if up.get(i + 1).map(|s| s.as_str()) == Some("LEVEL") => {
                let a = up.get(i + 2).cloned().unwrap_or_default();
                let (lvl, n) = match a.as_str() {
                    "READ" | "REPEATABLE" => (format!("{a} {}", up.get(i + 3).cloned().unwrap_or_default()), 4),
                    _ => (a, 3),
                };
                m.iso = Some(Iso::parse(&lvl)?);
                i += n;
                continue;
            }
            "READ" if i == 0 || up[i - 1] != "LEVEL" => match up.get(i + 1).map(|s| s.as_str()) {
                Some("ONLY") => {
                    m.read_only = Some(true);
                    i += 2;
                    continue;
                }
                Some("WRITE") => {
                    m.read_only = Some(false);
                    i += 2;
                    continue;
                }
                _ => {}
            },
            _ => {}
        }
        i += 1;
    }
    Ok(m)
}

#[derive(Clone)]
struct Prepared {
    cls: Classified,
    param_types: Vec<u32>,
    describe: Option<DescribeResult>,
}

struct Portal {
    stmt: Prepared,
    params: Vec<Value>,
    result_formats: Vec<i16>,
    result: Option<QueryResult>,
    /// Describe로 확정한 결과 컬럼 (Execute 결과 인코딩 기준)
    columns: Option<Vec<ColumnDesc>>,
    sent: usize,
    done: bool,
}

pub struct Session {
    ctx: Arc<ServerCtx>,
    id: Id128,
    user: String,
    /// 수신은 버퍼링하고(메시지당 syscall 최소화), 송신은 out 버퍼를 모아 한 번에 보낸다.
    io: tokio::io::BufReader<Box<dyn Io>>,
    out: BytesMut,
    params: HashMap<String, String>,
    tx: Tx,
    stmts: HashMap<String, Prepared>,
    portals: HashMap<String, Portal>,
    ignore_till_sync: bool,
    cancel: Arc<Notify>,
    pid: i32,
    key: i32,
    default_iso: Iso,
    default_read_only: bool,
    /// 처리 중인 대형 PG 메시지의 메모리 예산 예약 (RQ-11)
    large_guards: Vec<jamsil_common::budget::BudgetGuard>,
}

const SERVER_VERSION: &str = "14.0";

fn status_byte(tx: &Tx) -> u8 {
    match tx {
        Tx::Idle => b'I',
        Tx::Block { failed: true, .. } => b'E',
        Tx::Block { .. } => b'T',
    }
}

pub async fn run(stream: TcpStream, addr: SocketAddr, ctx: Arc<ServerCtx>) {
    let mut io: Box<dyn Io> = Box::new(stream);
    let mut tls = false;
    // 시작 패킷 (SSLRequest 처리 포함)
    let startup = loop {
        let st = match tokio::time::timeout(Duration::from_secs(30), read_startup(&mut io)).await {
            Ok(Ok(Some(s))) => s,
            _ => return,
        };
        match st {
            Startup::Ssl => {
                if tls {
                    return;
                }
                match &ctx.tls {
                    Some(acc) => {
                        if io.write_all(b"S").await.is_err() {
                            return;
                        }
                        // Box<dyn Io>에서 TcpStream을 다시 꺼낼 수 없으므로 TLS는 원본 소켓 위에 바로 구성한다.
                        let raw: Box<dyn Io> = io;
                        match acc.accept(raw).await {
                            Ok(t) => {
                                io = Box::new(t);
                                tls = true;
                            }
                            Err(e) => {
                                tracing::debug!("TLS handshake with {addr} failed: {e}");
                                return;
                            }
                        }
                    }
                    None => {
                        if io.write_all(b"N").await.is_err() {
                            return;
                        }
                    }
                }
            }
            Startup::GssEnc => {
                if io.write_all(b"N").await.is_err() {
                    return;
                }
            }
            Startup::Cancel { pid, key } => {
                if let Some(n) = ctx.cancels.lock().get(&(pid, key)) {
                    n.notify_waiters();
                    n.notify_one();
                }
                return;
            }
            Startup::Startup { version, params } => break (version, params),
        }
    };
    let (version, sparams) = startup;
    let mut out = BytesMut::with_capacity(8192);
    let fail = |out: &mut BytesMut, e: DbError| {
        encode(out, &BackendMessage::Error(&e.fatal()));
    };
    if version != PROTOCOL_V3 {
        fail(
            &mut out,
            DbError::new(
                "",
                "0A000",
                format!(
                    "unsupported frontend protocol {}.{}",
                    version >> 16,
                    version & 0xffff
                ),
            ),
        );
        let _ = io.write_all(&out).await;
        return;
    }
    let sp: HashMap<String, String> = sparams.into_iter().collect();
    let user = sp.get("user").cloned().unwrap_or_default();
    let database = sp.get("database").cloned().unwrap_or_else(|| user.clone());
    if !ctx.access.is_allowed(addr.ip()) {
        jamsil_observability::audit!(client = %addr, user = %user, "connection rejected by IP policy");
        fail(
            &mut out,
            DbError::new(
                "",
                "28000",
                format!("no access rule allows host \"{}\"", addr.ip()),
            ),
        );
        let _ = io.write_all(&out).await;
        return;
    }
    if ctx.cfg.security.tls_required && !tls {
        jamsil_observability::audit!(client = %addr, user = %user, "connection rejected: TLS required");
        fail(
            &mut out,
            DbError::new("", "28000", "TLS is required (sslmode=require)"),
        );
        let _ = io.write_all(&out).await;
        return;
    }
    if database != ctx.cfg.database.name {
        fail(
            &mut out,
            DbError::new(
                "",
                "3D000",
                format!("database \"{database}\" does not exist"),
            ),
        );
        let _ = io.write_all(&out).await;
        return;
    }
    let permit = match ctx.conn_limit.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            fail(
                &mut out,
                DbError::new("", "53300", "sorry, too many clients already"),
            );
            let _ = io.write_all(&out).await;
            return;
        }
    };
    // 인증
    match authenticate(&ctx, &mut io, &user).await {
        Ok(()) => {}
        Err(e) => {
            jamsil_observability::audit!(client = %addr, user = %user, "authentication failed: {}", e.message);
            let mut out = BytesMut::new();
            encode(&mut out, &BackendMessage::Error(&e.fatal()));
            let _ = io.write_all(&out).await;
            return;
        }
    }
    jamsil_observability::audit!(client = %addr, user = %user, tls = tls, "connection authorized");
    let pid: i32 = (rand::random::<u32>() & 0x7fff_ffff) as i32;
    let key: i32 = rand::random();
    let cancel = Arc::new(Notify::new());
    ctx.cancels.lock().insert((pid, key), cancel.clone());
    let id = new_id();
    let mut params: HashMap<String, String> = HashMap::new();
    params.insert(
        "application_name".into(),
        sp.get("application_name").cloned().unwrap_or_default(),
    );
    params.insert(
        "jamsil.read_consistency".into(),
        ctx.cfg.database.read_consistency.clone(),
    );
    params.insert(
        "jamsil.search_consistency".into(),
        ctx.cfg.search.default_consistency.clone(),
    );
    params.insert(
        "statement_timeout".into(),
        ctx.cfg.network.statement_timeout_ms.to_string(),
    );
    ctx.sessions.lock().insert(
        id,
        SessionInfo {
            user: user.clone(),
            client_addr: addr.to_string(),
            application_name: params["application_name"].clone(),
            started_at: now_text(),
            state: "idle".into(),
            query: String::new(),
            query_started: None,
            last_latency_ms: 0,
        },
    );
    let mut s = Session {
        ctx: ctx.clone(),
        id,
        user,
        io: tokio::io::BufReader::with_capacity(64 * 1024, io),
        out: BytesMut::with_capacity(64 * 1024),
        params,
        tx: Tx::Idle,
        stmts: HashMap::new(),
        portals: HashMap::new(),
        ignore_till_sync: false,
        default_iso: Iso::ReadCommitted,
        default_read_only: false,
        large_guards: Vec::new(),
        cancel,
        pid,
        key,
    };
    s.send_startup_params(&sp);
    let _ = s.main_loop().await;
    s.cleanup().await;
    ctx.cancels.lock().remove(&(pid, key));
    ctx.sessions.lock().remove(&id);
    drop(permit);
    jamsil_observability::audit!(client = %addr, "disconnected");
}

async fn authenticate(ctx: &ServerCtx, io: &mut Box<dyn Io>, user: &str) -> Result<(), DbError> {
    let mut out = BytesMut::new();
    match ctx.users.method.as_str() {
        "trust" => {}
        "password" => {
            encode(&mut out, &BackendMessage::AuthCleartext);
            io.write_all(&out).await?;
            out.clear();
            let m = read_message(io, 1 << 16)
                .await?
                .ok_or_else(|| DbError::protocol("connection closed"))?;
            let FrontendMessage::Password(b) = m else {
                return Err(DbError::protocol("expected password message"));
            };
            let pw = parse_password(&b)?;
            if !ctx.users.check_password(user, &pw) {
                return Err(DbError::auth_failed(user));
            }
        }
        _ => {
            encode(&mut out, &BackendMessage::AuthSasl(&["SCRAM-SHA-256"]));
            io.write_all(&out).await?;
            out.clear();
            let m = read_message(io, 1 << 16)
                .await?
                .ok_or_else(|| DbError::protocol("connection closed"))?;
            let FrontendMessage::Password(b) = m else {
                return Err(DbError::protocol("expected SASL response"));
            };
            let (mech, data) = parse_sasl_initial(&b)?;
            if mech != "SCRAM-SHA-256" {
                return Err(DbError::new(
                    "",
                    "28000",
                    format!("unsupported SASL mechanism {mech}"),
                ));
            }
            let mut srv = ScramServer::new(ctx.users.users.get(user).map(|u| u.secret.clone()));
            let first = srv.first(&data)?;
            encode(&mut out, &BackendMessage::AuthSaslContinue(&first));
            io.write_all(&out).await?;
            out.clear();
            let m = read_message(io, 1 << 16)
                .await?
                .ok_or_else(|| DbError::protocol("connection closed"))?;
            let FrontendMessage::Password(b) = m else {
                return Err(DbError::protocol("expected SASL response"));
            };
            let fin = srv.finish(&b, user)?;
            encode(&mut out, &BackendMessage::AuthSaslFinal(&fin));
        }
    }
    if ctx.users.method != "trust" && !ctx.users.users.contains_key(user) {
        return Err(DbError::auth_failed(user));
    }
    encode(&mut out, &BackendMessage::AuthOk);
    io.write_all(&out).await?;
    Ok(())
}

impl Session {
    fn send_startup_params(&mut self, sp: &HashMap<String, String>) {
        let app = sp.get("application_name").cloned().unwrap_or_default();
        let user = self.user.clone();
        let ps: Vec<(&str, String)> = vec![
            ("server_version", SERVER_VERSION.to_string()),
            ("server_encoding", "UTF8".into()),
            ("client_encoding", "UTF8".into()),
            ("DateStyle", "ISO, MDY".into()),
            ("TimeZone", "UTC".into()),
            ("IntervalStyle", "postgres".into()),
            ("integer_datetimes", "on".into()),
            ("standard_conforming_strings", "on".into()),
            ("is_superuser", if self.is_superuser() { "on" } else { "off" }.into()),
            ("session_authorization", user),
            ("application_name", app),
            ("default_transaction_read_only", "off".into()),
            ("in_hot_standby", "off".into()),
        ];
        for (k, v) in &ps {
            encode(&mut self.out, &BackendMessage::ParameterStatus(k, v));
        }
        encode(
            &mut self.out,
            &BackendMessage::BackendKeyData(self.pid, self.key),
        );
        encode(&mut self.out, &BackendMessage::ReadyForQuery(b'I'));
    }

    async fn flush(&mut self) -> Result<(), DbError> {
        if !self.out.is_empty() {
            let w = self.io.get_mut();
            w.write_all(&self.out).await?;
            w.flush().await?;
            self.out.clear();
        }
        Ok(())
    }

    async fn maybe_flush(&mut self) -> Result<(), DbError> {
        if self.out.len() > 1 << 20 {
            self.flush().await?;
        }
        Ok(())
    }

    async fn main_loop(&mut self) -> Result<(), DbError> {
        loop {
            // 클라이언트가 보낸 메시지가 버퍼에 남아 있으면 이어서 처리하고, 다 처리했을 때만 송신한다.
            if self.io.buffer().is_empty() {
                self.flush().await?;
            }
            let idle = self.ctx.cfg.network.idle_session_timeout_ms;
            let budget_wait = Duration::from_millis(self.ctx.cfg.cluster.request_timeout_ms);
            let msg = if idle > 0 {
                match tokio::time::timeout(
                    Duration::from_millis(idle),
                    read_message_budgeted(&mut self.io, MAX_PG_MESSAGE_BYTES, budget_wait),
                )
                .await
                {
                    Ok(r) => r,
                    Err(_) => {
                        let e = DbError::new(
                            "",
                            "57P05",
                            "terminating connection due to idle-session timeout",
                        )
                        .fatal();
                        encode(&mut self.out, &BackendMessage::Error(&e));
                        let _ = self.flush().await;
                        return Ok(());
                    }
                }
            } else {
                read_message_budgeted(&mut self.io, MAX_PG_MESSAGE_BYTES, budget_wait).await
            };
            let msg = match msg {
                Ok(Some(Ok((m, guard)))) => {
                    if guard.bytes() > 0 {
                        // 메시지 처리(Bind된 Portal 포함)가 끝나는 ReadyForQuery까지 유지
                        self.large_guards.push(guard);
                    }
                    m
                }
                Ok(Some(Err((tag, e)))) => {
                    // 메모리 예산 부족: 이 메시지만 실패 (연결 유지)
                    if tag == b'Q' {
                        if let Tx::Block { failed, .. } = &mut self.tx {
                            *failed = true;
                        }
                        encode(&mut self.out, &BackendMessage::Error(&e));
                        encode(
                            &mut self.out,
                            &BackendMessage::ReadyForQuery(status_byte(&self.tx)),
                        );
                    } else if !self.ignore_till_sync {
                        self.ext_error(e);
                    }
                    continue;
                }
                Ok(None) => return Ok(()),
                Err(e) => {
                    if e.fatal {
                        encode(&mut self.out, &BackendMessage::Error(&e));
                        let _ = self.flush().await;
                    }
                    return Ok(());
                }
            };
            // Query/Sync 처리가 끝나면(ReadyForQuery) 이번 요청 주기의 대형 메시지 예약을 반납한다.
            let cycle_end = matches!(msg, FrontendMessage::Query(_) | FrontendMessage::Sync);
            if self.ignore_till_sync
                && !matches!(msg, FrontendMessage::Sync | FrontendMessage::Terminate)
            {
                continue;
            }
            match msg {
                FrontendMessage::Query(sql) => self.simple_query(sql).await?,
                FrontendMessage::Parse {
                    name,
                    sql,
                    param_types,
                } => {
                    if let Err(e) = self.parse(name, sql, param_types) {
                        self.ext_error(e);
                    }
                }
                FrontendMessage::Bind {
                    portal,
                    statement,
                    param_formats,
                    params,
                    result_formats,
                } => match self.bind(portal, statement, param_formats, params, result_formats) {
                    Ok(()) => encode(&mut self.out, &BackendMessage::BindComplete),
                    Err(e) => self.ext_error(e),
                },
                FrontendMessage::Describe { kind, name } => {
                    if let Err(e) = self.describe(kind, name).await {
                        self.ext_error(e);
                    }
                }
                FrontendMessage::Execute { portal, max_rows } => {
                    if let Err(e) = self.execute_portal(portal, max_rows).await {
                        self.ext_error(e);
                    }
                }
                FrontendMessage::Sync => {
                    self.ignore_till_sync = false;
                    let s = status_byte(&self.tx);
                    encode(&mut self.out, &BackendMessage::ReadyForQuery(s));
                }
                FrontendMessage::Flush => self.flush().await?,
                FrontendMessage::Close { kind, name } => {
                    if kind == b'S' {
                        self.stmts.remove(&name);
                    } else {
                        self.portals.remove(&name);
                    }
                    encode(&mut self.out, &BackendMessage::CloseComplete);
                }
                FrontendMessage::Terminate => return Ok(()),
                FrontendMessage::Password(_) => {
                    self.ext_error(DbError::protocol("unexpected password message"));
                }
                FrontendMessage::CopyData
                | FrontendMessage::CopyDone
                | FrontendMessage::CopyFail => {}
                FrontendMessage::FunctionCall => {
                    self.ext_error(DbError::unsupported("fastpath function call"));
                    encode(
                        &mut self.out,
                        &BackendMessage::ReadyForQuery(status_byte(&self.tx)),
                    );
                }
            }
            if cycle_end {
                self.large_guards.clear();
            }
        }
    }

    fn ext_error(&mut self, e: DbError) {
        if let Tx::Block { failed, .. } = &mut self.tx {
            *failed = true;
        }
        encode(&mut self.out, &BackendMessage::Error(&e));
        self.ignore_till_sync = true;
    }

    /// 트랜잭션 종료: 고정 Snapshot 해제 (응답을 기다리지 않는다).
    fn end_tx(&mut self) {
        if let Tx::Block {
            snap: Some((sid, _)),
            ..
        } = std::mem::replace(&mut self.tx, Tx::Idle)
        {
            let ctx = self.ctx.clone();
            tokio::spawn(async move {
                let _ = ctx
                    .store
                    .call(&StoreRequest::SnapshotClose { snapshot: sid })
                    .await;
            });
        }
    }

    fn new_block(&self, m: TxModes) -> Tx {
        Tx::Block {
            txn: new_id(),
            writes: false,
            failed: false,
            iso: m.iso.unwrap_or(self.default_iso),
            read_only: m.read_only.unwrap_or(self.default_read_only),
            snap: None,
            started: false,
        }
    }

    /// REPEATABLE READ/SERIALIZABLE 트랜잭션의 Snapshot index (첫 쓰기 검증용).
    fn snapshot_index(&self) -> Option<u64> {
        match &self.tx {
            Tx::Block {
                iso,
                snap: Some((_, idx)),
                ..
            } if *iso != Iso::ReadCommitted => Some(*idx),
            _ => None,
        }
    }

    async fn cleanup(&mut self) {
        let tx = self.tx.clone();
        self.end_tx();
        if let Tx::Block {
            txn, writes: true, ..
        } = tx
        {
            let _ = self
                .ctx
                .sync
                .call(&SyncRequest::SessionClosed { txns: vec![txn] })
                .await;
        }
        self.tx = Tx::Idle;
    }

    fn set_state(&self, state: &str, query: &str) {
        if let Some(i) = self.ctx.sessions.lock().get_mut(&self.id) {
            if state == "active" {
                i.query_started = Some(Instant::now());
                i.query = query.chars().take(1024).collect();
            } else if let Some(t) = i.query_started.take() {
                i.last_latency_ms = t.elapsed().as_millis() as u64;
            }
            i.state = state.to_string();
        }
    }

    // ------------------------------------------------------------ simple query

    async fn simple_query(&mut self, sql: String) -> Result<(), DbError> {
        let stmts = match classify(&sql) {
            Ok(s) => s,
            Err(e) => {
                if let Tx::Block { failed, .. } = &mut self.tx {
                    *failed = true;
                }
                encode(&mut self.out, &BackendMessage::Error(&e));
                encode(
                    &mut self.out,
                    &BackendMessage::ReadyForQuery(status_byte(&self.tx)),
                );
                return Ok(());
            }
        };
        // 여러 문장(트랜잭션 제어 없음)은 암묵적 트랜잭션으로 원자 실행
        let has_control = stmts.iter().any(|c| {
            matches!(
                c.kind,
                StmtKind::Begin
                    | StmtKind::Commit
                    | StmtKind::Rollback
                    | StmtKind::Savepoint(_)
                    | StmtKind::Release(_)
                    | StmtKind::RollbackTo(_)
            )
        });
        let implicit = stmts.len() > 1 && !has_control && matches!(self.tx, Tx::Idle);
        if implicit {
            self.tx = self.new_block(TxModes::default());
        }
        let mut failed = false;
        for c in stmts {
            if c.kind == StmtKind::Empty {
                encode(&mut self.out, &BackendMessage::EmptyQuery);
                continue;
            }
            match self.run(&c, vec![], &[]).await {
                Ok(r) => {
                    self.write_result(&r, &[], true, None)?;
                    self.maybe_flush().await?;
                }
                Err(e) => {
                    if let Tx::Block { failed, .. } = &mut self.tx {
                        *failed = true;
                    }
                    encode(&mut self.out, &BackendMessage::Error(&e));
                    failed = true;
                    break;
                }
            }
        }
        if implicit {
            let ok = !failed;
            if let Tx::Block { txn, writes, .. } = self.tx.clone() {
                self.end_tx();
                if writes {
                    let req = if ok {
                        SyncRequest::TxCommit {
                            txn,
                            request_id: self.params.remove("jamsil.request_id"),
                        }
                    } else {
                        SyncRequest::TxRollback { txn }
                    };
                    if let Err(e) = self.sync_call(req).await {
                        if ok {
                            encode(&mut self.out, &BackendMessage::Error(&e));
                        }
                    }
                }
            }
        }
        encode(
            &mut self.out,
            &BackendMessage::ReadyForQuery(status_byte(&self.tx)),
        );
        Ok(())
    }

    /// 결과 출력. `describe`이면 RowDescription도 보낸다.
    fn write_result(
        &mut self,
        r: &QueryResult,
        formats: &[i16],
        describe: bool,
        cols_override: Option<&[ColumnDesc]>,
    ) -> Result<(), DbError> {
        for n in &r.notices {
            encode(&mut self.out, &BackendMessage::Notice(n));
        }
        if let Some(cols) = &r.columns {
            let cols: &[ColumnDesc] = cols_override.unwrap_or(cols);
            if describe {
                encode(
                    &mut self.out,
                    &BackendMessage::RowDescription(cols, formats),
                );
            }
            for row in &r.rows {
                self.encode_row(row, cols, formats)?;
            }
        }
        encode(&mut self.out, &BackendMessage::CommandComplete(&r.tag));
        Ok(())
    }

    fn encode_row(
        &mut self,
        row: &[Value],
        cols: &[ColumnDesc],
        formats: &[i16],
    ) -> Result<(), DbError> {
        let mut enc = Vec::with_capacity(row.len());
        for (i, v) in row.iter().enumerate() {
            let t = cols.get(i).map(|c| c.type_oid).unwrap_or(oid::TEXT);
            let f = match formats.len() {
                0 => 0,
                1 => formats[0],
                _ => formats.get(i).copied().unwrap_or(0),
            };
            enc.push(if f == 1 {
                encode_binary(v, t)?
            } else {
                encode_text(v, t)
            });
        }
        encode_data_row(&mut self.out, &enc);
        Ok(())
    }

    // ------------------------------------------------------------- execution

    async fn sync_call(&self, req: SyncRequest) -> Result<SyncResponse, DbError> {
        match self.ctx.sync.call(&req).await {
            Ok(r) => r,
            Err(e) => {
                if matches!(req, SyncRequest::Exec { .. } | SyncRequest::TxCommit { .. })
                    && e == jamsil_ipc::RpcError::Disconnected
                {
                    Err(DbError::commit_unknown(format!(
                        "sync process connection lost: {e}"
                    )))
                } else {
                    Err(DbError::unavailable(format!(
                        "sync process unavailable: {e}"
                    )))
                }
            }
        }
    }

    async fn store_call(&self, req: StoreRequest) -> Result<StoreResponse, DbError> {
        match self.ctx.store.call(&req).await {
            Ok(r) => r,
            Err(e) => Err(DbError::store_recovery_required(format!(
                "store process unavailable: {e}"
            ))),
        }
    }

    fn statement_timeout(&self) -> Option<Duration> {
        let ms: u64 = self
            .params
            .get("statement_timeout")
            .and_then(|v| v.trim_end_matches("ms").trim().parse().ok())
            .unwrap_or(0);
        if ms > 0 {
            Some(Duration::from_millis(ms))
        } else {
            None
        }
    }

    /// 문장 하나 실행 (취소/타임아웃 포함).
    async fn run(
        &mut self,
        c: &Classified,
        params: Vec<Value>,
        ptypes: &[u32],
    ) -> Result<QueryResult, DbError> {
        if let Tx::Block { failed: true, .. } = self.tx {
            if !matches!(
                c.kind,
                StmtKind::Commit | StmtKind::Rollback | StmtKind::RollbackTo(_)
            ) {
                return Err(DbError::in_failed_tx());
            }
        }
        self.check_snapshot_search(c)?;
        self.ctx.queries.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        self.set_state("active", &c.sql);
        let is_write = c.kind.is_write();
        let cancel = self.cancel.clone();
        let timeout = self.statement_timeout();
        let fut = self.run_inner(c, params, ptypes);
        let r = tokio::select! {
            r = fut => r,
            _ = cancel.notified() => Err(if is_write {
                DbError::commit_unknown("statement was canceled while the write was in flight")
            } else {
                DbError::new("", "57014", "canceling statement due to user request")
            }),
            _ = async { match timeout { Some(t) => tokio::time::sleep(t).await, None => std::future::pending().await } } => Err(if is_write {
                DbError::commit_unknown("statement timeout while the write was in flight")
            } else {
                DbError::new("", "57014", "canceling statement due to statement timeout")
            }),
        };
        let label = match c.kind {
            StmtKind::Read => "kind=read",
            StmtKind::Write => "kind=write",
            StmtKind::Ddl => "kind=ddl",
            _ => "kind=other",
        };
        jamsil_common::metrics::histogram("jamsil_sql_duration_seconds", label, "seconds")
            .record_duration(started.elapsed());
        if !is_write && matches!(&r, Err(e) if e.sqlstate == "57014") {
            // 취소/시간 초과: Store의 진행 중 읽기도 중단시킨다.
            let _ = self.ctx.store.call_timeout(&StoreRequest::CancelQuery { session: self.id }, Duration::from_secs(2)).await;
        }
        self.set_state(
            if matches!(self.tx, Tx::Idle) {
                "idle"
            } else {
                "idle in transaction"
            },
            "",
        );
        if self.ctx.cfg.logging.query_log {
            jamsil_observability::query_log!(
                session = %jamsil_common::id_to_string(self.id),
                user = %self.user,
                ms = started.elapsed().as_millis() as u64,
                ok = r.is_ok(),
                "{}",
                c.sql.chars().take(4096).collect::<String>()
            );
        }
        if c.kind == StmtKind::Ddl && r.is_ok() {
            jamsil_observability::audit!(user = %self.user, "DDL: {}", c.sql.chars().take(4096).collect::<String>());
        }
        r
    }

    async fn run_inner(
        &mut self,
        c: &Classified,
        params: Vec<Value>,
        ptypes: &[u32],
    ) -> Result<QueryResult, DbError> {
        let tag = |t: &str| QueryResult {
            tag: t.to_string(),
            ..Default::default()
        };
        match &c.kind {
            StmtKind::Empty => Ok(tag("")),
            StmtKind::Begin => {
                if matches!(self.tx, Tx::Block { .. }) {
                    let mut r = tag("BEGIN");
                    r.notices
                        .push("there is already a transaction in progress".into());
                    return Ok(r);
                }
                let modes = parse_tx_modes(&c.sql)?;
                self.tx = self.new_block(modes);
                Ok(tag("BEGIN"))
            }
            StmtKind::Commit => match self.tx.clone() {
                Tx::Idle => {
                    let mut r = tag("COMMIT");
                    r.notices.push("there is no transaction in progress".into());
                    Ok(r)
                }
                Tx::Block {
                    txn,
                    writes,
                    failed,
                    ..
                } => {
                    self.end_tx();
                    if failed {
                        if writes {
                            let _ = self.sync_call(SyncRequest::TxRollback { txn }).await;
                        }
                        return Ok(tag("ROLLBACK"));
                    }
                    if writes {
                        let rid = self.params.remove("jamsil.request_id");
                        self.sync_call(SyncRequest::TxCommit {
                            txn,
                            request_id: rid,
                        })
                        .await?;
                    }
                    Ok(tag("COMMIT"))
                }
            },
            StmtKind::Rollback => {
                if let Tx::Block { txn, writes, .. } = self.tx.clone() {
                    self.end_tx();
                    if writes {
                        self.sync_call(SyncRequest::TxRollback { txn }).await?;
                    }
                    Ok(tag("ROLLBACK"))
                } else {
                    let mut r = tag("ROLLBACK");
                    r.notices.push("there is no transaction in progress".into());
                    Ok(r)
                }
            }
            StmtKind::Savepoint(_) | StmtKind::Release(_) | StmtKind::RollbackTo(_) => {
                let Tx::Block { txn, failed, .. } = self.tx.clone() else {
                    return Err(DbError::new(
                        "",
                        "25P01",
                        format!(
                            "{} can only be used in transaction blocks",
                            c.sql
                                .split_whitespace()
                                .next()
                                .unwrap_or("SAVEPOINT")
                                .to_uppercase()
                        ),
                    ));
                };
                let stmt = Statement {
                    sql: c.sql.clone(),
                    params: vec![],
                    param_types: vec![],
                    user: self.user.clone(),
                };
                let r = self.sync_call(SyncRequest::TxExec {
                    txn,
                    stmt,
                    expect_applied: self.snapshot_index(),
                }).await;
                if let Tx::Block {
                    writes, failed: f, ..
                } = &mut self.tx
                {
                    *writes = true;
                    if matches!(c.kind, StmtKind::RollbackTo(_)) && r.is_ok() && failed {
                        *f = false;
                    }
                }
                r?;
                Ok(tag(match c.kind {
                    StmtKind::Savepoint(_) => "SAVEPOINT",
                    StmtKind::Release(_) => "RELEASE",
                    _ => "ROLLBACK",
                }))
            }
            StmtKind::Set { name, value, .. } => {
                self.set_param(name, value)?;
                Ok(tag("SET"))
            }
            StmtKind::Reset(name) => {
                if name == "all" {
                    self.params.retain(|k, _| k == "application_name");
                } else {
                    self.params.remove(name);
                }
                Ok(tag("RESET"))
            }
            StmtKind::Show(name) => Ok(self.show(name)),
            StmtKind::SetTransaction => {
                let modes = parse_tx_modes(&c.sql)?;
                let session_level = c.sql.to_ascii_lowercase().contains("characteristics");
                if session_level {
                    if let Some(i) = modes.iso {
                        self.default_iso = i;
                    }
                    if let Some(r) = modes.read_only {
                        self.default_read_only = r;
                    }
                    return Ok(tag("SET"));
                }
                match &mut self.tx {
                    Tx::Idle => {
                        let mut r = tag("SET");
                        r.notices
                            .push("SET TRANSACTION can only be used in transaction blocks".into());
                        Ok(r)
                    }
                    Tx::Block {
                        iso,
                        read_only,
                        started,
                        writes,
                        ..
                    } => {
                        if modes.iso.is_some() && *started {
                            return Err(DbError::new(
                                "",
                                "25001",
                                "SET TRANSACTION ISOLATION LEVEL must be called before any query",
                            ));
                        }
                        if modes.read_only == Some(false) && *read_only && *started {
                            return Err(DbError::new(
                                "",
                                "25001",
                                "transaction read-write mode must be set before any query",
                            ));
                        }
                        if modes.read_only == Some(true) && *writes {
                            return Err(DbError::new(
                                "",
                                "25001",
                                "transaction read-only mode must be set before any write",
                            ));
                        }
                        if let Some(i) = modes.iso {
                            *iso = i;
                        }
                        if let Some(r) = modes.read_only {
                            *read_only = r;
                        }
                        Ok(tag("SET"))
                    }
                }
            }
            StmtKind::Discard => {
                self.stmts.clear();
                self.portals.clear();
                Ok(tag("DISCARD ALL"))
            }
            StmtKind::Deallocate(n) => {
                match n {
                    Some(n) => {
                        self.stmts.remove(n);
                    }
                    None => self.stmts.clear(),
                }
                Ok(tag("DEALLOCATE"))
            }
            StmtKind::Unsupported(what) => Err(DbError::unsupported(what.clone())),
            StmtKind::Read => {
                let stmt = Statement {
                    sql: c.sql.clone(),
                    params,
                    param_types: ptypes.to_vec(),
                    user: self.user.clone(),
                };
                self.mark_started();
                if let Tx::Block {
                    iso,
                    writes: false,
                    snap,
                    ..
                } = &self.tx
                {
                    if *iso != Iso::ReadCommitted {
                        let sid = match snap {
                            Some((sid, _)) => *sid,
                            None => self.open_snapshot().await?,
                        };
                        return self.read_at(stmt, c.search_committed, Some(sid)).await;
                    }
                }
                if let Tx::Block {
                    txn, writes: true, ..
                } = self.tx
                {
                    // 같은 트랜잭션의 쓰기를 보려면 Leader Writer에서 읽는다.
                    return match self.sync_call(SyncRequest::TxExec {
                    txn,
                    stmt,
                    expect_applied: self.snapshot_index(),
                }).await? {
                        SyncResponse::Result(r) => Ok(r),
                        _ => Err(DbError::internal("unexpected sync response")),
                    };
                }
                self.read(stmt, c.search_committed).await
            }
            StmtKind::Write | StmtKind::Ddl => {
                let stmt = Statement {
                    sql: c.sql.clone(),
                    params,
                    param_types: ptypes.to_vec(),
                    user: self.user.clone(),
                };
                let ro = match &self.tx {
                    Tx::Block { read_only, .. } => *read_only,
                    Tx::Idle => self.default_read_only,
                };
                if ro {
                    return Err(DbError::new(
                        "",
                        "25006",
                        format!(
                            "cannot execute {} in a read-only transaction",
                            c.sql
                                .split_whitespace()
                                .next()
                                .unwrap_or("statement")
                                .to_ascii_uppercase()
                        ),
                    ));
                }
                self.mark_started();
                match self.tx.clone() {
                    Tx::Block { txn, .. } => {
                        if let Tx::Block { writes, .. } = &mut self.tx {
                            *writes = true;
                        }
                        match self.sync_call(SyncRequest::TxExec {
                    txn,
                    stmt,
                    expect_applied: self.snapshot_index(),
                }).await? {
                            SyncResponse::Result(r) => Ok(r),
                            _ => Err(DbError::internal("unexpected sync response")),
                        }
                    }
                    Tx::Idle => {
                        let rid = self.params.remove("jamsil.request_id");
                        match self
                            .sync_call(SyncRequest::Exec {
                                txn: new_id(),
                                stmt,
                                request_id: rid,
                            })
                            .await?
                        {
                            SyncResponse::Result(r) => Ok(r),
                            _ => Err(DbError::internal("unexpected sync response")),
                        }
                    }
                }
            }
        }
    }

    async fn read(
        &mut self,
        stmt: Statement,
        search_committed: Option<bool>,
    ) -> Result<QueryResult, DbError> {
        self.read_at(stmt, search_committed, None).await
    }

    fn mark_started(&mut self) {
        if let Tx::Block { started, .. } = &mut self.tx {
            *started = true;
        }
    }

    /// REPEATABLE READ/SERIALIZABLE: 첫 읽기에서 확정 index 이후의 Snapshot을 고정한다.
    async fn open_snapshot(&mut self) -> Result<Id128, DbError> {
        let min_applied = self.read_index_for_metadata().await?;
        let sid = new_id();
        let r = self
            .store_call(StoreRequest::SnapshotOpen {
                snapshot: sid,
                min_applied,
                wait_timeout_ms: self.ctx.cfg.cluster.request_timeout_ms,
            })
            .await?;
        let StoreResponse::Applied { index } = r else {
            return Err(DbError::internal("unexpected store response"));
        };
        if let Tx::Block { snap, .. } = &mut self.tx {
            *snap = Some((sid, index));
        }
        Ok(sid)
    }

    async fn read_at(
        &mut self,
        stmt: Statement,
        search_committed: Option<bool>,
        snapshot: Option<Id128>,
    ) -> Result<QueryResult, DbError> {
        let consistency = self
            .params
            .get("jamsil.read_consistency")
            .map(|s| s.as_str())
            .unwrap_or("linearizable");
        let min_applied = if consistency == "local" || snapshot.is_some() {
            0
        } else {
            match self.sync_call(SyncRequest::ReadIndex).await? {
                SyncResponse::ReadIndex { index } => index,
                _ => return Err(DbError::internal("unexpected sync response")),
            }
        };
        let sc = match search_committed {
            Some(true) => SearchConsistency::Committed,
            Some(false) => SearchConsistency::Eventual,
            None => {
                if self
                    .params
                    .get("jamsil.search_consistency")
                    .map(|s| s == "committed")
                    .unwrap_or(false)
                {
                    SearchConsistency::Committed
                } else {
                    SearchConsistency::Eventual
                }
            }
        };
        let lower = stmt.sql.to_ascii_lowercase();
        let virtual_tables = if lower.contains("jamsil_catalog") {
            crate::vcatalog::build(&self.ctx, &lower).await
        } else {
            vec![]
        };
        let req = StoreRequest::Query {
            session: self.id,
            stmt,
            min_applied,
            wait_timeout_ms: self.ctx.cfg.cluster.request_timeout_ms,
            search_consistency: sc,
            virtual_tables,
            snapshot,
        };
        match self.store_call(req).await? {
            StoreResponse::Result(mut r) => {
                if consistency == "local" {
                    r.notices
                        .push("read_consistency=local: result may be stale".into());
                }
                Ok(r)
            }
            _ => Err(DbError::internal("unexpected store response")),
        }
    }

    fn set_param(&mut self, name: &str, value: &str) -> Result<(), DbError> {
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim().trim_matches('\'').to_string();
        match name.as_str() {
            "client_encoding" | "names" => {
                let v = value.to_ascii_uppercase().replace('-', "");
                if v != "UTF8" && v != "UNICODE" && v != "DEFAULT" {
                    return Err(DbError::new(
                        "",
                        "22023",
                        format!("client_encoding {value} is not supported (UTF8 only)"),
                    ));
                }
                return Ok(());
            }
            "default_transaction_isolation" => {
                self.default_iso = Iso::parse(&value)?;
                return Ok(());
            }
            "default_transaction_read_only" => {
                self.default_read_only = parse_bool_setting(&name, &value)?;
                return Ok(());
            }
            "transaction_isolation" | "transaction_read_only" => {
                let sql = if name == "transaction_isolation" {
                    format!("SET TRANSACTION ISOLATION LEVEL {value}")
                } else if parse_bool_setting(&name, &value)? {
                    "SET TRANSACTION READ ONLY".to_string()
                } else {
                    "SET TRANSACTION READ WRITE".to_string()
                };
                if let Tx::Block {
                    iso,
                    read_only,
                    started,
                    ..
                } = &mut self.tx
                {
                    let m = parse_tx_modes(&sql)?;
                    if *started && m.iso.is_some_and(|i| i != *iso) {
                        return Err(DbError::new(
                            "",
                            "25001",
                            "SET TRANSACTION ISOLATION LEVEL must be called before any query",
                        ));
                    }
                    if let Some(i) = m.iso {
                        *iso = i;
                    }
                    if let Some(r) = m.read_only {
                        *read_only = r;
                    }
                } else {
                    // 트랜잭션 밖: PostgreSQL처럼 효과 없음 (값 검증만)
                    parse_tx_modes(&sql)?;
                }
                return Ok(());
            }
            "jamsil.read_consistency" => {
                if value != "linearizable" && value != "local" {
                    return Err(DbError::invalid_param(
                        "jamsil.read_consistency must be linearizable or local",
                    ));
                }
            }
            "jamsil.search_consistency" => {
                if value != "eventual" && value != "committed" {
                    return Err(DbError::invalid_param(
                        "jamsil.search_consistency must be eventual or committed",
                    ));
                }
            }
            "jamsil.request_id" => {
                if value.is_empty() || value.len() > 200 {
                    return Err(DbError::invalid_param(
                        "jamsil.request_id must be 1..200 characters",
                    ));
                }
            }
            "application_name" => {
                if let Some(i) = self.ctx.sessions.lock().get_mut(&self.id) {
                    i.application_name = value.clone();
                }
                encode(
                    &mut self.out,
                    &BackendMessage::ParameterStatus("application_name", &value),
                );
            }
            "datestyle" => {
                if !value.to_ascii_uppercase().contains("ISO") {
                    return Err(DbError::invalid_param("only ISO DateStyle is supported"));
                }
                return Ok(());
            }
            "timezone" | "time zone" => {
                let up = value.to_ascii_uppercase();
                if !matches!(
                    up.as_str(),
                    "UTC" | "GMT" | "Z" | "+00" | "+00:00" | "ETC/UTC" | "DEFAULT" | "LOCAL"
                ) {
                    // 시간대 변환은 지원하지 않는다: 저장/표시는 UTC 기준.
                    self.out.extend_from_slice(&[]);
                }
                return Ok(());
            }
            _ => {}
        }
        self.params.insert(name, value);
        Ok(())
    }

    fn show(&self, name: &str) -> QueryResult {
        let name = name.trim().to_ascii_lowercase();
        let fixed = |k: &str| -> Option<String> {
            Some(match k {
                "server_version" => SERVER_VERSION.into(),
                "server_version_num" => "140000".into(),
                "server_encoding" | "client_encoding" => "UTF8".into(),
                "datestyle" => "ISO, MDY".into(),
                "timezone" | "time zone" => "UTC".into(),
                "transaction_isolation" | "transaction isolation level" => match &self.tx {
                    Tx::Block { iso, .. } => iso.name().into(),
                    Tx::Idle => self.default_iso.name().into(),
                },
                "default_transaction_isolation" => self.default_iso.name().into(),
                "is_superuser" => if self.is_superuser() { "on" } else { "off" }.into(),
                "session_authorization" => self.user.clone(),
                "standard_conforming_strings" => "on".into(),
                "integer_datetimes" => "on".into(),
                "search_path" => "public".into(),
                "max_identifier_length" => "63".into(),
                "lc_collate" | "lc_ctype" => "C.UTF-8".into(),
                "transaction_read_only" => {
                    let ro = match &self.tx {
                        Tx::Block { read_only, .. } => *read_only,
                        Tx::Idle => self.default_read_only,
                    };
                    if ro { "on" } else { "off" }.into()
                }
                "default_transaction_read_only" => {
                    if self.default_read_only { "on" } else { "off" }.into()
                }
                "max_connections" => self.ctx.cfg.network.max_connections.to_string(),
                "port" => self.ctx.cfg.network.port.to_string(),
                _ => return None,
            })
        };
        if name == "all" {
            let mut rows: Vec<Vec<Value>> = [
                "server_version",
                "server_encoding",
                "client_encoding",
                "datestyle",
                "timezone",
                "transaction_isolation",
                "standard_conforming_strings",
                "search_path",
            ]
            .iter()
            .map(|k| {
                vec![
                    Value::Text(k.to_string()),
                    Value::Text(fixed(k).unwrap_or_default()),
                    Value::Text(String::new()),
                ]
            })
            .collect();
            for (k, v) in &self.params {
                rows.push(vec![
                    Value::Text(k.clone()),
                    Value::Text(v.clone()),
                    Value::Text(String::new()),
                ]);
            }
            let n = rows.len();
            return QueryResult {
                columns: Some(vec![
                    ColumnDesc::simple("name", oid::TEXT),
                    ColumnDesc::simple("setting", oid::TEXT),
                    ColumnDesc::simple("description", oid::TEXT),
                ]),
                rows,
                tag: format!("SHOW {n}"),
                ..Default::default()
            };
        }
        let v = self
            .params
            .get(&name)
            .cloned()
            .or_else(|| fixed(&name))
            .unwrap_or_default();
        let col = if name == "transaction isolation level" {
            "transaction_isolation".to_string()
        } else {
            name.clone()
        };
        QueryResult {
            columns: Some(vec![ColumnDesc::simple(col, oid::TEXT)]),
            rows: vec![vec![Value::Text(v)]],
            tag: "SHOW".into(),
            ..Default::default()
        }
    }

    // ---------------------------------------------------------- extended query

    fn parse(&mut self, name: String, sql: String, param_types: Vec<u32>) -> Result<(), DbError> {
        let mut v = classify(&sql)?;
        if v.len() > 1 {
            return Err(DbError::syntax(
                "cannot insert multiple commands into a prepared statement",
            ));
        }
        let cls = v.pop().unwrap();
        self.stmts.insert(
            name,
            Prepared {
                cls,
                param_types,
                describe: None,
            },
        );
        encode(&mut self.out, &BackendMessage::ParseComplete);
        Ok(())
    }

    fn bind(
        &mut self,
        portal: String,
        statement: String,
        param_formats: Vec<i16>,
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    ) -> Result<(), DbError> {
        let st = self.stmts.get(&statement).cloned().ok_or_else(|| {
            DbError::new(
                "",
                "26000",
                format!("prepared statement \"{statement}\" does not exist"),
            )
        })?;
        let mut vals = Vec::with_capacity(params.len());
        for (i, p) in params.into_iter().enumerate() {
            let f = match param_formats.len() {
                0 => 0,
                1 => param_formats[0],
                _ => param_formats.get(i).copied().unwrap_or(0),
            };
            let t = st
                .param_types
                .get(i)
                .copied()
                .filter(|t| *t != 0)
                .or_else(|| {
                    st.describe
                        .as_ref()
                        .and_then(|d| d.param_types.get(i).copied())
                })
                .unwrap_or(0);
            vals.push(decode_param(p, t, f == 1)?);
        }
        self.portals.insert(
            portal,
            Portal {
                stmt: st,
                params: vals,
                result_formats,
                result: None,
                columns: None,
                sent: 0,
                done: false,
            },
        );
        Ok(())
    }

    /// 설정 사용자 목록이 없으면(trust 개발 모드) 모두 superuser.
    fn is_superuser(&self) -> bool {
        let cfg = &self.ctx.cfg.security.users;
        cfg.is_empty()
            || cfg
                .iter()
                .find(|u| u.name == self.user)
                .is_some_and(|u| u.effective_role() == jamsil_common::acl::Role::Superuser)
    }

    fn tx_has_writes(&self) -> bool {
        matches!(self.tx, Tx::Block { writes: true, .. })
    }

    /// 메타데이터(Describe) 전에 필요한 적용 index. local 모드면 0.
    async fn read_index_for_metadata(&self) -> Result<u64, DbError> {
        if self.params.get("jamsil.read_consistency").map(|s| s.as_str()) == Some("local") {
            return Ok(0);
        }
        match self.sync_call(SyncRequest::ReadIndex).await? {
            SyncResponse::ReadIndex { index } => Ok(index),
            _ => Err(DbError::internal("unexpected sync response")),
        }
    }

    /// R2-03: 고정 Snapshot(REPEATABLE READ/SERIALIZABLE) 트랜잭션의 TEXT_MATCH/TEXT_SCORE 거부.
    /// 검색 인덱스는 현재 문서만 있어 Snapshot 행과 결합하면 조건이 거짓인 행이 나올 수 있으므로,
    /// 쓰기 여부·문장 종류·프로토콜과 관계없이 같은 오류(JDB-4105)로 거부한다.
    /// 준비된 문장은 다른 격리 수준의 트랜잭션에서 실행될 수 있어 Parse가 아니라 Describe/실행 때마다 확인한다.
    fn check_snapshot_search(&self, c: &Classified) -> Result<(), DbError> {
        match &self.tx {
            Tx::Block { iso, .. } if c.uses_search && *iso != Iso::ReadCommitted => {
                Err(DbError::search_in_snapshot_txn(iso.name()))
            }
            _ => Ok(()),
        }
    }

    async fn describe_stmt(&self, st: &Prepared) -> Result<DescribeResult, DbError> {
        self.check_snapshot_search(&st.cls)?;
        if let Some(d) = &st.describe {
            return Ok(d.clone());
        }
        match &st.cls.kind {
            StmtKind::Read | StmtKind::Write => {
                let stmt = Statement {
                    sql: st.cls.sql.clone(),
                    params: vec![],
                    param_types: st.param_types.clone(),
                    user: self.user.clone(),
                };
                // 다른 Endpoint/이전 Leader에서 방금 COMMIT된 DDL도 보이도록 확정 index까지 적용을 기다린다.
                let min_applied = self.read_index_for_metadata().await?;
                let r = match self
                    .store_call(StoreRequest::Describe {
                        stmt,
                        min_applied,
                        wait_timeout_ms: self.ctx.cfg.cluster.request_timeout_ms,
                    })
                    .await
                {
                    Ok(StoreResponse::Describe(d)) => Ok(d),
                    Ok(_) => Err(DbError::internal("unexpected store response")),
                    Err(e) => Err(e),
                };
                match r {
                    // 같은 트랜잭션에서 아직 COMMIT 전인 DDL의 객체: 설명 없이 진행 (실행은 Leader Writer에서)
                    Err(e)
                        if e.sqlstate == "42P01"
                            && self.tx_has_writes()
                            && st.cls.kind == StmtKind::Write =>
                    {
                        Ok(DescribeResult {
                            param_types: placeholder_types(&st.cls.sql, &st.param_types),
                            columns: None,
                        })
                    }
                    other => other,
                }
            }
            StmtKind::Show(n) => Ok(DescribeResult {
                param_types: vec![],
                columns: Some(vec![ColumnDesc::simple(
                    if n.as_str() == "transaction isolation level" {
                        "transaction_isolation".to_string()
                    } else {
                        n.clone()
                    },
                    oid::TEXT,
                )]),
            }),
            _ => Ok(DescribeResult {
                param_types: st.param_types.clone(),
                columns: None,
            }),
        }
    }

    async fn describe(&mut self, kind: u8, name: String) -> Result<(), DbError> {
        if kind == b'S' {
            let st = self.stmts.get(&name).cloned().ok_or_else(|| {
                DbError::new(
                    "",
                    "26000",
                    format!("prepared statement \"{name}\" does not exist"),
                )
            })?;
            let d = self.describe_stmt(&st).await?;
            let mut types = d.param_types.clone();
            for (i, t) in st.param_types.iter().enumerate() {
                if *t != 0 && i < types.len() {
                    types[i] = *t;
                }
            }
            encode(&mut self.out, &BackendMessage::ParameterDescription(&types));
            match &d.columns {
                Some(c) => encode(&mut self.out, &BackendMessage::RowDescription(c, &[])),
                None => encode(&mut self.out, &BackendMessage::NoData),
            }
            if let Some(s) = self.stmts.get_mut(&name) {
                s.describe = Some(d);
            }
            return Ok(());
        }
        let (cls, formats) = {
            let p = self.portals.get(&name).ok_or_else(|| {
                DbError::new("", "34000", format!("portal \"{name}\" does not exist"))
            })?;
            (p.stmt.cls.clone(), p.result_formats.clone())
        };
        match cls.kind {
            StmtKind::Read | StmtKind::Show(_) => {
                // 읽기는 Describe 시점에 실행해 실제 결과 타입으로 기술한다.
                let (params, ptypes) = {
                    let p = &self.portals[&name];
                    (p.params.clone(), p.stmt.param_types.clone())
                };
                let r = self.run(&cls, params, &ptypes).await?;
                let cols = r.columns.clone();
                match &cols {
                    Some(c) => encode(&mut self.out, &BackendMessage::RowDescription(c, &formats)),
                    None => encode(&mut self.out, &BackendMessage::NoData),
                }
                let p = self.portals.get_mut(&name).unwrap();
                p.columns = cols;
                p.result = Some(r);
            }
            StmtKind::Write => {
                let st = self.portals[&name].stmt.clone();
                let d = self.describe_stmt(&st).await?;
                match &d.columns {
                    Some(c) => encode(&mut self.out, &BackendMessage::RowDescription(c, &formats)),
                    None => encode(&mut self.out, &BackendMessage::NoData),
                }
                self.portals.get_mut(&name).unwrap().columns = d.columns;
            }
            _ => encode(&mut self.out, &BackendMessage::NoData),
        }
        Ok(())
    }

    async fn execute_portal(&mut self, name: String, max_rows: i32) -> Result<(), DbError> {
        let has = self.portals.contains_key(&name);
        if !has {
            return Err(DbError::new(
                "",
                "34000",
                format!("portal \"{name}\" does not exist"),
            ));
        }
        if self.portals[&name].done {
            encode(&mut self.out, &BackendMessage::CommandComplete("SELECT 0"));
            return Ok(());
        }
        if self.portals[&name].result.is_none() {
            let (cls, params, ptypes) = {
                let p = &self.portals[&name];
                (
                    p.stmt.cls.clone(),
                    p.params.clone(),
                    p.stmt.param_types.clone(),
                )
            };
            if cls.kind == StmtKind::Empty {
                encode(&mut self.out, &BackendMessage::EmptyQuery);
                return Ok(());
            }
            let r = self.run(&cls, params, &ptypes).await?;
            self.portals.get_mut(&name).unwrap().result = Some(r);
        }
        let mut p = self.portals.remove(&name).unwrap();
        let r = p.result.as_ref().unwrap();
        for n in &r.notices {
            if p.sent == 0 {
                encode(&mut self.out, &BackendMessage::Notice(n));
            }
        }
        let res = (|| -> Result<bool, DbError> {
            if let Some(rc) = &r.columns {
                let cols: Vec<ColumnDesc> = p.columns.clone().unwrap_or_else(|| rc.clone());
                let total = r.rows.len();
                let end = if max_rows > 0 {
                    (p.sent + max_rows as usize).min(total)
                } else {
                    total
                };
                for i in p.sent..end {
                    self.encode_row(&r.rows[i], &cols, &p.result_formats)?;
                }
                let n = end - p.sent;
                p.sent = end;
                if end < total {
                    encode(&mut self.out, &BackendMessage::PortalSuspended);
                    return Ok(false);
                }
                let tag = if r.tag.starts_with("SELECT") {
                    format!("SELECT {n}")
                } else {
                    r.tag.clone()
                };
                encode(&mut self.out, &BackendMessage::CommandComplete(&tag));
            } else {
                encode(&mut self.out, &BackendMessage::CommandComplete(&r.tag));
            }
            Ok(true)
        })();
        let finished = res?;
        if finished {
            p.done = true;
            p.result = None;
            if !name.is_empty() {
                self.portals.insert(name, p);
            }
        } else {
            self.portals.insert(name, p);
        }
        self.maybe_flush().await
    }
}

/// `$n` 자리표시자 개수만큼 파라미터 타입 (지정되지 않은 것은 TEXT).
fn placeholder_types(sql: &str, given: &[u32]) -> Vec<u32> {
    let mut max = 0usize;
    let b = sql.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'$' {
            let mut j = i + 1;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 {
                if let Ok(n) = sql[i + 1..j].parse::<usize>() {
                    max = max.max(n);
                }
            }
            i = j;
        } else {
            i += 1;
        }
    }
    (0..max)
        .map(|k| given.get(k).copied().filter(|t| *t != 0).unwrap_or(oid::TEXT))
        .collect()
}

fn parse_bool_setting(name: &str, v: &str) -> Result<bool, DbError> {
    match v.trim().to_ascii_lowercase().as_str() {
        "on" | "true" | "yes" | "1" => Ok(true),
        "off" | "false" | "no" | "0" => Ok(false),
        _ => Err(DbError::new(
            "",
            "22023",
            format!("parameter \"{name}\" requires a Boolean value"),
        )),
    }
}

#[cfg(test)]
mod tx_mode_tests {
    use super::*;

    #[test]
    fn parses_modes() {
        let m = parse_tx_modes("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY").unwrap();
        assert_eq!(m.iso, Some(Iso::RepeatableRead));
        assert_eq!(m.read_only, Some(true));
        let m = parse_tx_modes("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE").unwrap();
        assert_eq!(m.iso, Some(Iso::Serializable));
        assert_eq!(m.read_only, None);
        let m = parse_tx_modes("start transaction isolation level read committed read write").unwrap();
        assert_eq!(m.iso, Some(Iso::ReadCommitted));
        assert_eq!(m.read_only, Some(false));
        assert!(parse_tx_modes("BEGIN ISOLATION LEVEL SNAPSHOT").is_err());
        assert_eq!(placeholder_types("insert into t values ($1, $3)", &[23]), vec![23, 25, 25]);
    }
}
