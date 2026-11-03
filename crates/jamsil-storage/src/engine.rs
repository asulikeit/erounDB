//! 비동기 Storage Engine 파사드: StoreRequest 처리.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use rusqlite::limits::Limit;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use tokio::sync::{oneshot, watch};

use jamsil_common::{DbError, DbResult, Id128, QueryResult, Value};
use jamsil_ipc::msg::{
    ChangeEvent, ChangeOp, DescribeResult, OutboxBatch, RequestOutcome, ScanBatch,
    SearchConsistency, SearchHit, SearchIndexDef, SnapshotInfo, Statement, StoreRequest,
    StoreResponse, StoreResult, StoreStatus,
};
use jamsil_ipc::rpc::BoxFut;
use jamsil_sql::{StmtKind, TextMatchQuery, Translator};

use crate::catalog::{
    init_system_tables, local_get, read_state, CatalogCache, OP_DELETE, OP_INDEX_CREATED,
    OP_INDEX_DROPPED,
};
use crate::convert::{map_err, pk_from_json, to_sql, value_from_ref};
use crate::exec::{describe, exec, to_result};
use crate::functions::{self, FunctionEnv};
use crate::pgcatalog;
use crate::reader::ReaderPool;
use crate::writer::{self, WriterConfig, WriterMsg};

/// 검색 프로세스 호출 (jamsil-store 바이너리가 UDS 클라이언트로 구현).
pub trait SearchProvider: Send + Sync {
    fn search(
        &self,
        index_name: String,
        field: Option<String>,
        query: String,
        limit: usize,
        min_visible: u64,
        timeout_ms: u64,
    ) -> BoxFut<DbResult<Vec<SearchHit>>>;
}

#[derive(Debug, Clone)]
pub struct EngineOptions {
    pub db_path: PathBuf,
    pub cluster_id: String,
    pub database: String,
    pub default_user: String,
    pub synchronous: String,
    pub reader_pool_size: usize,
    pub sqlite_max_length: usize,
    pub text_max_bytes: usize,
    pub max_mutation_bytes: usize,
    pub write_lock_timeout: Duration,
    pub idle_tx_timeout: Duration,
    pub search_max_candidates: usize,
    pub search_committed_timeout_ms: u64,
    pub integrity_check: bool,
    /// 사용자별 권한 (비어 있으면 권한 검사 없음 = 모든 사용자 superuser)
    pub principals: std::collections::HashMap<String, jamsil_common::acl::Principal>,
}

#[derive(Default)]
pub struct Metrics {
    pub text_rejected: AtomicU64,
    pub queries: AtomicU64,
    pub writes: AtomicU64,
    /// Writer 소유를 기다리는 요청 수
    pub writer_queue: AtomicU64,
}

pub struct Shared {
    pub opts: EngineOptions,
    applied: watch::Sender<(u64, u64)>,
    catalog: RwLock<Arc<CatalogCache>>,
    quarantine: Mutex<Option<String>>,
    writer_owner: Mutex<Option<Id128>>,
    outbox_notify: watch::Sender<u64>,
    outbox_ack: Mutex<Option<u64>>,
    /// Reader 세대. Snapshot 설치 시 쓰기 잠금을 잡고 증가시킨다.
    pub gate: RwLock<u64>,
    pub metrics: Metrics,
    fenv: FunctionEnv,
}

impl Shared {
    pub fn catalog(&self) -> Arc<CatalogCache> {
        self.catalog.read().clone()
    }
    pub fn set_catalog(&self, c: CatalogCache) {
        *self.catalog.write() = Arc::new(c);
    }
    /// 문장 실행 사용자의 권한. None = 검사 생략 (내부 호출 또는 사용자 목록 없음).
    pub fn principal(&self, user: &str) -> Option<jamsil_common::acl::Principal> {
        if user.is_empty() || self.opts.principals.is_empty() {
            return None;
        }
        Some(self.opts.principals.get(user).cloned().unwrap_or(jamsil_common::acl::Principal {
            // 설정에 없는 사용자(trust 인증 등): 메타데이터만 읽을 수 있다.
            name: user.to_string(),
            role: jamsil_common::acl::Role::ReadOnly,
            tables: Some(vec![]),
        }))
    }

    pub fn acl_ctx(&self, user: &str, catalog: &CatalogCache) -> crate::functions::AclCtx {
        crate::functions::AclCtx {
            user: user.to_string(),
            principal: self.principal(user),
            tables: std::sync::Arc::new(catalog.tables.keys().cloned().collect()),
        }
    }

    pub fn quarantined(&self) -> Option<String> {
        self.quarantine.lock().clone()
    }
    pub fn quarantine(&self, reason: String) {
        let mut q = self.quarantine.lock();
        if q.is_none() {
            tracing::error!("store quarantined: {reason}");
            *q = Some(reason);
        }
    }
    pub fn set_writer_owner(&self, t: Option<Id128>) {
        *self.writer_owner.lock() = t;
    }
    pub fn on_applied(&self, index: u64, term: u64) {
        self.applied.send_replace((index, term));
    }
    pub fn applied_index(&self) -> u64 {
        self.applied.borrow().0
    }
    pub fn outbox_bumped(&self) {
        self.outbox_notify.send_modify(|v| *v += 1);
    }
    pub fn outbox_ack_request(&self, upto: u64) {
        let mut a = self.outbox_ack.lock();
        *a = Some(a.unwrap_or(0).max(upto));
    }
    pub fn take_outbox_ack(&self) -> Option<u64> {
        self.outbox_ack.lock().take()
    }

    /// Snapshot 파일로 RDB를 교체한다 (Writer 연결이 닫힌 상태에서 호출).
    pub fn install_snapshot_files(
        &self,
        db_path: &Path,
        snap: &Path,
        index: u64,
        term: u64,
    ) -> DbResult<()> {
        let mut gate = self.gate.write();
        let dir = db_path.parent().unwrap();
        let tmp = dir.join("main.sqlite.installing");
        let _ = std::fs::remove_file(&tmp);
        std::fs::copy(snap, &tmp)?;
        std::fs::File::open(&tmp)?.sync_all()?;
        {
            let c = Connection::open(&tmp).map_err(map_err)?;
            let st = read_state(&c)?;
            if st.applied_index != index || st.applied_term != term {
                return Err(DbError::internal(format!(
                    "snapshot metadata mismatch: file has {}/{}, expected {index}/{term}",
                    st.applied_index, st.applied_term
                )));
            }
            init_system_tables(&c, &self.opts.cluster_id)?;
            c.execute_batch("DELETE FROM _jamsil_outbox; PRAGMA journal_mode=DELETE;")
                .map_err(map_err)?;
            writer::bump_search_epoch(&c)?;
            // 이전 세대보다 큰 epoch 보장
            let prev = Connection::open(db_path)
                .ok()
                .and_then(|p| local_get(&p, "search_rebuild_epoch").ok().flatten())
                .unwrap_or(0);
            let cur = local_get(&c, "search_rebuild_epoch")?.unwrap_or(0);
            if cur <= prev {
                crate::catalog::local_set(&c, "search_rebuild_epoch", prev + 1)?;
            }
        }
        for suffix in ["-wal", "-shm"] {
            let p = PathBuf::from(format!("{}{}", db_path.display(), suffix));
            let _ = std::fs::remove_file(p);
        }
        std::fs::rename(&tmp, db_path)?;
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
        let c = open_connection(self, db_path, true)?;
        let st = read_state(&c)?;
        self.set_catalog(CatalogCache::load(&c)?);
        drop(c);
        *gate += 1;
        drop(gate);
        *self.quarantine.lock() = None;
        self.on_applied(st.applied_index, st.applied_term);
        self.outbox_bumped();
        tracing::info!("installed snapshot at index {index} term {term}");
        Ok(())
    }
}

pub fn open_connection(shared: &Shared, path: &Path, writer: bool) -> DbResult<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_URI;
    let conn = Connection::open_with_flags(path, flags).map_err(map_err)?;
    conn.busy_timeout(Duration::from_secs(10))
        .map_err(map_err)?;
    let sync = match shared.opts.synchronous.to_ascii_uppercase().as_str() {
        "EXTRA" => "EXTRA",
        "NORMAL" => "NORMAL",
        "OFF" => "OFF",
        _ => "FULL",
    };
    conn.execute_batch(&format!(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous={sync}; PRAGMA foreign_keys=ON; \
         PRAGMA case_sensitive_like=ON; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-65536;"
    ))
    .map_err(map_err)?;
    conn.set_limit(
        Limit::SQLITE_LIMIT_LENGTH,
        shared.opts.sqlite_max_length as i32,
    );
    let got = conn.limit(Limit::SQLITE_LIMIT_LENGTH) as usize;
    if got < shared.opts.sqlite_max_length {
        return Err(DbError::internal(format!(
            "SQLite build limit too small: SQLITE_MAX_LENGTH {got} < {}",
            shared.opts.sqlite_max_length
        )));
    }
    functions::register(&conn, &shared.fenv).map_err(map_err)?;
    if !writer {
        pgcatalog::install(&conn, &shared.opts.database, &shared.opts.default_user)?;
    }
    Ok(conn)
}

pub struct Engine {
    shared: Arc<Shared>,
    writer_tx: Mutex<Sender<WriterMsg>>,
    readers: ReaderPool,
    pinned: crate::pinned::PinnedSnapshots,
    search: RwLock<Option<Arc<dyn SearchProvider>>>,
    query_seq: AtomicU64,
    /// 세션별 진행 중 읽기 (취소용).
    running: Arc<Mutex<std::collections::HashMap<Id128, (rusqlite::InterruptHandle, Arc<std::sync::atomic::AtomicBool>)>>>,
}

impl Engine {
    /// 로컬 영속성 복구 후 엔진을 연다 (설계서 7.4 Cold Start 1~2, 6단계).
    pub fn open(opts: EngineOptions) -> DbResult<Arc<Engine>> {
        if let Some(dir) = opts.db_path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let (applied_tx, _) = watch::channel((0u64, 0u64));
        let (outbox_tx, _) = watch::channel(0u64);
        let shared = Arc::new(Shared {
            fenv: FunctionEnv {
                database: opts.database.clone(),
                user: opts.default_user.clone(),
                start_time: functions::now_timestamptz(),
            },
            opts: opts.clone(),
            applied: applied_tx,
            catalog: RwLock::new(Arc::new(CatalogCache::default())),
            quarantine: Mutex::new(None),
            writer_owner: Mutex::new(None),
            outbox_notify: outbox_tx,
            outbox_ack: Mutex::new(None),
            gate: RwLock::new(0),
            metrics: Metrics::default(),
        });
        {
            let conn = open_connection(&shared, &opts.db_path, true)?;
            init_system_tables(&conn, &opts.cluster_id)?;
            if opts.integrity_check {
                let r: String = conn
                    .query_row("PRAGMA quick_check", [], |r| r.get(0))
                    .map_err(map_err)?;
                if r != "ok" {
                    return Err(DbError::store_recovery_required(format!(
                        "SQLite quick_check failed: {r}"
                    ))
                    .fatal());
                }
            }
            let st = read_state(&conn)?;
            shared.on_applied(st.applied_index, st.applied_term);
            shared.set_catalog(CatalogCache::load(&conn)?);
            tracing::info!(
                "store recovered: applied_index={} applied_term={} schema_version={}",
                st.applied_index,
                st.applied_term,
                st.schema_version
            );
        }
        let (wtx, wrx) = std::sync::mpsc::channel();
        let wcfg = WriterConfig {
            db_path: opts.db_path.clone(),
            text_max_bytes: opts.text_max_bytes,
            max_mutation_bytes: opts.max_mutation_bytes,
            idle_tx_timeout: opts.idle_tx_timeout,
        };
        let ws = shared.clone();
        std::thread::Builder::new()
            .name("jamsil-writer".into())
            .spawn(move || writer::run(ws, wcfg, wrx))
            .map_err(|e| DbError::internal(e.to_string()))?;
        let readers =
            ReaderPool::start(shared.clone(), opts.db_path.clone(), opts.reader_pool_size);
        // 지표는 관측 전에도 0으로 보이도록 미리 등록
        jamsil_common::metrics::histogram("jamsil_storage_fsync_duration_seconds", "", "seconds");
        jamsil_common::metrics::histogram("jamsil_mutation_payload_bytes", "", "bytes");
        let pinned = crate::pinned::PinnedSnapshots::new(
            shared.clone(),
            opts.db_path.clone(),
            opts.idle_tx_timeout,
        );
        Ok(Arc::new(Engine {
            shared,
            writer_tx: Mutex::new(wtx),
            readers,
            pinned,
            search: RwLock::new(None),
            query_seq: AtomicU64::new(1),
            running: Arc::new(Mutex::new(Default::default())),
        }))
    }

    pub fn set_search_provider(&self, p: Arc<dyn SearchProvider>) {
        *self.search.write() = Some(p);
    }

    pub fn applied_index(&self) -> u64 {
        self.shared.applied_index()
    }

    pub fn subscribe_outbox(&self) -> watch::Receiver<u64> {
        self.shared.outbox_notify.subscribe()
    }

    async fn writer<T>(
        &self,
        f: impl FnOnce(oneshot::Sender<DbResult<T>>) -> WriterMsg,
    ) -> DbResult<T> {
        let (tx, rx) = oneshot::channel();
        self.writer_tx
            .lock()
            .send(f(tx))
            .map_err(|_| DbError::unavailable("store writer stopped"))?;
        rx.await
            .map_err(|_| DbError::internal("store writer dropped request"))?
    }

    pub fn shutdown(&self) {
        let _ = self.writer_tx.lock().send(WriterMsg::Shutdown);
    }

    pub async fn handle(self: &Arc<Self>, req: StoreRequest) -> StoreResult {
        match req {
            StoreRequest::Hello { protocol_version } => {
                if protocol_version != jamsil_ipc::msg::IPC_PROTOCOL_VERSION {
                    return Err(DbError::protocol("IPC protocol version mismatch"));
                }
                Ok(StoreResponse::Hello {
                    protocol_version: jamsil_ipc::msg::IPC_PROTOCOL_VERSION,
                    node_ready: self.shared.quarantined().is_none(),
                })
            }
            StoreRequest::Status => Ok(StoreResponse::Status(self.status().await?)),
            StoreRequest::Query {
                session,
                stmt,
                min_applied,
                wait_timeout_ms,
                search_consistency,
                virtual_tables,
                snapshot,
            } => {
                self.shared.metrics.queries.fetch_add(1, Ordering::Relaxed);
                let r = self
                    .query(
                        session,
                        stmt,
                        min_applied,
                        wait_timeout_ms,
                        search_consistency,
                        virtual_tables,
                        snapshot,
                    )
                    .await?;
                Ok(StoreResponse::Result(r))
            }
            StoreRequest::SnapshotOpen {
                snapshot,
                min_applied,
                wait_timeout_ms,
            } => {
                if let Some(q) = self.shared.quarantined() {
                    return Err(DbError::store_recovery_required(q));
                }
                if min_applied > 0 {
                    self.wait_applied(min_applied, Duration::from_millis(wait_timeout_ms.max(1)))
                        .await?;
                }
                let index = self.pinned.open(snapshot).await?;
                Ok(StoreResponse::Applied { index })
            }
            StoreRequest::SnapshotClose { snapshot } => {
                self.pinned.close(snapshot);
                Ok(StoreResponse::Ok)
            }
            StoreRequest::Describe {
                stmt,
                min_applied,
                wait_timeout_ms,
            } => {
                if min_applied > 0 {
                    self.wait_applied(min_applied, Duration::from_millis(wait_timeout_ms.max(1)))
                        .await?;
                }
                Ok(StoreResponse::Describe(self.describe(stmt).await?))
            }
            StoreRequest::TxExec {
                txn,
                stmt,
                lock_timeout_ms,
                request_id,
                expect_applied,
            } => {
                self.shared.metrics.writes.fetch_add(1, Ordering::Relaxed);
                let t = if lock_timeout_ms == 0 {
                    self.shared.opts.write_lock_timeout
                } else {
                    Duration::from_millis(lock_timeout_ms)
                };
                let deadline = Instant::now() + t;
                let r = self
                    .writer(|reply| WriterMsg::Exec {
                        txn,
                        stmt,
                        request_id,
                        expect_applied,
                        deadline,
                        reply,
                    })
                    .await?;
                Ok(StoreResponse::Result(r))
            }
            StoreRequest::TxPrepare { txn, request_id } => {
                let r = self
                    .writer(|reply| WriterMsg::Prepare {
                        txn,
                        request_id,
                        reply,
                    })
                    .await?;
                Ok(StoreResponse::Prepared(r))
            }
            StoreRequest::TxAbort { txn } => {
                self.writer(|reply| WriterMsg::Abort { txn, reply }).await?;
                Ok(StoreResponse::Ok)
            }
            StoreRequest::AbortAll => {
                self.writer(|reply| WriterMsg::AbortAll { reply }).await?;
                Ok(StoreResponse::Ok)
            }
            StoreRequest::Apply { index, term, cmd } => {
                let t0 = Instant::now();
                let i = self
                    .writer(|reply| WriterMsg::Apply {
                        index,
                        term,
                        cmd,
                        reply,
                    })
                    .await?;
                if t0.elapsed() > Duration::from_secs(1) {
                    tracing::warn!("slow store apply of log index {index}: {} ms", t0.elapsed().as_millis());
                }
                Ok(StoreResponse::Applied { index: i })
            }
            StoreRequest::CreateSnapshot { path } => {
                Ok(StoreResponse::Snapshot(self.create_snapshot(path).await?))
            }
            StoreRequest::InstallSnapshot { path, index, term } => {
                let p = PathBuf::from(path);
                self.writer(|reply| WriterMsg::InstallSnapshot {
                    path: p,
                    index,
                    term,
                    reply,
                })
                .await?;
                Ok(StoreResponse::Ok)
            }
            StoreRequest::OutboxRead {
                after_seq,
                max_events,
                max_bytes,
            } => Ok(StoreResponse::Outbox(
                self.outbox_read(after_seq, max_events, max_bytes).await?,
            )),
            StoreRequest::OutboxAck { upto_seq } => {
                self.writer(|reply| WriterMsg::OutboxAck {
                    upto: upto_seq,
                    reply,
                })
                .await?;
                Ok(StoreResponse::Ok)
            }
            StoreRequest::ScanRows {
                index_name,
                after_pk,
                limit,
                max_bytes,
            } => Ok(StoreResponse::Rows(
                self.scan_rows(index_name, after_pk, limit, max_bytes)
                    .await?,
            )),
            StoreRequest::SearchIndexes => Ok(StoreResponse::SearchIndexes(
                self.shared.catalog().search.clone(),
            )),
            StoreRequest::RequestStatus { request_id } => {
                let r = self
                    .readers
                    .run(move |c| {
                        c.query_row(
                            "SELECT raft_index, tag FROM _jamsil_requests WHERE request_id = ?1",
                            [&request_id],
                            |r| {
                                Ok(RequestOutcome {
                                    raft_index: r.get::<_, i64>(0)? as u64,
                                    tag: r.get(1)?,
                                })
                            },
                        )
                        .optional()
                        .map_err(map_err)
                    })
                    .await?;
                Ok(StoreResponse::RequestStatus(r))
            }
            StoreRequest::CancelQuery { session } => {
                if let Some((h, flag)) = self.running.lock().get(&session) {
                    flag.store(true, Ordering::SeqCst);
                    h.interrupt();
                }
                Ok(StoreResponse::Ok)
            }
            StoreRequest::Checksum => {
                let (applied_index, checksum) = self.readers.run(checksum).await?;
                Ok(StoreResponse::Checksum {
                    applied_index,
                    checksum,
                })
            }
        }
    }

    async fn status(&self) -> DbResult<StoreStatus> {
        let (idx, term) = *self.shared.applied.borrow();
        let db = self.shared.opts.db_path.clone();
        let db_bytes = std::fs::metadata(&db).map(|m| m.len()).unwrap_or(0);
        let wal_bytes = std::fs::metadata(format!("{}-wal", db.display()))
            .map(|m| m.len())
            .unwrap_or(0);
        let (schema_version, outbox_seq, epoch) = self
            .readers
            .run(|c| {
                let st = read_state(c)?;
                let seq: i64 = c
                    .query_row(
                        "SELECT coalesce(max(seq), 0) FROM _jamsil_outbox",
                        [],
                        |r| r.get(0),
                    )
                    .map_err(map_err)?;
                let ep = local_get(c, "search_rebuild_epoch")?.unwrap_or(0);
                Ok((st.schema_version, seq as u64, ep as u64))
            })
            .await
            .unwrap_or((0, 0, 0));
        let owner = *self.shared.writer_owner.lock();
        Ok(StoreStatus {
            applied_index: idx,
            applied_term: term,
            schema_version,
            ready: self.shared.quarantined().is_none(),
            quarantined: self.shared.quarantined(),
            db_bytes,
            wal_bytes,
            text_bytes_estimated: db_bytes,
            outbox_seq,
            search_rebuild_epoch: epoch,
            writer_owner: owner.map(jamsil_common::id_to_string),
            open_transactions: owner.is_some() as usize,
            text_rejected_total: self.shared.metrics.text_rejected.load(Ordering::Relaxed),
            queries_total: self.shared.metrics.queries.load(Ordering::Relaxed),
            writes_total: self.shared.metrics.writes.load(Ordering::Relaxed),
            metrics: {
                use jamsil_common::metrics::MetricSample as M;
                let mut m = jamsil_common::metrics::process_samples();
                m.extend(jamsil_ipc::rpc::ipc_samples());
                m.push(M::new("jamsil_storage_writer_queue_depth", "", self.shared.metrics.writer_queue.load(Ordering::Relaxed) as f64, "count"));
                m.push(M::new("jamsil_storage_open_snapshots", "", self.pinned.len() as f64, "count"));
                if let Some(free) = db.parent().and_then(jamsil_common::metrics::disk_free_bytes) {
                    m.push(M::new("jamsil_storage_disk_free_bytes", "", free as f64, "bytes"));
                }
                m
            },
        })
    }

    /// applied_index가 `min`에 도달할 때까지 대기.
    pub async fn wait_applied(&self, min: u64, timeout: Duration) -> DbResult<u64> {
        let mut rx = self.shared.applied.subscribe();
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let cur = rx.borrow_and_update().0;
            if cur >= min {
                return Ok(cur);
            }
            if let Some(q) = self.shared.quarantined() {
                return Err(DbError::store_recovery_required(q));
            }
            match tokio::time::timeout_at(deadline, rx.changed()).await {
                Ok(Ok(())) => continue,
                Ok(Err(_)) => return Err(DbError::internal("applied watch closed")),
                Err(_) => {
                    return Err(DbError::timeout(format!(
                        "timed out waiting for local apply of index {min} (applied {cur})"
                    )))
                }
            }
        }
    }

    async fn query(
        &self,
        session: Id128,
        stmt: Statement,
        min_applied: u64,
        wait_timeout_ms: u64,
        consistency: SearchConsistency,
        virtual_tables: Vec<jamsil_ipc::msg::VirtualTable>,
        snapshot: Option<Id128>,
    ) -> DbResult<QueryResult> {
        if let Some(q) = self.shared.quarantined() {
            return Err(DbError::store_recovery_required(q));
        }
        if min_applied > 0 {
            self.wait_applied(min_applied, Duration::from_millis(wait_timeout_ms.max(1)))
                .await?;
        }
        let catalog = self.shared.catalog();
        let qn = self.query_seq.fetch_add(1, Ordering::Relaxed);
        let prefix = format!("_jm_q{qn}");
        let acl = self.shared.acl_ctx(&stmt.user, catalog.as_ref());
        let tr = Translator::new(catalog.as_ref(), prefix)
            .with_principal(acl.principal.as_ref())
            .translate(&stmt.sql)?;
        if tr.kind != StmtKind::Read {
            return Err(DbError::internal("non-read statement routed to reader"));
        }
        if snapshot.is_some() && !tr.text_matches.is_empty() {
            // 검색 인덱스는 현재 상태뿐이라 고정 Snapshot 행과 결합하면 조건이 거짓인 행이 나올 수 있다 (R2-03).
            // SQL 프로세스가 먼저 거부하며, 이것은 다른 경로로 들어온 요청을 위한 방어선이다.
            return Err(DbError::search_in_snapshot_txn("repeatable read or serializable"));
        }
        let consistency = match tr.search_committed {
            Some(true) => SearchConsistency::Committed,
            Some(false) => SearchConsistency::Eventual,
            None => consistency,
        };
        // TEXT_MATCH 후보 조회
        let mut fills: Vec<(String, usize, Vec<SearchHit>)> = vec![];
        if !tr.text_matches.is_empty() {
            let provider = self
                .search
                .read()
                .clone()
                .ok_or_else(|| DbError::unavailable("search process is not available"))?;
            let min_visible = match consistency {
                SearchConsistency::Committed => self.applied_index(),
                SearchConsistency::Eventual => 0,
            };
            for m in &tr.text_matches {
                let q = match &m.query {
                    TextMatchQuery::Literal(s) => s.clone(),
                    TextMatchQuery::Param(n) => match stmt.params.get(n - 1) {
                        Some(Value::Text(s)) => s.clone(),
                        Some(Value::Null) | None => {
                            return Err(DbError::invalid_param(
                                "TEXT_MATCH query parameter is NULL",
                            ))
                        }
                        Some(other) => format!("{other:?}"),
                    },
                };
                let hits = provider
                    .search(
                        m.index.name.clone(),
                        Some(m.column.clone()),
                        q,
                        self.shared.opts.search_max_candidates,
                        min_visible,
                        self.shared.opts.search_committed_timeout_ms,
                    )
                    .await?;
                fills.push((m.temp_table.clone(), m.index.pk_columns.len(), hits));
            }
        }
        let hints = tr.column_type_hints.clone();
        let sql = tr.statements[0].clone();
        let tag = tr.tag.clone();
        let notices = tr.notices.clone();
        let params = stmt.params;
        let running = self.running.clone();
        let job = move |c: &rusqlite::Connection| {
                let _acl = crate::functions::AclScope::enter(acl);
                let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
                running.lock().insert(session, (c.get_interrupt_handle(), flag.clone()));
                crate::functions::CANCEL_FLAG.with(|f| *f.borrow_mut() = Some(flag));
                let _guard = scopeguard(move || {
                    running.lock().remove(&session);
                    crate::functions::CANCEL_FLAG.with(|f| *f.borrow_mut() = None);
                });
                // 고정 Snapshot이면 이미 트랜잭션 안이다.
                let own_txn = c.is_autocommit();
                if own_txn {
                    c.execute_batch("BEGIN").map_err(map_err)?;
                }
                let r = (|| {
                    for (t, npk, hits) in &fills {
                        create_match_table(c, t, *npk, hits)?;
                    }
                    for vt in &virtual_tables {
                        create_virtual_table(c, vt)?;
                    }
                    let out = exec(c, &sql, &params, &hints)?;
                    let mut res = to_result(&tag, out);
                    res.notices = notices;
                    Ok(res)
                })();
                for (t, _, _) in &fills {
                    let _ = c.execute_batch(&format!("DROP TABLE IF EXISTS temp.\"{t}\""));
                }
                for vt in &virtual_tables {
                    let _ = c.execute_batch(&format!("DROP TABLE IF EXISTS temp.\"{}\"", vt.name));
                }
                if own_txn {
                    let _ = c.execute_batch("COMMIT");
                }
                r
        };
        match snapshot {
            Some(id) => self.pinned.run(id, job).await,
            None => self.readers.run(job).await,
        }
    }

    async fn describe(&self, stmt: Statement) -> DbResult<DescribeResult> {
        let catalog = self.shared.catalog();
        let acl = self.shared.acl_ctx(&stmt.user, catalog.as_ref());
        let tr = Translator::new(catalog.as_ref(), "_jm_d")
            .with_principal(acl.principal.as_ref())
            .translate(&stmt.sql)?;
        let ptypes = stmt.param_types.clone();
        if tr.statements.is_empty() || tr.kind == StmtKind::Ddl {
            return Ok(DescribeResult {
                param_types: ptypes,
                columns: None,
            });
        }
        let sql = tr.statements[0].clone();
        let hints = tr.column_type_hints.clone();
        let has_match = !tr.text_matches.is_empty();
        let fills: Vec<(String, usize)> = tr
            .text_matches
            .iter()
            .map(|m| (m.temp_table.clone(), m.index.pk_columns.len()))
            .collect();
        self.readers
            .run(move |c| {
                for (t, n) in &fills {
                    create_match_table(c, t, *n, &[])?;
                }
                let r = describe(c, &sql, &hints);
                if has_match {
                    for (t, _) in &fills {
                        let _ = c.execute_batch(&format!("DROP TABLE IF EXISTS temp.\"{t}\""));
                    }
                }
                let (n, cols) = r?;
                let mut pt = ptypes;
                if pt.len() < n {
                    pt.resize(n, 0);
                }
                for p in pt.iter_mut() {
                    if *p == 0 {
                        *p = jamsil_sql::types::oid::TEXT;
                    }
                }
                Ok(DescribeResult {
                    param_types: pt,
                    columns: cols,
                })
            })
            .await
    }

    async fn create_snapshot(&self, path: String) -> DbResult<SnapshotInfo> {
        self.readers
            .run(move |c| {
                let _ = std::fs::remove_file(&path);
                c.execute("VACUUM INTO ?1", [&path]).map_err(map_err)?;
                let f = std::fs::File::open(&path)?;
                f.sync_all()?;
                let size = f.metadata()?.len();
                let sc = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)
                    .map_err(map_err)?;
                let st = read_state(&sc)?;
                Ok(SnapshotInfo {
                    path,
                    index: st.applied_index,
                    term: st.applied_term,
                    size,
                })
            })
            .await
    }

    async fn outbox_read(
        &self,
        after: u64,
        max_events: usize,
        max_bytes: usize,
    ) -> DbResult<OutboxBatch> {
        self.readers
            .run(move |c| {
                c.execute_batch("BEGIN").map_err(map_err)?;
                let r = (|| {
                    let st = read_state(c)?;
                    let epoch = local_get(c, "search_rebuild_epoch")?.unwrap_or(0) as u64;
                    let cat = CatalogCache::load(c)?;
                    let mut st_ev = c
                        .prepare("SELECT seq, raft_index, index_name, op, pk FROM _jamsil_outbox WHERE seq > ?1 ORDER BY seq LIMIT ?2")
                        .map_err(map_err)?;
                    let rows: Vec<(i64, i64, String, i64, String)> = st_ev
                        .query_map(params![after as i64, (max_events + 1) as i64], |r| {
                            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
                        })
                        .map_err(map_err)?
                        .collect::<Result<_, _>>()
                        .map_err(map_err)?;
                    let mut drained = rows.len() <= max_events;
                    let mut events = vec![];
                    let mut bytes = 0usize;
                    for (seq, ridx, name, op, pk) in rows.into_iter().take(max_events) {
                        let pk = pk_from_json(&pk)?;
                        let op = match op {
                            OP_DELETE => ChangeOp::Delete,
                            OP_INDEX_CREATED => ChangeOp::IndexCreated,
                            OP_INDEX_DROPPED => ChangeOp::IndexDropped,
                            _ => ChangeOp::Upsert,
                        };
                        let texts = if op == ChangeOp::Upsert {
                            match cat.search_def(&name) {
                                Some(def) => fetch_texts(c, def, &pk)?,
                                None => None,
                            }
                        } else {
                            None
                        };
                        bytes += texts.as_ref().map(|t| t.iter().map(|x| x.as_ref().map(|s| s.len()).unwrap_or(0)).sum()).unwrap_or(0);
                        events.push(ChangeEvent { seq: seq as u64, raft_index: ridx as u64, index_name: name, op, pk, texts });
                        if bytes >= max_bytes {
                            drained = false;
                            break;
                        }
                    }
                    // AUTOINCREMENT 시퀀스: 정리(ack)된 이벤트까지 포함한 최대 seq
                    let max_seq: i64 = c
                        .query_row(
                            "SELECT max(coalesce((SELECT seq FROM sqlite_sequence WHERE name = '_jamsil_outbox'), 0), coalesce((SELECT max(seq) FROM _jamsil_outbox), 0))",
                            [],
                            |r| r.get(0),
                        )
                        .map_err(map_err)?;
                    let max_seq = max_seq as u64;
                    Ok(OutboxBatch {
                        events,
                        applied_index: st.applied_index,
                        drained,
                        rebuild_epoch: epoch,
                        indexes: cat.search,
                        max_seq,
                    })
                })();
                let _ = c.execute_batch("COMMIT");
                r
            })
            .await
    }

    async fn scan_rows(
        &self,
        index_name: String,
        after: Option<Vec<Value>>,
        limit: usize,
        max_bytes: usize,
    ) -> DbResult<ScanBatch> {
        self.readers
            .run(move |c| {
                c.execute_batch("BEGIN").map_err(map_err)?;
                let r = (|| {
                    let st = read_state(c)?;
                    let cat = CatalogCache::load(c)?;
                    let def = cat
                        .search_def(&index_name)
                        .ok_or_else(|| DbError::search_index_missing(index_name.clone()))?
                        .clone();
                    let q = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
                    let pkl: Vec<String> = def.pk_columns.iter().map(|x| q(x)).collect();
                    let cl: Vec<String> = def.columns.iter().map(|x| q(x)).collect();
                    let tuple = |v: &[String]| {
                        if v.len() == 1 {
                            v[0].clone()
                        } else {
                            format!("({})", v.join(", "))
                        }
                    };
                    let mut sql = format!(
                        "SELECT {}, {} FROM {}",
                        pkl.join(", "),
                        cl.join(", "),
                        q(&def.table)
                    );
                    let mut params: Vec<Value> = vec![];
                    if let Some(a) = &after {
                        let ph: Vec<String> = (1..=a.len()).map(|i| format!("?{i}")).collect();
                        sql.push_str(&format!(" WHERE {} > {}", tuple(&pkl), tuple(&ph)));
                        params = a.clone();
                    }
                    sql.push_str(&format!(" ORDER BY {} LIMIT {}", pkl.join(", "), limit + 1));
                    let mut s = c.prepare(&sql).map_err(map_err)?;
                    for (i, p) in params.iter().enumerate() {
                        s.raw_bind_parameter(i + 1, to_sql(p)).map_err(map_err)?;
                    }
                    let mut rows = s.raw_query();
                    let mut out = vec![];
                    let mut bytes = 0usize;
                    let mut done = true;
                    let npk = pkl.len();
                    while let Some(row) = rows.next().map_err(map_err)? {
                        if out.len() >= limit || bytes >= max_bytes {
                            done = false;
                            break;
                        }
                        let mut pk = vec![];
                        for i in 0..npk {
                            pk.push(value_from_ref(row.get_ref(i).map_err(map_err)?)?);
                        }
                        let mut texts = vec![];
                        for i in 0..cl.len() {
                            let v = value_from_ref(row.get_ref(npk + i).map_err(map_err)?)?;
                            texts.push(match v {
                                Value::Text(s) => {
                                    bytes += s.len();
                                    Some(s)
                                }
                                Value::Null => None,
                                Value::Int(i) => Some(i.to_string()),
                                Value::Real(f) => Some(f.to_string()),
                                Value::Blob(_) => None,
                            });
                        }
                        out.push((pk, texts));
                    }
                    Ok(ScanBatch {
                        rows: out,
                        applied_index: st.applied_index,
                        done,
                    })
                })();
                let _ = c.execute_batch("COMMIT");
                r
            })
            .await
    }
}

fn fetch_texts(
    c: &Connection,
    def: &SearchIndexDef,
    pk: &[Value],
) -> DbResult<Option<Vec<Option<String>>>> {
    if pk.len() != def.pk_columns.len() || def.pk_columns.is_empty() {
        return Ok(None);
    }
    let q = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let cols: Vec<String> = def.columns.iter().map(|x| q(x)).collect();
    let conds: Vec<String> = def
        .pk_columns
        .iter()
        .enumerate()
        .map(|(i, p)| format!("{} = ?{}", q(p), i + 1))
        .collect();
    let sql = format!(
        "SELECT {} FROM {} WHERE {}",
        cols.join(", "),
        q(&def.table),
        conds.join(" AND ")
    );
    let mut s = match c.prepare_cached(&sql) {
        Ok(s) => s,
        Err(_) => return Ok(None),
    };
    for (i, p) in pk.iter().enumerate() {
        s.raw_bind_parameter(i + 1, to_sql(p)).map_err(map_err)?;
    }
    let mut rows = s.raw_query();
    match rows.next().map_err(map_err)? {
        None => Ok(None),
        Some(row) => {
            let mut v = vec![];
            for i in 0..cols.len() {
                v.push(match value_from_ref(row.get_ref(i).map_err(map_err)?)? {
                    Value::Text(s) => Some(s),
                    Value::Int(i) => Some(i.to_string()),
                    Value::Real(f) => Some(f.to_string()),
                    _ => None,
                });
            }
            Ok(Some(v))
        }
    }
}

fn create_match_table(c: &Connection, name: &str, npk: usize, hits: &[SearchHit]) -> DbResult<()> {
    let cols: Vec<String> = (0..npk).map(|i| format!("c{i}")).collect();
    c.execute_batch(&format!(
        "DROP TABLE IF EXISTS temp.\"{name}\"; CREATE TEMP TABLE \"{name}\" ({}, score REAL, PRIMARY KEY ({}));",
        cols.join(", "),
        cols.join(", ")
    ))
    .map_err(map_err)?;
    if hits.is_empty() {
        return Ok(());
    }
    let ph: Vec<String> = (1..=npk + 1).map(|i| format!("?{i}")).collect();
    let mut st = c
        .prepare(&format!(
            "INSERT OR IGNORE INTO temp.\"{name}\" VALUES ({})",
            ph.join(", ")
        ))
        .map_err(map_err)?;
    for h in hits {
        if h.pk.len() != npk {
            continue;
        }
        for (i, v) in h.pk.iter().enumerate() {
            st.raw_bind_parameter(i + 1, to_sql(v)).map_err(map_err)?;
        }
        st.raw_bind_parameter(npk + 1, h.score as f64)
            .map_err(map_err)?;
        st.raw_execute().map_err(map_err)?;
    }
    Ok(())
}

fn create_virtual_table(c: &Connection, vt: &jamsil_ipc::msg::VirtualTable) -> DbResult<()> {
    let q = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));
    let cols: Vec<String> = vt
        .columns
        .iter()
        .map(|(n, oid)| {
            let decl = match *oid {
                16 => "BOOLEAN",
                20 => "BIGINT",
                23 => "INT",
                21 => "SMALLINT",
                701 => "DOUBLE PRECISION",
                1184 => "TIMESTAMPTZ",
                _ => "TEXT",
            };
            format!("{} {decl}", q(n))
        })
        .collect();
    c.execute_batch(&format!(
        "DROP TABLE IF EXISTS temp.{n}; CREATE TEMP TABLE {n} ({});",
        cols.join(", "),
        n = q(&vt.name)
    ))
    .map_err(map_err)?;
    if vt.rows.is_empty() {
        return Ok(());
    }
    let ph: Vec<String> = (1..=vt.columns.len()).map(|i| format!("?{i}")).collect();
    let mut st = c
        .prepare(&format!(
            "INSERT INTO temp.{} VALUES ({})",
            q(&vt.name),
            ph.join(", ")
        ))
        .map_err(map_err)?;
    for r in &vt.rows {
        for (i, v) in r.iter().enumerate() {
            st.raw_bind_parameter(i + 1, to_sql(v)).map_err(map_err)?;
        }
        st.raw_execute().map_err(map_err)?;
    }
    Ok(())
}

/// 복제본 데이터 Checksum: 사용자 테이블 + 복제 시스템 테이블, PK 순서.
fn checksum(c: &Connection) -> DbResult<(u64, String)> {
    c.execute_batch("BEGIN").map_err(map_err)?;
    let r = (|| {
        let st = read_state(c)?;
        let mut tables: Vec<String> = {
            let mut s = c
                .prepare("SELECT name FROM sqlite_schema WHERE type = 'table' AND substr(name,1,7) <> 'sqlite_' AND name NOT IN ('_jamsil_local', '_jamsil_outbox')")
                .map_err(map_err)?;
            let v = s
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(map_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_err)?;
            v
        };
        tables.sort();
        let mut h = crc32fast::Hasher::new();
        let mut rows_total = 0u64;
        for t in tables {
            let cols = crate::catalog::table_columns(c, &t)?;
            let pk: Vec<String> = cols
                .iter()
                .filter(|c| c.pk)
                .map(|c| format!("\"{}\"", c.name))
                .collect();
            let order = if pk.is_empty() {
                "rowid".to_string()
            } else {
                pk.join(", ")
            };
            h.update(t.as_bytes());
            let mut s = c
                .prepare(&format!("SELECT * FROM \"{t}\" ORDER BY {order}"))
                .map_err(map_err)?;
            let n = s.column_count();
            let mut rows = s.raw_query();
            while let Some(row) = rows.next().map_err(map_err)? {
                rows_total += 1;
                for i in 0..n {
                    match row.get_ref(i).map_err(map_err)? {
                        rusqlite::types::ValueRef::Null => h.update(&[0]),
                        rusqlite::types::ValueRef::Integer(v) => {
                            h.update(&[1]);
                            h.update(&v.to_le_bytes())
                        }
                        rusqlite::types::ValueRef::Real(v) => {
                            h.update(&[2]);
                            h.update(&v.to_bits().to_le_bytes())
                        }
                        rusqlite::types::ValueRef::Text(v) => {
                            h.update(&[3]);
                            h.update(&(v.len() as u64).to_le_bytes());
                            h.update(v)
                        }
                        rusqlite::types::ValueRef::Blob(v) => {
                            h.update(&[4]);
                            h.update(&(v.len() as u64).to_le_bytes());
                            h.update(v)
                        }
                    }
                }
            }
        }
        Ok((
            st.applied_index,
            format!("{:08x}-{rows_total}", h.finalize()),
        ))
    })();
    let _ = c.execute_batch("COMMIT");
    r
}

/// 범위를 벗어날 때 실행되는 정리 함수.
struct ScopeGuard<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> Drop for ScopeGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

fn scopeguard<F: FnOnce()>(f: F) -> ScopeGuard<F> {
    ScopeGuard(Some(f))
}
