//! 단일 Writer 스레드.
//!
//! 쓰기 트랜잭션 수명 (설계서 6.3):
//!
//! ```text
//! Exec*        BEGIN IMMEDIATE + Session attach, 문장 실행 (미COMMIT)
//! Prepare      Patchset/DDL 추출, 크기 검증 → TxnPayload (Raft 제안 대상)
//! Apply(N)     같은 tx_id면 원래 트랜잭션에 applied_index=N 기록 후 COMMIT,
//!              아니면 (Follower/Leader 교체) DDL + Patchset 적용 후 COMMIT
//! Abort        ROLLBACK
//! ```
//!
//! SQLite 선COMMIT 후 Raft 제안은 구조적으로 불가능하다: COMMIT은 Apply 경로에서만 일어난다.

use std::collections::{HashMap, VecDeque};
use std::io::Read;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rusqlite::session::{ChangesetIter, ConflictAction, ConflictType, Session};
use rusqlite::{params, Connection, OptionalExtension};
use tokio::sync::oneshot;

use jamsil_common::{
    DbError, DbResult, Id128, QueryResult, ReplicatedCommand, RequestRecord, TxnPayload, Value,
};
use jamsil_ipc::msg::Statement;
use jamsil_sql::search_ddl::SearchDdl;
use jamsil_sql::types::decl_is_text;
use jamsil_sql::{StmtKind, Translator};

use crate::catalog::{
    is_system_table, local_get, local_set, read_state, CatalogCache, OP_DELETE, OP_INDEX_CREATED,
    OP_INDEX_DROPPED, OP_UPSERT,
};
use crate::convert::{map_err, pk_to_json, value_from_ref};
use crate::engine::{open_connection, Shared};
use crate::exec::{exec, to_result};

pub enum WriterMsg {
    Exec {
        txn: Id128,
        stmt: Statement,
        request_id: Option<String>,
        expect_applied: Option<u64>,
        deadline: Instant,
        reply: oneshot::Sender<DbResult<QueryResult>>,
    },
    Prepare {
        txn: Id128,
        request_id: Option<String>,
        reply: oneshot::Sender<DbResult<Option<ReplicatedCommand>>>,
    },
    Abort {
        txn: Id128,
        reply: oneshot::Sender<DbResult<()>>,
    },
    AbortAll {
        reply: oneshot::Sender<DbResult<()>>,
    },
    Apply {
        index: u64,
        term: u64,
        cmd: ReplicatedCommand,
        reply: oneshot::Sender<DbResult<u64>>,
    },
    InstallSnapshot {
        path: PathBuf,
        index: u64,
        term: u64,
        reply: oneshot::Sender<DbResult<()>>,
    },
    OutboxAck {
        upto: u64,
        reply: oneshot::Sender<DbResult<()>>,
    },
    Shutdown,
}

impl WriterMsg {
    fn txn(&self) -> Option<Id128> {
        match self {
            WriterMsg::Exec { txn, .. } | WriterMsg::Prepare { txn, .. } => Some(*txn),
            _ => None,
        }
    }

    fn fail(self, e: DbError) {
        match self {
            WriterMsg::Exec { reply, .. } => {
                let _ = reply.send(Err(e));
            }
            WriterMsg::Prepare { reply, .. } => {
                let _ = reply.send(Err(e));
            }
            WriterMsg::Abort { reply, .. }
            | WriterMsg::AbortAll { reply }
            | WriterMsg::InstallSnapshot { reply, .. }
            | WriterMsg::OutboxAck { reply, .. } => {
                let _ = reply.send(Err(e));
            }
            WriterMsg::Apply { reply, .. } => {
                let _ = reply.send(Err(e));
            }
            WriterMsg::Shutdown => {}
        }
    }
}

struct Owner<'c> {
    txn: Id128,
    session: Session<'c>,
    ddl: Vec<String>,
    has_dml: bool,
    failed: Option<DbError>,
    last_activity: Instant,
    prepared: Option<TxnPayload>,
    /// 복제할 명령 (멱등 요청 이력 포함 여부)
    prepared_cmd: Option<ReplicatedCommand>,
    prepared_at_applied: u64,
    tag: String,
    /// 멱등 키 (request_id, 지문)
    request: Option<(String, String)>,
    /// 마지막 문장 결과 (autocommit 멱등 재생용)
    last_result: Option<QueryResult>,
    /// SAVEPOINT 스택: (이름, 당시 DDL 개수, 당시 DML 여부)
    savepoints: Vec<(String, usize, bool)>,
    /// 이 트랜잭션에서 실행한 문장들의 지문 누적 (명시적 트랜잭션 COMMIT 멱등 키 판정)
    txn_digest: sha2::Sha256,
    /// 이 트랜잭션 DDL 반영 전 검색 인덱스 정의 (Outbox 이벤트 계산용).
    search_before: Vec<jamsil_ipc::msg::SearchIndexDef>,
}

enum LoopExit {
    Shutdown,
    InstallSnapshot {
        path: PathBuf,
        index: u64,
        term: u64,
        reply: oneshot::Sender<DbResult<()>>,
    },
}

pub struct WriterConfig {
    pub db_path: PathBuf,
    pub text_max_bytes: usize,
    pub max_mutation_bytes: usize,
    pub idle_tx_timeout: Duration,
}

pub fn run(shared: Arc<Shared>, cfg: WriterConfig, rx: Receiver<WriterMsg>) {
    let mut pending: VecDeque<WriterMsg> = VecDeque::new();
    loop {
        let conn = match open_connection(&shared, &cfg.db_path, true) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("writer cannot open database: {e}");
                shared.quarantine(format!("cannot open database: {e}"));
                // 열 수 없으면 모든 요청 실패
                while let Ok(m) = rx.recv() {
                    if matches!(m, WriterMsg::Shutdown) {
                        return;
                    }
                    m.fail(DbError::store_recovery_required(e.message.clone()));
                }
                return;
            }
        };
        let exit = {
            let mut w = Writer {
                shared: &shared,
                cfg: &cfg,
                conn: &conn,
                owner: None,
                waiting: std::mem::take(&mut pending),
                aborted: HashMap::new(),
            };
            let exit = w.run_loop(&rx);
            if let Some(o) = w.owner.take() {
                let _ = w.rollback(o);
            }
            pending = std::mem::take(&mut w.waiting);
            exit
        };
        match exit {
            LoopExit::Shutdown => {
                let _ = conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)");
                drop(conn);
                for m in pending {
                    m.fail(DbError::unavailable("store shutting down"));
                }
                return;
            }
            LoopExit::InstallSnapshot {
                path,
                index,
                term,
                reply,
            } => {
                drop(conn);
                let r = shared.install_snapshot_files(&cfg.db_path, &path, index, term);
                if let Err(e) = &r {
                    tracing::error!("snapshot install failed: {e}");
                    shared.quarantine(format!("snapshot install failed: {e}"));
                }
                let _ = reply.send(r);
            }
        }
    }
}

struct Writer<'a, 'c> {
    shared: &'a Arc<Shared>,
    cfg: &'a WriterConfig,
    conn: &'c Connection,
    owner: Option<Owner<'c>>,
    waiting: VecDeque<WriterMsg>,
    /// 서버가 강제로 중단한 트랜잭션 (사유 보고용).
    aborted: HashMap<Id128, DbError>,
}

impl<'a, 'c> Writer<'a, 'c> {
    fn run_loop(&mut self, rx: &Receiver<WriterMsg>) -> LoopExit {
        loop {
            // 잠금이 풀렸으면 대기열 처리
            if self.owner.is_none() {
                if let Some(exit) = self.drain_waiting() {
                    return exit;
                }
            }
            let msg = match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(m) => m,
                Err(RecvTimeoutError::Timeout) => {
                    self.tick();
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => return LoopExit::Shutdown,
            };
            if let Some(exit) = self.handle(msg) {
                return exit;
            }
            self.shared
                .metrics
                .writer_queue
                .store(self.waiting.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn drain_waiting(&mut self) -> Option<LoopExit> {
        while self.owner.is_none() {
            let Some(m) = self.waiting.pop_front() else {
                break;
            };
            if let Some(exit) = self.handle(m) {
                return Some(exit);
            }
        }
        None
    }

    fn tick(&mut self) {
        let now = Instant::now();
        // 대기 타임아웃
        let mut keep = VecDeque::new();
        while let Some(m) = self.waiting.pop_front() {
            match &m {
                WriterMsg::Exec { deadline, .. } if *deadline <= now => m.fail(DbError::new(
                    "",
                    "55P03",
                    "could not obtain write lock: another transaction holds the cluster writer",
                )),
                _ => keep.push_back(m),
            }
        }
        self.waiting = keep;
        // 유휴 트랜잭션 강제 종료 (Prepared 상태는 Raft 결과를 기다리므로 제외)
        let idle = self
            .owner
            .as_ref()
            .map(|o| {
                o.prepared.is_none()
                    && now.duration_since(o.last_activity) > self.cfg.idle_tx_timeout
            })
            .unwrap_or(false);
        if idle {
            let o = self.owner.take().unwrap();
            tracing::warn!(
                "aborting idle write transaction {}",
                jamsil_common::id_to_string(o.txn)
            );
            self.aborted.insert(
                o.txn,
                DbError::new(
                    "",
                    "25P03",
                    "terminating transaction due to idle-in-transaction timeout",
                ),
            );
            let _ = self.rollback(o);
        }
        if self.owner.is_none() {
            self.flush_outbox_ack();
        }
        if self.aborted.len() > 10_000 {
            self.aborted.clear();
        }
    }

    fn handle(&mut self, msg: WriterMsg) -> Option<LoopExit> {
        // 다른 트랜잭션이 Writer를 소유 중이면 대기
        if let (Some(o), Some(t)) = (&self.owner, msg.txn()) {
            if o.txn != t {
                self.waiting.push_back(msg);
                return None;
            }
        }
        match msg {
            WriterMsg::Exec {
                txn,
                stmt,
                request_id,
                expect_applied,
                reply,
                ..
            } => {
                let r = self.exec(txn, stmt, request_id, expect_applied);
                let _ = reply.send(r);
            }
            WriterMsg::Prepare {
                txn,
                request_id,
                reply,
            } => {
                let r = self.prepare(txn, request_id);
                let _ = reply.send(r);
            }
            WriterMsg::Abort { txn, reply } => {
                if self.owner.as_ref().map(|o| o.txn == txn).unwrap_or(false) {
                    let o = self.owner.take().unwrap();
                    let r = self.rollback(o);
                    let _ = reply.send(r);
                } else {
                    // 대기 중인 요청 제거
                    let mut keep = VecDeque::new();
                    while let Some(m) = self.waiting.pop_front() {
                        if m.txn() == Some(txn) {
                            m.fail(DbError::new("", "57014", "transaction aborted"));
                        } else {
                            keep.push_back(m);
                        }
                    }
                    self.waiting = keep;
                    self.aborted.remove(&txn);
                    let _ = reply.send(Ok(()));
                }
            }
            WriterMsg::AbortAll { reply } => {
                if let Some(o) = self.owner.take() {
                    self.aborted.insert(
                        o.txn,
                        DbError::leader_changed("transaction aborted by leader change"),
                    );
                    let _ = self.rollback(o);
                }
                for m in std::mem::take(&mut self.waiting) {
                    if matches!(m, WriterMsg::Exec { .. } | WriterMsg::Prepare { .. }) {
                        m.fail(DbError::leader_changed(
                            "transaction aborted by leader change",
                        ));
                    } else {
                        self.waiting.push_back(m);
                    }
                }
                let _ = reply.send(Ok(()));
            }
            WriterMsg::Apply {
                index,
                term,
                cmd,
                reply,
            } => {
                let r = self.apply(index, term, cmd);
                if let Err(e) = &r {
                    if e.sqlstate != "57P03" {
                        tracing::error!("apply of log index {index} failed: {e}");
                        self.shared
                            .quarantine(format!("apply of log index {index} failed: {e}"));
                    }
                }
                let _ = reply.send(r);
            }
            WriterMsg::InstallSnapshot {
                path,
                index,
                term,
                reply,
            } => {
                if let Some(o) = self.owner.take() {
                    self.aborted.insert(
                        o.txn,
                        DbError::leader_changed("aborted by snapshot install"),
                    );
                    let _ = self.rollback(o);
                }
                return Some(LoopExit::InstallSnapshot {
                    path,
                    index,
                    term,
                    reply,
                });
            }
            WriterMsg::OutboxAck { upto, reply } => {
                self.shared.outbox_ack_request(upto);
                if self.owner.is_none() {
                    self.flush_outbox_ack();
                }
                let _ = reply.send(Ok(()));
            }
            WriterMsg::Shutdown => return Some(LoopExit::Shutdown),
        }
        None
    }

    fn rollback(&mut self, o: Owner<'c>) -> DbResult<()> {
        drop(o);
        if !self.conn.is_autocommit() {
            self.conn.execute_batch("ROLLBACK").map_err(map_err)?;
        }
        self.shared.set_writer_owner(None);
        Ok(())
    }

    fn begin(&mut self, txn: Id128) -> DbResult<()> {
        if let Some(q) = self.shared.quarantined() {
            return Err(DbError::store_recovery_required(q));
        }
        self.disk_full_point();
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(map_err)?;
        let mut session = match Session::new(self.conn) {
            Ok(s) => s,
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                return Err(map_err(e));
            }
        };
        session.table_filter(Some(|t: &str| !is_system_table(t)));
        if let Err(e) = session.attach(None) {
            drop(session);
            let _ = self.conn.execute_batch("ROLLBACK");
            return Err(map_err(e));
        }
        let search_before = self.shared.catalog().search.clone();
        self.owner = Some(Owner {
            txn,
            session,
            ddl: vec![],
            has_dml: false,
            failed: None,
            last_activity: Instant::now(),
            prepared: None,
            prepared_cmd: None,
            prepared_at_applied: 0,
            tag: String::new(),
            request: None,
            last_result: None,
            savepoints: vec![],
            txn_digest: <sha2::Sha256 as sha2::Digest>::new(),
            search_before,
        });
        self.shared.set_writer_owner(Some(txn));
        Ok(())
    }

    fn exec(
        &mut self,
        txn: Id128,
        stmt: Statement,
        request_id: Option<String>,
        expect_applied: Option<u64>,
    ) -> DbResult<QueryResult> {
        if let Some(e) = self.aborted.remove(&txn) {
            return Err(e);
        }
        if self.owner.is_none() {
            self.begin(txn)?;
            // 이후 판단은 모두 Writer 소유(클러스터 단일 직렬화 경계) 안에서 이뤄진다.
            if let Some(snap) = expect_applied {
                let st = read_state(self.conn)?;
                if st.applied_index != snap {
                    let o = self.owner.take().unwrap();
                    self.rollback(o)?;
                    return Err(DbError::new(
                        "",
                        "40001",
                        format!(
                            "could not serialize access due to concurrent update (snapshot at log index {snap}, now {})",
                            st.applied_index
                        ),
                    )
                    .with_hint("retry the transaction"));
                }
            }
            if let Some(rid) = &request_id {
                let fp = fingerprint(&stmt);
                if let Some(rec) = lookup_request(self.conn, rid)? {
                    let o = self.owner.take().unwrap();
                    self.rollback(o)?;
                    return replay(rid, &fp, rec);
                }
                self.owner.as_mut().unwrap().request = Some((rid.clone(), fp));
            }
        }
        let kind = jamsil_sql::classify(&stmt.sql)
            .ok()
            .and_then(|v| v.into_iter().next())
            .map(|c| c.kind);
        {
            let o = self.owner.as_mut().unwrap();
            o.last_activity = Instant::now();
            if o.prepared.is_some() {
                return Err(DbError::new(
                    "",
                    "25000",
                    "transaction is already committing",
                ));
            }
            // 실패한 트랜잭션에서는 ROLLBACK TO SAVEPOINT 만 허용 (PostgreSQL 의미)
            if o.failed.is_some() && !matches!(kind, Some(StmtKind::RollbackTo(_))) {
                return Err(DbError::in_failed_tx());
            }
        }
        {
            use sha2::Digest;
            let fp = fingerprint(&stmt);
            self.owner.as_mut().unwrap().txn_digest.update(fp.as_bytes());
        }
        let r = self.exec_inner(&stmt);
        let o = self.owner.as_mut().unwrap();
        o.last_activity = Instant::now();
        match r {
            Ok(res) => {
                let fold = |n: &str| n.trim_matches('"').to_ascii_lowercase();
                match &kind {
                    Some(StmtKind::Savepoint(n)) => o.savepoints.push((fold(n), o.ddl.len(), o.has_dml)),
                    Some(StmtKind::RollbackTo(n)) => {
                        let n = fold(n);
                        if let Some(pos) = o.savepoints.iter().rposition(|(x, _, _)| *x == n) {
                            let (_, ddl_len, had_dml) = o.savepoints[pos].clone();
                            // 되돌린 DDL은 복제 대상에서도 제거 (Patchset은 최종 상태 비교라 자동 반영)
                            o.ddl.truncate(ddl_len);
                            o.has_dml = had_dml;
                            o.savepoints.truncate(pos + 1);
                        }
                        o.failed = None;
                    }
                    Some(StmtKind::Release(n)) => {
                        let n = fold(n);
                        if let Some(pos) = o.savepoints.iter().rposition(|(x, _, _)| *x == n) {
                            o.savepoints.truncate(pos);
                        }
                    }
                    _ => o.last_result = Some(res.clone()),
                }
                Ok(res)
            }
            Err(e) => {
                o.failed = Some(e.clone());
                Err(e)
            }
        }
    }

    fn catalog_for_txn(&self) -> DbResult<Arc<CatalogCache>> {
        let o = self.owner.as_ref().unwrap();
        if o.ddl.is_empty() {
            Ok(self.shared.catalog())
        } else {
            Ok(Arc::new(CatalogCache::load(self.conn)?))
        }
    }

    fn exec_inner(&mut self, stmt: &Statement) -> DbResult<QueryResult> {
        let catalog = self.catalog_for_txn()?;
        let acl = self.shared.acl_ctx(&stmt.user, catalog.as_ref());
        let tr = Translator::new(catalog.as_ref(), "_jm_w")
            .with_principal(acl.principal.as_ref())
            .translate(&stmt.sql)?;
        let _acl = crate::functions::AclScope::enter(acl);
        if !tr.text_matches.is_empty() {
            return Err(DbError::unsupported(
                "TEXT_MATCH inside a write transaction",
            ));
        }
        let is_ddl = tr.kind == StmtKind::Ddl;
        {
            let o = self.owner.as_ref().unwrap();
            if is_ddl && o.has_dml {
                return Err(DbError::new(
                    "JDB-5101",
                    "25001",
                    "DDL after DML in the same transaction is not supported; commit the DML first",
                ));
            }
        }
        if let Some(sd) = &tr.search_ddl {
            let sql = self.search_ddl_sql(sd, catalog.as_ref())?;
            let mut res = QueryResult {
                tag: tr.tag.clone(),
                ..Default::default()
            };
            for s in sql {
                self.conn.execute_batch(&s).map_err(map_err)?;
                self.owner.as_mut().unwrap().ddl.push(s);
            }
            res.notices = tr.notices.clone();
            self.owner.as_mut().unwrap().tag = tr.tag.clone();
            return Ok(res);
        }
        // PostgreSQL 의미: 의존하는 뷰가 있으면 DROP TABLE 거부 (CASCADE 미지원)
        for t in &tr.dropped_tables {
            let deps = crate::catalog::dependent_views(self.conn, t)?;
            if !deps.is_empty() {
                return Err(DbError::new(
                    "",
                    "2BP01",
                    format!("cannot drop table {t} because other objects depend on it (view {})", deps.join(", ")),
                )
                .with_hint("drop the dependent views first"));
            }
        }
        let mut last = QueryResult {
            tag: tr.tag.clone(),
            notices: tr.notices.clone(),
            ..Default::default()
        };
        for s in &tr.statements {
            let out = exec(self.conn, s, &stmt.params, &tr.column_type_hints)?;
            if is_ddl {
                self.owner.as_mut().unwrap().ddl.push(s.clone());
            }
            let mut r = to_result(&tr.tag, out);
            r.notices = tr.notices.clone();
            last = r;
        }
        if is_ddl {
            let o = self.owner.as_mut().unwrap();
            for t in &tr.dropped_tables {
                o.ddl.push(format!(
                    "DELETE FROM _jamsil_search_indexes WHERE table_name = '{}'",
                    t.replace('\'', "''")
                ));
            }
            for (old, new) in &tr.renamed_tables {
                o.ddl.push(format!(
                    "UPDATE _jamsil_search_indexes SET table_name = '{}' WHERE table_name = '{}'",
                    new.replace('\'', "''"),
                    old.replace('\'', "''")
                ));
            }
            for s in o
                .ddl
                .iter()
                .rev()
                .take(tr.dropped_tables.len() + tr.renamed_tables.len())
                .collect::<Vec<_>>()
            {
                self.conn.execute_batch(s).map_err(map_err)?;
            }
        } else if tr.kind == StmtKind::Write {
            self.owner.as_mut().unwrap().has_dml = true;
        }
        self.owner.as_mut().unwrap().tag = last.tag.clone();
        Ok(last)
    }

    fn search_ddl_sql(&self, sd: &SearchDdl, cat: &CatalogCache) -> DbResult<Vec<String>> {
        let q = |s: &str| s.replace('\'', "''");
        match sd {
            SearchDdl::Create(c) => {
                if cat.search_def(&c.name).is_some() {
                    if c.if_not_exists {
                        return Ok(vec![]);
                    }
                    return Err(DbError::new(
                        "",
                        "42P07",
                        format!("search index \"{}\" already exists", c.name),
                    ));
                }
                let cols = cat.tables.get(&c.table).ok_or_else(|| {
                    DbError::new(
                        "",
                        "42P01",
                        format!("relation \"{}\" does not exist", c.table),
                    )
                })?;
                if !cols.iter().any(|x| x.pk) {
                    return Err(DbError::unsupported(
                        "search index on a table without PRIMARY KEY",
                    ));
                }
                for col in &c.columns {
                    let ci = cols
                        .iter()
                        .find(|x| x.name.eq_ignore_ascii_case(col))
                        .ok_or_else(|| {
                            DbError::new(
                                "",
                                "42703",
                                format!(
                                    "column \"{col}\" of relation \"{}\" does not exist",
                                    c.table
                                ),
                            )
                        })?;
                    let (o, _) = jamsil_sql::types::decl_to_oid(&ci.decl);
                    use jamsil_sql::types::oid;
                    if !matches!(o, oid::TEXT | oid::VARCHAR | oid::BPCHAR) {
                        return Err(DbError::new(
                            "",
                            "42804",
                            format!("search index column \"{col}\" must be TEXT, VARCHAR or CHAR"),
                        ));
                    }
                }
                let canon: Vec<String> = c
                    .columns
                    .iter()
                    .map(|col| {
                        cols.iter()
                            .find(|x| x.name.eq_ignore_ascii_case(col))
                            .unwrap()
                            .name
                            .clone()
                    })
                    .collect();
                let o = self.owner.as_ref().unwrap();
                let generation = (o.txn as u64 & 0x7fff_ffff_ffff_ffff) as i64;
                Ok(vec![format!(
                    "INSERT INTO _jamsil_search_indexes (name, table_name, columns, analyzer, scoring, created_index) VALUES ('{}', '{}', '{}', '{}', '{}', {})",
                    q(&c.name),
                    q(&c.table),
                    q(&serde_json::to_string(&canon).unwrap()),
                    q(&c.analyzer),
                    q(&c.scoring),
                    generation
                )])
            }
            SearchDdl::Drop(d) => {
                if cat.search_def(&d.name).is_none() {
                    if d.if_exists {
                        return Ok(vec![]);
                    }
                    return Err(DbError::new(
                        "",
                        "42704",
                        format!("search index \"{}\" does not exist", d.name),
                    ));
                }
                Ok(vec![format!(
                    "DELETE FROM _jamsil_search_indexes WHERE name = '{}'",
                    q(&d.name)
                )])
            }
        }
    }

    fn prepare(
        &mut self,
        txn: Id128,
        request_id: Option<String>,
    ) -> DbResult<Option<ReplicatedCommand>> {
        if let Some(e) = self.aborted.remove(&txn) {
            return Err(e);
        }
        let Some(o) = self.owner.as_mut() else {
            // 실행 단계에서 이미 처리된 멱등 요청이거나 변경이 없는 트랜잭션
            return Ok(None);
        };
        if o.txn != txn {
            return Err(DbError::internal("prepare for non-owner transaction"));
        }
        if o.failed.is_some() {
            return Err(DbError::in_failed_tx());
        }
        if let Some(c) = &o.prepared_cmd {
            return Ok(Some(c.clone()));
        }
        // 명시적 트랜잭션의 COMMIT 멱등 키: Writer를 소유한 상태에서 확인
        if let (Some(rid), None) = (&request_id, &o.request) {
            if let Some(rec) = lookup_request(self.conn, rid)? {
                let fp = txn_fingerprint(&o.txn_digest);
                if rec.2.as_deref().is_some_and(|f| f != fp) {
                    let o = self.owner.take().unwrap();
                    self.rollback(o)?;
                    return Err(reuse_error(rid));
                }
                // 이미 COMMIT된 트랜잭션의 재시도: 이번 실행분은 버린다.
                let o = self.owner.take().unwrap();
                self.rollback(o)?;
                return Ok(None);
            }
            let o = self.owner.as_mut().unwrap();
            let fp = txn_fingerprint(&o.txn_digest);
            o.request = Some((rid.clone(), fp));
            o.last_result = Some(QueryResult { tag: "COMMIT".into(), ..Default::default() });
        }
        let o = self.owner.as_mut().unwrap();
        let mut patch = Vec::new();
        if let Err(e) = o.session.patchset_strm(&mut patch) {
            let e = map_err(e);
            let o = self.owner.take().unwrap();
            let _ = self.rollback(o);
            return Err(e);
        }
        let o = self.owner.as_mut().unwrap();
        if patch.is_empty() && o.ddl.is_empty() && o.request.is_none() {
            // 변경 없음: 복제할 것이 없다. (멱등 키가 있으면 결과 기록을 위해 복제한다)
            let o = self.owner.take().unwrap();
            self.rollback(o)?;
            return Ok(None);
        }
        let ddl = o.ddl.clone();
        let tag = o.tag.clone();
        let request = o.request.clone();
        let last_result = o.last_result.clone();
        if let Err(e) = self.validate_patch(&patch, &ddl) {
            let o = self.owner.take().unwrap();
            let _ = self.rollback(o);
            self.shared
                .metrics
                .text_rejected
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(e);
        }
        jamsil_common::metrics::histogram("jamsil_mutation_payload_bytes", "", "bytes")
            .record_raw((patch.len() + ddl.iter().map(|d| d.len()).sum::<usize>()) as u64);
        let state = read_state(self.conn)?;
        let checksum = TxnPayload::compute_checksum(&ddl, &patch);
        let payload = TxnPayload {
            tx_id: txn,
            request_id: request.as_ref().map(|r| r.0.clone()),
            schema_version: state.schema_version,
            ddl,
            changeset: patch,
            tag: tag.clone(),
            checksum,
        };
        let cmd = match request {
            None => ReplicatedCommand::Txn(payload.clone()),
            Some((rid, fp)) => {
                let result = last_result.unwrap_or(QueryResult { tag, ..Default::default() });
                let bytes = bincode::serialize(&result).map_err(|e| DbError::internal(e.to_string()))?;
                if bytes.len() > MAX_RECORDED_RESULT {
                    let o = self.owner.take().unwrap();
                    let _ = self.rollback(o);
                    return Err(DbError::new(
                        "",
                        "54000",
                        format!(
                            "result of an idempotent request ({} bytes) exceeds the recordable limit ({MAX_RECORDED_RESULT} bytes)",
                            bytes.len()
                        ),
                    )
                    .with_hint("reduce RETURNING output or run without jamsil.request_id"));
                }
                ReplicatedCommand::TxnWithRequest {
                    payload: payload.clone(),
                    request: RequestRecord { request_id: rid, fingerprint: fp, result: bytes },
                }
            }
        };
        let o = self.owner.as_mut().unwrap();
        o.prepared = Some(payload);
        o.prepared_cmd = Some(cmd.clone());
        o.prepared_at_applied = state.applied_index;
        Ok(Some(cmd))
    }

    /// TEXT 값별 32MiB, 트랜잭션 Mutation 64MiB 상한 검사.
    fn validate_patch(&self, patch: &[u8], ddl: &[String]) -> DbResult<()> {
        let total = patch.len() + ddl.iter().map(|d| d.len()).sum::<usize>();
        if total > self.cfg.max_mutation_bytes {
            return Err(DbError::mutation_too_large(
                total,
                self.cfg.max_mutation_bytes,
            ));
        }
        let cat = CatalogCache::load(self.conn)?;
        let mut input: &[u8] = patch;
        let r: &mut dyn Read = &mut input;
        let mut iter = ChangesetIter::start_strm(&r).map_err(map_err)?;
        use fallible_streaming_iterator::FallibleStreamingIterator;
        while let Some(item) = iter.next().map_err(map_err)? {
            let op = item.op().map_err(map_err)?;
            let table = op.table_name().to_string();
            let ncol = op.number_of_columns() as usize;
            let code = op.code();
            if !matches!(
                code,
                rusqlite::hooks::Action::SQLITE_INSERT | rusqlite::hooks::Action::SQLITE_UPDATE
            ) {
                continue;
            }
            let Some(cols) = cat.tables.get(&table.to_ascii_lowercase()) else {
                continue;
            };
            for i in 0..ncol {
                let Some(ci) = cols.get(i) else { continue };
                if !decl_is_text(&ci.decl) {
                    continue;
                }
                if let Ok(v) = item.new_value(i) {
                    let len = match v {
                        rusqlite::types::ValueRef::Text(t) => t.len(),
                        rusqlite::types::ValueRef::Blob(b) => b.len(),
                        _ => 0,
                    };
                    if len > self.cfg.text_max_bytes {
                        return Err(DbError::text_too_large(
                            &table,
                            &ci.name,
                            len,
                            self.cfg.text_max_bytes,
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn apply(&mut self, index: u64, term: u64, cmd: ReplicatedCommand) -> DbResult<u64> {
        if let Some(q) = self.shared.quarantined() {
            return Err(DbError::store_recovery_required(q));
        }
        let state = read_state(self.conn)?;
        if index <= state.applied_index {
            // 이미 적용됨 (exactly-once effect)
            if let Some(o) = &self.owner {
                if let Some(p) = cmd.payload() {
                    if o.txn == p.tx_id {
                        let o = self.owner.take().unwrap();
                        let _ = self.rollback(o);
                    }
                }
            }
            return Ok(state.applied_index);
        }
        if index != state.applied_index + 1 {
            return Err(DbError::new(
                "",
                "57P03",
                format!(
                    "log gap: applied {} but received {}",
                    state.applied_index, index
                ),
            ));
        }
        // 소유 트랜잭션이 이 Entry가 아니면 중단 (Leader 교체 등)
        let own = match (&self.owner, cmd.payload()) {
            (Some(o), Some(p)) => o.txn == p.tx_id && o.prepared.is_some(),
            _ => false,
        };
        let req = match &cmd {
            ReplicatedCommand::TxnWithRequest { request, .. } => Some(request),
            _ => None,
        };
        if !own {
            if let Some(o) = self.owner.take() {
                tracing::warn!(
                    "aborting local transaction {} to apply log index {index}",
                    jamsil_common::id_to_string(o.txn)
                );
                self.aborted.insert(
                    o.txn,
                    DbError::leader_changed("transaction superseded by replicated log"),
                );
                self.rollback(o)?;
            }
        }
        let r = if own {
            let o = self.owner.take().unwrap();
            if o.prepared_at_applied + 1 != index {
                // 실행 기준 상태와 적용 위치가 다르면 Patchset 적용 경로로 검증한다.
                tracing::warn!(
                    "prepared txn executed at {} but committed at {index}; re-applying",
                    o.prepared_at_applied
                );
                let payload = o.prepared.clone().unwrap();
                self.rollback(o)?;
                self.apply_remote(index, term, &payload, req)
            } else {
                self.commit_own(o, index, term, req)
            }
        } else {
            match &cmd {
                ReplicatedCommand::Noop => self.apply_noop(index, term),
                ReplicatedCommand::Txn(p) => self.apply_remote(index, term, p, None),
                ReplicatedCommand::TxnWithRequest { payload, request } => {
                    self.apply_remote(index, term, payload, Some(request))
                }
            }
        };
        match r {
            Ok(()) => {
                // 카탈로그를 먼저 갱신한 뒤 applied_index를 공개한다: 이 index를 기다린 읽기/Describe가
                // 새 스키마(테이블·검색 인덱스)를 보도록.
                if let Some(p) = cmd.payload() {
                    if p.has_ddl() {
                        match CatalogCache::load(self.conn) {
                            Ok(c) => self.shared.set_catalog(c),
                            Err(e) => tracing::error!("catalog reload failed: {e}"),
                        }
                    }
                }
                self.shared.on_applied(index, term);
                self.flush_outbox_ack();
                Ok(index)
            }
            Err(e) => {
                if !self.conn.is_autocommit() {
                    let _ = self.conn.execute_batch("ROLLBACK");
                }
                self.shared.set_writer_owner(None);
                Err(e)
            }
        }
    }

    fn apply_noop(&mut self, index: u64, term: u64) -> DbResult<()> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(map_err)?;
        self.update_state(index, term, false)?;
        self.commit_sqlite()
    }

    /// SQLite COMMIT 소요 시간 기록 (synchronous=FULL: WAL 기록 + fsync 포함).
    fn commit_sqlite(&self) -> DbResult<()> {
        let t0 = Instant::now();
        let r = self.conn.execute_batch("COMMIT").map_err(map_err);
        jamsil_common::metrics::histogram("jamsil_storage_fsync_duration_seconds", "", "seconds")
            .record_duration(t0.elapsed());
        r
    }

    fn update_state(&self, index: u64, term: u64, schema_change: bool) -> DbResult<()> {
        let n = self
            .conn
            .execute(
                "UPDATE _jamsil_state SET applied_index = ?1, applied_term = ?2, schema_version = schema_version + ?3 WHERE singleton_id = 1 AND applied_index = ?4",
                params![index as i64, term as i64, schema_change as i64, index as i64 - 1],
            )
            .map_err(map_err)?;
        if n != 1 {
            return Err(DbError::internal(format!(
                "applied_index precondition failed at {index}"
            )));
        }
        Ok(())
    }

    /// Leader: 자신의 미확정 트랜잭션을 applied_index와 함께 원자적으로 COMMIT.
    fn commit_own(
        &mut self,
        o: Owner<'c>,
        index: u64,
        term: u64,
        req: Option<&RequestRecord>,
    ) -> DbResult<()> {
        let payload = o.prepared.clone().unwrap();
        let search_before = o.search_before.clone();
        drop(o);
        self.shared.set_writer_owner(None);
        self.post_apply(index, term, &payload, req, &search_before)?;
        self.crash_point();
        self.commit_sqlite()?;
        self.crash_after_commit_point();
        Ok(())
    }

    /// Follower(또는 Leader 교체 후): DDL + Patchset 적용 후 COMMIT.
    fn apply_remote(
        &mut self,
        index: u64,
        term: u64,
        p: &TxnPayload,
        req: Option<&RequestRecord>,
    ) -> DbResult<()> {
        if !p.verify() {
            return Err(DbError::internal(format!(
                "log index {index}: payload checksum mismatch"
            )));
        }
        let state = read_state(self.conn)?;
        if p.schema_version != state.schema_version {
            return Err(DbError::internal(format!(
                "log index {index}: schema version mismatch (entry {}, local {})",
                p.schema_version, state.schema_version
            )));
        }
        let search_before = self.shared.catalog().search.clone();
        self.disk_full_point();
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(map_err)?;
        for d in &p.ddl {
            self.conn.execute_batch(d).map_err(map_err)?;
        }
        if !p.changeset.is_empty() {
            let conflict: Arc<parking_lot::Mutex<Option<String>>> =
                Arc::new(parking_lot::Mutex::new(None));
            let c2 = conflict.clone();
            let mut input: &[u8] = &p.changeset;
            let r = self.conn.apply_strm(
                &mut input,
                None::<fn(&str) -> bool>,
                move |ct: ConflictType, item| {
                    let table = item
                        .op()
                        .map(|o| o.table_name().to_string())
                        .unwrap_or_default();
                    *c2.lock() = Some(format!("{ct:?} on table {table}"));
                    ConflictAction::SQLITE_CHANGESET_ABORT
                },
            );
            if let Err(e) = r {
                let detail = conflict
                    .lock()
                    .clone()
                    .unwrap_or_else(|| map_err(e).message);
                return Err(DbError::internal(format!(
                    "log index {index}: replica divergence while applying patchset: {detail}"
                )));
            }
        }
        self.post_apply(index, term, p, req, &search_before)?;
        self.crash_point();
        self.commit_sqlite()?;
        self.crash_after_commit_point();
        Ok(())
    }

    /// 장애 주입 (시험 전용): JAMSIL_FAULT_INJECTION=1 이고 db 디렉터리에 `crash_before_apply_commit` 파일이 있으면
    /// Raft 확정 후 SQLite COMMIT 직전에 프로세스를 강제 종료한다 (설계서 T-07).
    fn crash_point(&self) {
        if let Some(dir) = self.cfg.db_path.parent() {
            if jamsil_common::fault::take(dir, "crash_before_apply_commit") {
                tracing::error!("fault injection: crashing before apply COMMIT");
                jamsil_common::fault::crash("crash_before_apply_commit");
            }
        }
    }

    /// T-08: COMMIT 직후 (Apply 완료 보고 전) Crash.
    fn crash_after_commit_point(&self) {
        if let Some(dir) = self.cfg.db_path.parent() {
            if jamsil_common::fault::take(dir, "crash_after_apply_commit") {
                tracing::error!("fault injection: crashing right after apply COMMIT");
                jamsil_common::fault::crash("crash_after_apply_commit");
            }
        }
    }

    /// T-15: 디스크 Full 모의 — 이 Apply 트랜잭션에서 DB 파일이 커질 수 없게 한다.
    fn disk_full_point(&self) {
        if let Some(dir) = self.cfg.db_path.parent() {
            if jamsil_common::fault::active(dir, "sqlite_full") {
                let pages: i64 = self
                    .conn
                    .query_row("PRAGMA page_count", [], |r| r.get(0))
                    .unwrap_or(1);
                let _ = self
                    .conn
                    .query_row(&format!("PRAGMA max_page_count = {pages}"), [], |_| Ok(()));
            } else {
                let _ = self
                    .conn
                    .query_row("PRAGMA max_page_count = 4294967294", [], |_| Ok(()));
            }
        }
    }

    /// 적용 공통 처리 (같은 SQLite 트랜잭션 안): applied_index, 멱등 키, 검색 Outbox.
    fn post_apply(
        &self,
        index: u64,
        term: u64,
        p: &TxnPayload,
        req: Option<&RequestRecord>,
        search_before: &[jamsil_ipc::msg::SearchIndexDef],
    ) -> DbResult<()> {
        self.update_state(index, term, p.has_ddl())?;
        if let Some(r) = req {
            // 요청 이력·결과를 Row 변경과 같은 원자 COMMIT으로 기록 (모든 복제본 동일)
            self.conn
                .execute(
                    "INSERT INTO _jamsil_requests (request_id, raft_index, tag, fingerprint, result) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![r.request_id, index as i64, p.tag, r.fingerprint, r.result],
                )
                .map_err(map_err)?;
        } else if let Some(rid) = &p.request_id {
            self.conn
                .execute(
                    "INSERT INTO _jamsil_requests (request_id, raft_index, tag) VALUES (?1, ?2, ?3) ON CONFLICT(request_id) DO NOTHING",
                    params![rid, index as i64, p.tag],
                )
                .map_err(map_err)?;
        }
        // 검색 인덱스 정의 변경
        let cat_after = if p.has_ddl() {
            Arc::new(CatalogCache::load(self.conn)?)
        } else {
            self.shared.catalog()
        };
        let mut ins = self
            .conn
            .prepare_cached("INSERT INTO _jamsil_outbox (raft_index, index_name, op, pk) VALUES (?1, ?2, ?3, ?4)")
            .map_err(map_err)?;
        let mut events = 0u64;
        if p.has_ddl() {
            for old in search_before {
                let still = cat_after
                    .search
                    .iter()
                    .any(|n| n.name == old.name && n.created_index == old.created_index);
                if !still {
                    ins.execute(params![index as i64, old.name, OP_INDEX_DROPPED, "[]"])
                        .map_err(map_err)?;
                    events += 1;
                }
            }
            for new in &cat_after.search {
                let existed = search_before
                    .iter()
                    .any(|o| o.name == new.name && o.created_index == new.created_index);
                if !existed {
                    ins.execute(params![index as i64, new.name, OP_INDEX_CREATED, "[]"])
                        .map_err(map_err)?;
                    events += 1;
                }
            }
        }
        if !cat_after.search.is_empty() && !p.changeset.is_empty() {
            let mut input: &[u8] = &p.changeset;
            let r: &mut dyn Read = &mut input;
            let mut iter = ChangesetIter::start_strm(&r).map_err(map_err)?;
            use fallible_streaming_iterator::FallibleStreamingIterator;
            while let Some(item) = iter.next().map_err(map_err)? {
                let op = item.op().map_err(map_err)?;
                let table = op.table_name().to_ascii_lowercase();
                let idxs = cat_after.search_for_table(&table);
                if idxs.is_empty() {
                    continue;
                }
                let Some(cols) = cat_after.tables.get(&table) else {
                    continue;
                };
                let pkflags = item.pk().map_err(map_err)?.to_vec();
                let pk_idx: Vec<usize> = pkflags
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| **f != 0)
                    .map(|(i, _)| i)
                    .collect();
                let get_pk = |old: bool| -> DbResult<Option<Vec<Value>>> {
                    let mut v = Vec::with_capacity(pk_idx.len());
                    for &i in &pk_idx {
                        let r = if old {
                            item.old_value(i)
                        } else {
                            item.new_value(i)
                        };
                        match r {
                            Ok(x) => v.push(value_from_ref(x)?),
                            Err(_) => return Ok(None),
                        }
                    }
                    Ok(Some(v))
                };
                use rusqlite::hooks::Action;
                for idx in idxs {
                    let idx_cols: Vec<usize> = idx
                        .columns
                        .iter()
                        .filter_map(|c| cols.iter().position(|x| x.name.eq_ignore_ascii_case(c)))
                        .collect();
                    match op.code() {
                        Action::SQLITE_INSERT => {
                            if let Some(pk) = get_pk(false)? {
                                ins.execute(params![
                                    index as i64,
                                    idx.name,
                                    OP_UPSERT,
                                    pk_to_json(&pk)
                                ])
                                .map_err(map_err)?;
                                events += 1;
                            }
                        }
                        Action::SQLITE_DELETE => {
                            if let Some(pk) = get_pk(true)? {
                                ins.execute(params![
                                    index as i64,
                                    idx.name,
                                    OP_DELETE,
                                    pk_to_json(&pk)
                                ])
                                .map_err(map_err)?;
                                events += 1;
                            }
                        }
                        Action::SQLITE_UPDATE => {
                            let old_pk = get_pk(true)?;
                            let new_pk_changed = pk_idx.iter().any(|&i| item.new_value(i).is_ok());
                            let text_changed = idx_cols.iter().any(|&i| item.new_value(i).is_ok());
                            if new_pk_changed {
                                if let Some(op) = &old_pk {
                                    ins.execute(params![
                                        index as i64,
                                        idx.name,
                                        OP_DELETE,
                                        pk_to_json(op)
                                    ])
                                    .map_err(map_err)?;
                                }
                                // 변경된 PK: 바뀌지 않은 PK 컬럼은 old 값 사용
                                let mut np = Vec::with_capacity(pk_idx.len());
                                for &i in &pk_idx {
                                    let v = match item.new_value(i) {
                                        Ok(x) => value_from_ref(x)?,
                                        Err(_) => {
                                            value_from_ref(item.old_value(i).map_err(map_err)?)?
                                        }
                                    };
                                    np.push(v);
                                }
                                ins.execute(params![
                                    index as i64,
                                    idx.name,
                                    OP_UPSERT,
                                    pk_to_json(&np)
                                ])
                                .map_err(map_err)?;
                                events += 2;
                            } else if text_changed {
                                if let Some(pk) = old_pk {
                                    ins.execute(params![
                                        index as i64,
                                        idx.name,
                                        OP_UPSERT,
                                        pk_to_json(&pk)
                                    ])
                                    .map_err(map_err)?;
                                    events += 1;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        if events > 0 {
            self.shared.outbox_bumped();
        }
        Ok(())
    }

    fn flush_outbox_ack(&mut self) {
        let Some(upto) = self.shared.take_outbox_ack() else {
            return;
        };
        let r = self
            .conn
            .execute("DELETE FROM _jamsil_outbox WHERE seq <= ?1", params![upto as i64])
            .and_then(|_| {
                let cur: Option<i64> = self
                    .conn
                    .query_row("SELECT v FROM _jamsil_local WHERE k = 'outbox_acked'", [], |r| r.get(0))
                    .optional()?;
                if cur.unwrap_or(0) < upto as i64 {
                    self.conn.execute(
                        "INSERT INTO _jamsil_local(k, v) VALUES ('outbox_acked', ?1) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                        params![upto as i64],
                    )?;
                }
                Ok(())
            });
        if let Err(e) = r {
            tracing::warn!("outbox cleanup failed: {e}");
        }
    }
}

/// 노드 로컬: Snapshot 설치 후 검색 재색인 세대 증가.
pub fn bump_search_epoch(conn: &Connection) -> DbResult<i64> {
    let cur = local_get(conn, "search_rebuild_epoch")?.unwrap_or(0) + 1;
    local_set(conn, "search_rebuild_epoch", cur)?;
    Ok(cur)
}

/// 멱등 요청 결과로 기록할 수 있는 최대 크기.
const MAX_RECORDED_RESULT: usize = 1024 * 1024;
/// 명시적 트랜잭션 COMMIT에 쓰인 키의 지문: 실행한 문장(+파라미터) 순서 전체.
fn txn_fingerprint(d: &sha2::Sha256) -> String {
    use sha2::Digest;
    format!("txn:{}", hex::encode(&d.clone().finalize()[..16]))
}

/// 문장 + 파라미터 지문 (같은 키를 다른 요청에 재사용했는지 판정).
fn fingerprint(stmt: &Statement) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(stmt.sql.trim().as_bytes());
    h.update([0u8]);
    h.update(bincode::serialize(&stmt.params).unwrap_or_default());
    hex::encode(&h.finalize()[..16])
}

type RequestRow = (i64, String, Option<String>, Option<Vec<u8>>);

fn lookup_request(conn: &Connection, rid: &str) -> DbResult<Option<RequestRow>> {
    conn.query_row(
        "SELECT raft_index, tag, fingerprint, result FROM _jamsil_requests WHERE request_id = ?1",
        [rid],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )
    .optional()
    .map_err(map_err)
}

fn reuse_error(rid: &str) -> DbError {
    DbError::new(
        "",
        "22023",
        format!("jamsil.request_id '{rid}' was already used for a different request"),
    )
    .with_hint("use a new request id for each logical request")
}

/// 이미 확정된 멱등 요청: 기록된 결과를 그대로 돌려준다 (중복 효과 없음).
fn replay(rid: &str, fp: &str, rec: RequestRow) -> DbResult<QueryResult> {
    let (index, tag, stored_fp, result) = rec;
    if let Some(sf) = &stored_fp {
        if sf != fp {
            return Err(reuse_error(rid));
        }
    }
    let mut r = match result.as_deref().map(bincode::deserialize::<QueryResult>) {
        Some(Ok(r)) => r,
        _ => QueryResult { tag, ..Default::default() },
    };
    r.notices.push(format!(
        "request {rid} was already committed at log index {index}; returning the recorded result"
    ));
    Ok(r)
}
