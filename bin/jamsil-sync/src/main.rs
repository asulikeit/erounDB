//! jamsil-sync: Raft 합의·로그·Snapshot·Catch-up과 쓰기 트랜잭션 조정.
//!
//! 쓰기 경로 (설계서 6.2):
//! SQL → (Follower면 Leader Sync로 전달) → Store.TxExec(미COMMIT) → Store.TxPrepare(Patchset)
//! → Raft propose (Leader fsync + Follower Durable ACK 과반수) → Store.Apply(원자 COMMIT) → 성공 응답.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use jamsil_common::config::Mode;
use jamsil_common::{Config, DbError, Id128, NodeId, QueryResult, ReplicatedCommand};
use jamsil_consensus::msg::{PeerRequest, PeerResponse};
use jamsil_consensus::{RaftConfig, RaftHandle, Role, StateMachine, Transport, TransportConfig};
use jamsil_ipc::msg::{
    ClusterStatus, PeerStatus, Statement, StoreRequest, StoreResponse, StoreResult, SyncRequest,
    SyncResponse, SyncResult,
};
use jamsil_ipc::rpc::{BoxFut, Handler, UnixClient};

type Store = Arc<UnixClient<StoreRequest, StoreResult>>;

async fn store_call(store: &Store, req: StoreRequest) -> Result<StoreResponse, DbError> {
    match store.call(&req).await {
        Ok(r) => r,
        Err(e) => Err(DbError::new(
            "",
            "57P03",
            format!("store process unavailable: {e}"),
        )),
    }
}

/// Raft 상태 머신: Store 프로세스 프록시.
struct StoreSm {
    store: Store,
}

impl StateMachine for StoreSm {
    fn apply(&self, index: u64, term: u64, data: Arc<Vec<u8>>) -> BoxFut<Result<(), DbError>> {
        let s = self.store.clone();
        Box::pin(async move {
            let cmd: ReplicatedCommand = bincode::deserialize(&data)
                .map_err(|e| DbError::internal(format!("corrupted log entry {index}: {e}")))?;
            store_call(&s, StoreRequest::Apply { index, term, cmd })
                .await
                .map(|_| ())
        })
    }
    fn applied_index(&self) -> BoxFut<Result<u64, DbError>> {
        let s = self.store.clone();
        Box::pin(async move {
            match store_call(&s, StoreRequest::Status).await? {
                StoreResponse::Status(st) => Ok(st.applied_index),
                _ => Err(DbError::internal("unexpected store response")),
            }
        })
    }
    fn create_snapshot(&self, path: PathBuf) -> BoxFut<Result<(u64, u64), DbError>> {
        let s = self.store.clone();
        Box::pin(async move {
            match store_call(
                &s,
                StoreRequest::CreateSnapshot {
                    path: path.display().to_string(),
                },
            )
            .await?
            {
                StoreResponse::Snapshot(i) => Ok((i.index, i.term)),
                _ => Err(DbError::internal("unexpected store response")),
            }
        })
    }
    fn install_snapshot(
        &self,
        path: PathBuf,
        index: u64,
        term: u64,
    ) -> BoxFut<Result<(), DbError>> {
        let s = self.store.clone();
        Box::pin(async move {
            store_call(
                &s,
                StoreRequest::InstallSnapshot {
                    path: path.display().to_string(),
                    index,
                    term,
                },
            )
            .await
            .map(|_| ())
        })
    }
    fn abort_all(&self) -> BoxFut<Result<(), DbError>> {
        let s = self.store.clone();
        Box::pin(async move { store_call(&s, StoreRequest::AbortAll).await.map(|_| ()) })
    }
}

struct Coordinator {
    cfg: Config,
    raft: RaftHandle,
    store: Store,
    transport: Arc<Transport>,
    request_timeout: Duration,
    /// 원격 노드가 시작한 트랜잭션 (노드 장애 시 정리용)
    remote_txns: parking_lot::Mutex<HashMap<Id128, (NodeId, Instant)>>,
}

impl Coordinator {
    fn is_ready_leader(&self) -> bool {
        let s = self.raft.status();
        s.role == Role::Leader && s.leader_ready
    }

    async fn wait_leader(&self, deadline: Instant) -> Result<Option<NodeId>, DbError> {
        loop {
            let s = self.raft.status();
            if s.role == Role::Leader && s.leader_ready {
                return Ok(None);
            }
            if let Some(l) = s.leader_id {
                if l != self.cfg.server.node_id {
                    return Ok(Some(l));
                }
            }
            if Instant::now() >= deadline {
                return Err(DbError::no_quorum());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// SQL 프로세스 또는 다른 노드로부터의 요청.
    async fn handle(self: &Arc<Self>, req: SyncRequest, origin: NodeId) -> SyncResult {
        match req {
            SyncRequest::Status => Ok(SyncResponse::Status(self.status().await)),
            SyncRequest::ReadIndex => {
                let deadline = Instant::now() + self.request_timeout.min(Duration::from_secs(10));
                loop {
                    match self.raft.read_index().await {
                        Ok(i) => return Ok(SyncResponse::ReadIndex { index: i }),
                        Err(e) => {
                            if Instant::now() >= deadline {
                                return Err(e);
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
            other => {
                // 쓰기 요청: Leader에서 처리
                let retryable = matches!(other, SyncRequest::Exec { .. });
                let deadline = Instant::now()
                    + Duration::from_millis((self.cfg.cluster.election_timeout_max_ms * 8).max(5_000));
                loop {
                    let target = if retryable {
                        self.wait_leader(deadline).await?
                    } else {
                        let s = self.raft.status();
                        if s.role == Role::Leader && s.leader_ready {
                            None
                        } else if let Some(l) =
                            s.leader_id.filter(|l| *l != self.cfg.server.node_id)
                        {
                            Some(l)
                        } else if matches!(
                            other,
                            SyncRequest::TxRollback { .. } | SyncRequest::SessionClosed { .. }
                        ) {
                            return Ok(SyncResponse::Ok);
                        } else {
                            return Err(DbError::no_quorum());
                        }
                    };
                    let r = match target {
                        None => self.leader_handle(other.clone(), origin).await,
                        Some(l) => self.forward(l, &other).await,
                    };
                    // 아무것도 실행하지 않은 상태에서 Leader가 아니라고 거절된 경우만 재시도
                    match &r {
                        Err(e)
                            if retryable
                                && e.code == "JDB-2002"
                                && e.message.contains("not the leader")
                                && Instant::now() < deadline =>
                        {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                        _ => return r,
                    }
                }
            }
        }
    }

    async fn forward(&self, leader: NodeId, req: &SyncRequest) -> SyncResult {
        let body = bincode::serialize(req).map_err(|e| DbError::internal(e.to_string()))?;
        match self
            .transport
            .call(leader, &PeerRequest::Forward(body), self.request_timeout)
            .await
        {
            Ok(PeerResponse::Forward(b)) => bincode::deserialize::<SyncResult>(&b)
                .map_err(|e| DbError::internal(e.to_string()))?,
            Ok(PeerResponse::Error(e)) => Err(e),
            Ok(_) => Err(DbError::internal("unexpected forward response")),
            Err(e) => {
                // 전달 후 응답 유실이면 커밋 여부를 알 수 없다.
                if matches!(req, SyncRequest::Exec { .. } | SyncRequest::TxCommit { .. })
                    && e == jamsil_ipc::RpcError::Timeout
                {
                    Err(DbError::commit_unknown(format!(
                        "leader {leader} did not answer: {e}"
                    )))
                } else {
                    Err(DbError::leader_changed(format!(
                        "cannot reach leader {leader}: {e}"
                    )))
                }
            }
        }
    }

    async fn leader_handle(self: &Arc<Self>, req: SyncRequest, origin: NodeId) -> SyncResult {
        if !self.is_ready_leader() {
            return Err(DbError::leader_changed("this node is not the leader"));
        }
        match req {
            SyncRequest::Exec {
                txn,
                stmt,
                request_id,
            } => {
                // 멱등 키 확인은 Store Writer가 소유권을 잡은 뒤 수행한다 (경쟁 없는 단일 판단 지점, RQ-01).
                let res = match self.exec(txn, stmt, request_id.clone(), None).await {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = store_call(&self.store, StoreRequest::TxAbort { txn }).await;
                        return Err(e);
                    }
                };
                self.commit(txn, request_id).await?;
                self.crash_after_ack_point();
                Ok(SyncResponse::Result(res))
            }
            SyncRequest::TxExec {
                txn,
                stmt,
                expect_applied,
            } => {
                if origin != self.cfg.server.node_id {
                    self.remote_txns
                        .lock()
                        .insert(txn, (origin, Instant::now()));
                }
                Ok(SyncResponse::Result(
                    self.exec(txn, stmt, None, expect_applied).await?,
                ))
            }
            SyncRequest::TxCommit { txn, request_id } => {
                self.remote_txns.lock().remove(&txn);
                let idx = self.commit(txn, request_id).await?;
                self.crash_after_ack_point();
                Ok(SyncResponse::Committed { index: idx })
            }
            SyncRequest::TxRollback { txn } => {
                self.remote_txns.lock().remove(&txn);
                store_call(&self.store, StoreRequest::TxAbort { txn }).await?;
                Ok(SyncResponse::Ok)
            }
            SyncRequest::SessionClosed { txns } => {
                for t in txns {
                    self.remote_txns.lock().remove(&t);
                    let _ = store_call(&self.store, StoreRequest::TxAbort { txn: t }).await;
                }
                Ok(SyncResponse::Ok)
            }
            _ => Err(DbError::internal("unexpected request")),
        }
    }

    /// T-09: 성공 응답이 나간 직후 Leader의 jamsil-sync를 죽인다 (응답 송신 시간 30ms 확보).
    fn crash_after_ack_point(&self) {
        if jamsil_common::fault::take(&self.cfg.raft_dir(), "crash_after_commit_ack") {
            std::thread::spawn(|| {
                std::thread::sleep(Duration::from_millis(30));
                jamsil_common::fault::crash("crash_after_commit_ack");
            });
        }
    }

    async fn exec(
        &self,
        txn: Id128,
        stmt: Statement,
        request_id: Option<String>,
        expect_applied: Option<u64>,
    ) -> Result<QueryResult, DbError> {
        match store_call(
            &self.store,
            StoreRequest::TxExec {
                txn,
                stmt,
                lock_timeout_ms: 0,
                request_id,
                expect_applied,
            },
        )
        .await?
        {
            StoreResponse::Result(r) => Ok(r),
            _ => Err(DbError::internal("unexpected store response")),
        }
    }

    /// Prepare → Raft Quorum → 로컬 Apply.
    async fn commit(&self, txn: Id128, request_id: Option<String>) -> Result<u64, DbError> {
        let payload =
            match store_call(&self.store, StoreRequest::TxPrepare { txn, request_id }).await {
                Ok(StoreResponse::Prepared(p)) => p,
                Ok(_) => return Err(DbError::internal("unexpected store response")),
                Err(e) => {
                    let _ = store_call(&self.store, StoreRequest::TxAbort { txn }).await;
                    return Err(e);
                }
            };
        let Some(cmd) = payload else {
            return Ok(self.raft.status().applied_index);
        };
        let data = bincode::serialize(&cmd)
            .map_err(|e| DbError::internal(e.to_string()))?;
        let t0 = Instant::now();
        let r = tokio::time::timeout(self.request_timeout, self.raft.propose(data)).await;
        let el = t0.elapsed();
        if el > Duration::from_millis(100) {
            tracing::warn!(target: "cluster", "slow commit: raft propose+apply took {} ms", el.as_millis());
        }
        match r {
            Ok(Ok(())) => Ok(self.raft.status().applied_index),
            Ok(Err(e)) => {
                // 로그에 기록되지 않았거나(강등) 결과 불명. Prepared 트랜잭션은 정리하고,
                // 이후 같은 엔트리가 확정되면 Store가 Patchset으로 적용한다.
                let _ = store_call(&self.store, StoreRequest::TxAbort { txn }).await;
                Err(e)
            }
            Err(_) => {
                let _ = store_call(&self.store, StoreRequest::TxAbort { txn }).await;
                Err(DbError::commit_unknown("timed out waiting for quorum"))
            }
        }
    }

    async fn status(&self) -> ClusterStatus {
        let s = self
            .raft
            .fresh_status()
            .await
            .unwrap_or_else(|| (*self.raft.status()).clone());
        let store_applied = match store_call(&self.store, StoreRequest::Status).await {
            Ok(StoreResponse::Status(st)) => st.applied_index,
            _ => 0,
        };
        ClusterStatus {
            node_id: s.node_id,
            cluster_id: self.cfg.cluster.cluster_id.clone(),
            role: s.role.as_str().to_string(),
            term: s.term,
            leader_id: s.leader_id,
            commit_index: s.commit_index,
            last_log_index: s.last_log_index,
            snapshot_index: s.snapshot_index,
            store_applied_index: store_applied,
            quorum_available: s.quorum_available,
            leader_ready: s.leader_ready,
            fsync_errors: s.fsync_errors,
            peers: s
                .peers
                .iter()
                .map(|p| PeerStatus {
                    node_id: p.node_id,
                    address: self
                        .cfg
                        .peer_address(p.node_id)
                        .unwrap_or_default()
                        .to_string(),
                    connected: p.connected,
                    match_index: p.match_index,
                    next_index: p.next_index,
                    last_contact_ms: p.last_contact_ms,
                })
                .collect(),
            metrics: {
                use jamsil_common::metrics::MetricSample as M;
                let mut m = jamsil_common::metrics::process_samples();
                m.extend(jamsil_ipc::rpc::ipc_samples());
                m.push(M::new("jamsil_raft_fsync_errors_total", "", s.fsync_errors as f64, "count"));
                if let Some(free) = jamsil_common::metrics::disk_free_bytes(&self.cfg.raft_dir()) {
                    m.push(M::new("jamsil_raft_disk_free_bytes", "", free as f64, "bytes"));
                }
                m
            },
        }
    }

    /// 원격 노드가 소유한 유휴 트랜잭션 정리 (전달 노드 장애 대비).
    async fn reap_remote(self: Arc<Self>) {
        let idle = Duration::from_millis(self.cfg.storage.idle_tx_timeout_ms);
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let stale: Vec<Id128> = {
                let mut m = self.remote_txns.lock();
                let now = Instant::now();
                let v: Vec<Id128> = m
                    .iter()
                    .filter(|(_, (_, t))| now.duration_since(*t) > idle)
                    .map(|(k, _)| *k)
                    .collect();
                for k in &v {
                    m.remove(k);
                }
                v
            };
            for t in stale {
                let _ = store_call(&self.store, StoreRequest::TxAbort { txn: t }).await;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let cfg = jamsil_common::cli::load_config_or_exit("jamsil-sync");
    let _g = jamsil_observability::init("jamsil-sync", Some(&cfg));
    let store: Store = Arc::new(UnixClient::new(cfg.store_socket()));

    let peers: HashMap<NodeId, String> = match cfg.server.mode {
        Mode::Cluster => cfg
            .cluster
            .peers
            .iter()
            .map(|p| (p.node_id, p.address.clone()))
            .collect(),
        Mode::Standalone => HashMap::new(),
    };
    if cfg.server.mode == Mode::Cluster && cfg.security.cluster_secret.is_none() {
        tracing::warn!("security.cluster_secret is not set: cluster port 9787 accepts any node that knows the cluster id");
    }
    let transport = Transport::new(TransportConfig {
        node_id: cfg.server.node_id,
        cluster_id: cfg.cluster.cluster_id.clone(),
        secret: cfg.security.cluster_secret.clone(),
        peers,
        frame_size: cfg.frame_bytes(),
        connect_timeout: Duration::from_secs(3),
    });
    let rcfg = RaftConfig {
        node_id: cfg.server.node_id,
        voters: cfg.voters(),
        data_dir: cfg.raft_dir(),
        heartbeat: Duration::from_millis(cfg.cluster.heartbeat_ms),
        election_min: Duration::from_millis(cfg.cluster.election_timeout_min_ms),
        election_max: Duration::from_millis(cfg.cluster.election_timeout_max_ms),
        max_append_bytes: cfg.cluster.max_append_bytes_mib * 1024 * 1024,
        snapshot_threshold: cfg.cluster.snapshot_threshold_entries,
        snapshot_keep: cfg.cluster.snapshot_keep_entries,
        rpc_timeout: Duration::from_millis(cfg.cluster.heartbeat_ms * 4)
            .max(Duration::from_millis(500)),
        snapshot_chunk: cfg.frame_bytes(),
    };
    let raft = match jamsil_consensus::start(
        rcfg,
        transport.clone(),
        Arc::new(StoreSm {
            store: store.clone(),
        }),
    ) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("raft start failed: {e}");
            std::process::exit(jamsil_common::cli::EXIT_PERMANENT);
        }
    };
    let coord = Arc::new(Coordinator {
        request_timeout: Duration::from_millis(cfg.cluster.request_timeout_ms),
        cfg: cfg.clone(),
        raft: raft.clone(),
        store,
        transport: transport.clone(),
        remote_txns: parking_lot::Mutex::new(HashMap::new()),
    });
    tokio::spawn(coord.clone().reap_remote());

    if cfg.server.mode == Mode::Cluster {
        let bind: SocketAddr = match cfg.network.cluster_bind.parse() {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("invalid network.cluster_bind: {e}");
                std::process::exit(jamsil_common::cli::EXIT_PERMANENT);
            }
        };
        let r = raft.clone();
        let c = coord.clone();
        let t = transport.clone();
        tokio::spawn(async move {
            let h = Arc::new(
                move |from: NodeId, req: PeerRequest| -> BoxFut<PeerResponse> {
                    let r = r.clone();
                    let c = c.clone();
                    Box::pin(async move {
                        match req {
                            PeerRequest::Forward(b) => {
                                let resp = match bincode::deserialize::<SyncRequest>(&b) {
                                    Ok(req) => c.handle(req, from).await,
                                    Err(e) => Err(DbError::protocol(e.to_string())),
                                };
                                PeerResponse::Forward(bincode::serialize(&resp).unwrap_or_default())
                            }
                            other => r.handle_peer(from, other).await,
                        }
                    })
                },
            );
            if let Err(e) = t.serve(bind, h).await {
                tracing::error!("cluster listener failed on {bind}: {e}");
                // 이 노드에 없는 IP(AddrNotAvailable)·잘못된 주소는 재시작해도 해결되지 않는 설정 오류다.
                let permanent = matches!(e.kind(), std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::InvalidInput | std::io::ErrorKind::PermissionDenied);
                if permanent {
                    eprintln!("jamsil-sync: cannot listen on network.cluster_bind {bind}: {e} (check that this address belongs to this host)");
                }
                std::process::exit(if permanent { jamsil_common::cli::EXIT_PERMANENT } else { 1 });
            }
        });
    }

    // 장애 주입 (테스트 전용): JAMSIL_FAULT_INJECTION=1 일 때 sync.sock의 Status 외 요청과 함께
    // 파일 run_dir/partition 에 적힌 노드 ID들과 통신을 차단한다.
    if std::env::var("JAMSIL_FAULT_INJECTION").ok().as_deref() == Some("1") {
        let t = transport.clone();
        let path = cfg.run_dir().join("partition");
        tokio::spawn(async move {
            let mut last = String::new();
            loop {
                let cur = std::fs::read_to_string(&path).unwrap_or_default();
                if cur != last {
                    let ids: Vec<NodeId> = cur
                        .split(|c: char| !c.is_ascii_digit())
                        .filter_map(|s| s.parse().ok())
                        .collect();
                    tracing::warn!("fault injection: blocking peers {ids:?}");
                    t.set_blocked(&ids);
                    last = cur;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
    }

    let me = cfg.server.node_id;
    let c2 = coord.clone();
    let handler: Handler<SyncRequest, SyncResult> = Arc::new(move |req| {
        let c = c2.clone();
        Box::pin(async move { c.handle(req, me).await })
    });
    let sock = cfg.sync_socket();
    tracing::info!("jamsil-sync node {me} serving {}", sock.display());
    tokio::select! {
        r = jamsil_ipc::serve_unix(&sock, handler) => {
            if let Err(e) = r {
                tracing::error!("sync socket error: {e}");
                std::process::exit(1);
            }
        }
        _ = shutdown_signal() => {
            tracing::info!("jamsil-sync shutting down");
        }
    }
    let _ = std::fs::remove_file(&sock);
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut t = signal(SignalKind::terminate()).unwrap();
    let mut i = signal(SignalKind::interrupt()).unwrap();
    tokio::select! { _ = t.recv() => {}, _ = i.recv() => {} }
}
