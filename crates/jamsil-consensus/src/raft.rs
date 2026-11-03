//! Raft 합의 코어 (설계서 6장).
//!
//! * 단일 이벤트 루프 태스크가 Vote/Log 영속 상태를 소유한다.
//! * Leader는 로컬 fsync + Follower Durable ACK로 과반수를 확인한 뒤 commit_index를 올린다.
//! * Applier 태스크가 확정 로그를 순서대로 Store에 적용한다 (Leader의 성공 응답은 로컬 적용 후).
//! * PreVote + Leader Lease 검사로 분리됐던 노드가 재합류할 때 불필요한 선출을 막는다.
//! * Linearizable Read는 ReadIndex(현재 임기 commit + 과반수 Heartbeat 확인)로 제공한다.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::Rng;
use tokio::sync::{mpsc, oneshot, watch};

use jamsil_common::{DbError, NodeId};
use jamsil_ipc::rpc::{BoxFut, RpcError};

use crate::msg::{PeerRequest, PeerResponse, WireEntry};
use crate::storage::{Entry, HardState, RaftStorage, SnapshotMeta};
use crate::transport::Transport;

/// 확정 로그를 적용하는 상태 머신 (jamsil-store 프록시).
pub trait StateMachine: Send + Sync + 'static {
    fn apply(&self, index: u64, term: u64, data: Arc<Vec<u8>>) -> BoxFut<Result<(), DbError>>;
    fn applied_index(&self) -> BoxFut<Result<u64, DbError>>;
    /// 일관된 Snapshot 파일을 path에 만들고 (index, term) 반환.
    fn create_snapshot(&self, path: PathBuf) -> BoxFut<Result<(u64, u64), DbError>>;
    fn install_snapshot(&self, path: PathBuf, index: u64, term: u64)
        -> BoxFut<Result<(), DbError>>;
    /// 미확정 트랜잭션 전부 중단 (Leader 교체 시).
    fn abort_all(&self) -> BoxFut<Result<(), DbError>>;
}


/// 한 번에 Applier로 넘기는 커밋 엔트리 총 바이트 (RQ-11 Raft Apply 대기 예산).
const APPLY_BATCH_BYTES: usize = 64 * 1024 * 1024;
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub node_id: NodeId,
    pub voters: Vec<NodeId>,
    pub data_dir: PathBuf,
    pub heartbeat: Duration,
    pub election_min: Duration,
    pub election_max: Duration,
    pub max_append_bytes: usize,
    pub snapshot_threshold: u64,
    pub snapshot_keep: u64,
    pub rpc_timeout: Duration,
    pub snapshot_chunk: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Follower => "follower",
            Role::PreCandidate => "pre-candidate",
            Role::Candidate => "candidate",
            Role::Leader => "leader",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct PeerProgress {
    pub node_id: NodeId,
    pub match_index: u64,
    pub next_index: u64,
    pub last_contact_ms: Option<u64>,
    pub connected: bool,
}

#[derive(Debug, Clone)]
pub struct RaftStatus {
    pub node_id: NodeId,
    pub role: Role,
    pub term: u64,
    pub leader_id: Option<NodeId>,
    pub commit_index: u64,
    pub last_log_index: u64,
    pub applied_index: u64,
    pub snapshot_index: u64,
    pub leader_ready: bool,
    pub quorum_available: bool,
    pub fsync_errors: u64,
    pub peers: Vec<PeerProgress>,
}

enum Event {
    Peer {
        from: NodeId,
        req: PeerRequest,
        reply: oneshot::Sender<PeerResponse>,
    },
    AppendResult {
        peer: NodeId,
        term: u64,
        last_sent: u64,
        heartbeat: bool,
        resp: Result<PeerResponse, RpcError>,
    },
    VoteResult {
        peer: NodeId,
        term: u64,
        pre_vote: bool,
        resp: Result<PeerResponse, RpcError>,
    },
    SnapshotResult {
        peer: NodeId,
        term: u64,
        snap_index: u64,
        offset: u64,
        done: bool,
        resp: Result<PeerResponse, RpcError>,
    },
    Propose {
        data: Vec<u8>,
        reply: oneshot::Sender<Result<(), DbError>>,
    },
    ReadIndex {
        reply: oneshot::Sender<Result<u64, DbError>>,
    },
    Applied {
        index: u64,
        term: u64,
        result: Result<(), DbError>,
    },
    SnapshotCreated {
        meta: SnapshotMeta,
    },
    SnapshotInstalled {
        index: u64,
        term: u64,
        result: Result<(), DbError>,
    },
    ApplierReady {
        applied: u64,
    },
    /// Leader의 비동기 로그 fsync 완료 (upto까지 durable).
    SyncDone {
        upto: u64,
        epoch: u64,
        result: Result<(), String>,
    },
    Status {
        reply: oneshot::Sender<RaftStatus>,
    },
    Tick,
}

enum ApplyCmd {
    Entries(Vec<Entry>),
    AbortAll,
    InstallSnapshot {
        path: PathBuf,
        index: u64,
        term: u64,
    },
    MaybeSnapshot {
        snapshot_index: u64,
    },
}

struct Peer {
    next_index: u64,
    match_index: u64,
    inflight: bool,
    /// 데이터 전송과 별도로 진행 중인 Heartbeat
    hb_inflight: bool,
    last_hb: Instant,
    last_sent: Instant,
    last_ack: Option<Instant>,
    /// 진행 중인 Snapshot 전송 오프셋 (index, offset)
    snapshot: Option<(u64, u64)>,
}

struct PendingRead {
    index: u64,
    round: u64,
    acks: HashSet<NodeId>,
    reply: oneshot::Sender<Result<u64, DbError>>,
    deadline: Instant,
}

struct Core {
    cfg: RaftConfig,
    st: RaftStorage,
    transport: Arc<Transport>,
    tx: mpsc::Sender<Event>,
    apply_tx: mpsc::UnboundedSender<ApplyCmd>,
    role: Role,
    leader_id: Option<NodeId>,
    commit_index: u64,
    applied_index: u64,
    apply_sent: u64,
    applier_ready: bool,
    election_deadline: Instant,
    last_leader_contact: Option<Instant>,
    votes: HashSet<NodeId>,
    peers: HashMap<NodeId, Peer>,
    /// Ping 응답 시각 (Check-quorum 생존 판단 전용)
    ping_acks: Arc<parking_lot::Mutex<HashMap<NodeId, Instant>>>,
    live: Arc<Liveness>,
    /// 이 Leader 임기의 첫 엔트리 (Noop) index. 적용되면 leader_ready.
    leader_noop: Option<u64>,
    proposals: BTreeMap<u64, (u64, oneshot::Sender<Result<(), DbError>>)>,
    reads: Vec<PendingRead>,
    /// noop 적용 전 들어온 Read 요청
    deferred_reads: Vec<oneshot::Sender<Result<u64, DbError>>>,
    round: u64,
    need_sync: bool,
    /// Leader 비동기 fsync 진행 중 / 진행 중에 새로 기록되어 다시 해야 함
    sync_inflight: bool,
    sync_again: bool,
    /// 로그가 잘리거나 Snapshot으로 교체될 때 증가 (그 전에 시작한 fsync 결과 무시)
    log_epoch: u64,
    /// fsync 완료된 마지막 로그 index (Leader 자신의 match index).
    durable_index: u64,
    propose_times: HashMap<u64, Instant>,
    commit_times: HashMap<u64, Instant>,
    fsync_errors: u64,
    status_tx: watch::Sender<Arc<RaftStatus>>,
    /// 진행 중인 Snapshot 수신 (index, term, file, 다음 offset, reply)
    incoming: Option<(u64, u64, std::fs::File, u64)>,
    installing: Option<oneshot::Sender<PeerResponse>>,
    broken: Option<String>,
}

/// Raft 코어와 독립된 생존 정보 (Ping).
struct Liveness {
    epoch: Instant,
    /// 이 노드 코어가 마지막으로 진행한 시각 (epoch 기준 ms)
    core_ms: AtomicU64,
    /// Follower: 마지막으로 유효한 Ping을 보낸 Leader (node, term, 수신 시각)
    leader_ping: parking_lot::Mutex<Option<(NodeId, u64, Instant)>>,
    /// Leader 코어 정체가 이 시간을 넘으면 Ping을 생존 신호로 보지 않는다 (멈춘 Leader는 교체).
    stall_limit_ms: u64,
}

impl Liveness {
    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }
    fn touch(&self) {
        self.core_ms.store(self.now_ms(), std::sync::atomic::Ordering::Relaxed);
    }
    fn stall_ms(&self) -> u64 {
        self.now_ms()
            .saturating_sub(self.core_ms.load(std::sync::atomic::Ordering::Relaxed))
    }
}

#[derive(Clone)]
pub struct RaftHandle {
    tx: mpsc::Sender<Event>,
    status: watch::Receiver<Arc<RaftStatus>>,
    applied: watch::Receiver<u64>,
    live: Arc<Liveness>,
}

impl RaftHandle {
    /// 명령을 제안하고 로컬 적용까지 기다린다.
    pub async fn propose(&self, data: Vec<u8>) -> Result<(), DbError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Event::Propose { data, reply: tx })
            .await
            .map_err(|_| DbError::unavailable("raft stopped"))?;
        rx.await
            .map_err(|_| DbError::commit_unknown("raft stopped"))?
    }

    /// Linearizable Read Index (Follower면 Leader에게 질의).
    pub async fn read_index(&self) -> Result<u64, DbError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Event::ReadIndex { reply: tx })
            .await
            .map_err(|_| DbError::unavailable("raft stopped"))?;
        rx.await.map_err(|_| DbError::unavailable("raft stopped"))?
    }

    pub async fn handle_peer(&self, from: NodeId, req: PeerRequest) -> PeerResponse {
        if let PeerRequest::Ping { term, core_stall_ms } = req {
            if core_stall_ms < self.live.stall_limit_ms {
                *self.live.leader_ping.lock() = Some((from, term, Instant::now()));
            }
            return PeerResponse::Pong {
                term: self.status.borrow().term,
            };
        }
        let (tx, rx) = oneshot::channel();
        if self
            .tx
            .send(Event::Peer {
                from,
                req,
                reply: tx,
            })
            .await
            .is_err()
        {
            return PeerResponse::Error(DbError::unavailable("raft stopped"));
        }
        rx.await
            .unwrap_or_else(|_| PeerResponse::Error(DbError::unavailable("raft dropped request")))
    }

    pub fn status(&self) -> Arc<RaftStatus> {
        self.status.borrow().clone()
    }

    pub fn status_watch(&self) -> watch::Receiver<Arc<RaftStatus>> {
        self.status.clone()
    }

    pub fn applied_watch(&self) -> watch::Receiver<u64> {
        self.applied.clone()
    }

    pub async fn fresh_status(&self) -> Option<RaftStatus> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Event::Status { reply: tx }).await.ok()?;
        rx.await.ok()
    }
}

pub fn start(
    cfg: RaftConfig,
    transport: Arc<Transport>,
    sm: Arc<dyn StateMachine>,
) -> Result<RaftHandle, DbError> {
    let st =
        RaftStorage::open(&cfg.data_dir).map_err(|e| DbError::io(format!("raft storage: {e}")))?;
    tracing::info!(
        "raft storage recovered: term={} voted_for={} log=[{}..{}] snapshot={}/{}",
        st.hard.term,
        st.hard.voted_for,
        st.first_index(),
        st.last_index(),
        st.snapshot.index,
        st.snapshot.term
    );
    let (tx, rx) = mpsc::channel(4096);
    let (apply_tx, apply_rx) = mpsc::unbounded_channel();
    let (applied_tx, applied_rx) = watch::channel(0u64);
    let snap_index = st.snapshot.index;
    let initial = Arc::new(RaftStatus {
        node_id: cfg.node_id,
        role: Role::Follower,
        term: st.hard.term,
        leader_id: None,
        commit_index: snap_index,
        last_log_index: st.last_index(),
        applied_index: 0,
        snapshot_index: snap_index,
        leader_ready: false,
        quorum_available: false,
        fsync_errors: 0,
        peers: vec![],
    });
    let (status_tx, status_rx) = watch::channel(initial);
    let peers = cfg
        .voters
        .iter()
        .filter(|v| **v != cfg.node_id)
        .map(|v| {
            (
                *v,
                Peer {
                    next_index: 1,
                    match_index: 0,
                    inflight: false,
                    hb_inflight: false,
                    last_hb: Instant::now(),
                    last_sent: Instant::now(),
                    last_ack: None,
                    snapshot: None,
                },
            )
        })
        .collect();
    let mut core = Core {
        election_deadline: Instant::now() + cfg.election_max,
        cfg: cfg.clone(),
        st,
        transport,
        tx: tx.clone(),
        apply_tx,
        role: Role::Follower,
        leader_id: None,
        commit_index: snap_index,
        applied_index: snap_index,
        apply_sent: snap_index,
        applier_ready: false,
        last_leader_contact: None,
        votes: HashSet::new(),
        peers,
        leader_noop: None,
        proposals: BTreeMap::new(),
        reads: vec![],
        deferred_reads: vec![],
        round: 0,
        need_sync: false,
        ping_acks: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        live: Arc::new(Liveness {
            epoch: Instant::now(),
            core_ms: AtomicU64::new(0),
            leader_ping: parking_lot::Mutex::new(None),
            stall_limit_ms: (cfg.election_max * 4).as_millis() as u64,
        }),
        sync_inflight: false,
        sync_again: false,
        log_epoch: 0,
        durable_index: 0,
        propose_times: HashMap::new(),
        commit_times: HashMap::new(),
        fsync_errors: 0,
        status_tx,
        incoming: None,
        installing: None,
        broken: None,
    };
    core.durable_index = core.st.last_index();
    core.reset_election_timer();
    // Applier
    let snapshot_meta = core.st.snapshot.clone();
    let snap_path = core.st.snapshot_path();
    tokio::spawn(applier(
        sm,
        apply_rx,
        tx.clone(),
        applied_tx,
        cfg.clone(),
        snapshot_meta,
        snap_path,
    ));
    // Ticker
    let ttx = tx.clone();
    let tick = (cfg.heartbeat / 3).max(Duration::from_millis(10));
    tokio::spawn(async move {
        let mut iv = tokio::time::interval(tick);
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            iv.tick().await;
            if ttx.send(Event::Tick).await.is_err() {
                break;
            }
        }
    });
    let live = core.live.clone();
    core.live.touch();
    jamsil_common::metrics::histogram("jamsil_raft_fsync_duration_seconds", "", "seconds");
    // Leader 생존 확인 Ping (Check-quorum, 코어와 독립)
    {
        let status = core.status_tx.subscribe();
        let acks = core.ping_acks.clone();
        let live = core.live.clone();
        let transport = core.transport.clone();
        let peers: Vec<NodeId> = core.peers.keys().copied().collect();
        let hb = cfg.heartbeat;
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(hb);
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                iv.tick().await;
                let s = status.borrow().clone();
                if s.role != Role::Leader {
                    continue;
                }
                for &p in &peers {
                    let t = transport.clone();
                    let acks = acks.clone();
                    let term = s.term;
                    let core_stall_ms = live.stall_ms();
                    tokio::spawn(async move {
                        let sent = Instant::now();
                        if let Ok(PeerResponse::Pong { term: pt }) = t
                            .call(p, &PeerRequest::Ping { term, core_stall_ms }, hb * 4)
                            .await
                        {
                            if pt <= term {
                                acks.lock().insert(p, sent);
                            }
                        }
                    });
                }
            }
        });
    }
    tokio::spawn(core.run(rx));
    Ok(RaftHandle {
        tx,
        status: status_rx,
        applied: applied_rx,
        live,
    })
}

async fn applier(
    sm: Arc<dyn StateMachine>,
    mut rx: mpsc::UnboundedReceiver<ApplyCmd>,
    core: mpsc::Sender<Event>,
    applied_tx: watch::Sender<u64>,
    cfg: RaftConfig,
    snapshot: SnapshotMeta,
    snap_path: Option<PathBuf>,
) {
    // 기동: Store applied index 확인, 필요하면 로컬 Snapshot부터 설치
    let mut applied = loop {
        match sm.applied_index().await {
            Ok(a) => break a,
            Err(e) => {
                tracing::warn!("waiting for store: {e}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    };
    if applied < snapshot.index {
        if let Some(p) = snap_path {
            tracing::info!(
                "store applied {applied} < snapshot {}: installing local snapshot",
                snapshot.index
            );
            loop {
                match sm
                    .install_snapshot(p.clone(), snapshot.index, snapshot.term)
                    .await
                {
                    Ok(()) => {
                        applied = snapshot.index;
                        break;
                    }
                    Err(e) => {
                        tracing::error!("local snapshot install failed: {e}");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        }
    }
    applied_tx.send_replace(applied);
    let _ = core.send(Event::ApplierReady { applied }).await;
    let mut last_snapshot = snapshot.index;
    while let Some(cmd) = rx.recv().await {
        match cmd {
            ApplyCmd::Entries(entries) => {
                for e in entries {
                    if e.index <= applied {
                        continue;
                    }
                    let mut attempt = 0u32;
                    let result = loop {
                        let r = sm.apply(e.index, e.term, e.data.clone()).await;
                        match &r {
                            Err(err)
                                if err.sqlstate == "57P03"
                                    && err.code.is_empty()
                                    && attempt < 50 =>
                            {
                                // Store 프로세스 재시작 중 등 일시 장애: 재시도
                                attempt += 1;
                                tokio::time::sleep(Duration::from_millis(200)).await;
                                continue;
                            }
                            _ => break r,
                        }
                    };
                    let ok = result.is_ok();
                    if ok {
                        applied = e.index;
                        applied_tx.send_replace(applied);
                    }
                    let _ = core
                        .send(Event::Applied {
                            index: e.index,
                            term: e.term,
                            result,
                        })
                        .await;
                    if !ok {
                        // 적용 실패: 이후 엔트리도 적용 불가. Core가 상태를 판단한다.
                        break;
                    }
                }
            }
            ApplyCmd::AbortAll => {
                if let Err(e) = sm.abort_all().await {
                    tracing::warn!("abort_all failed: {e}");
                }
            }
            ApplyCmd::InstallSnapshot { path, index, term } => {
                let r = sm.install_snapshot(path, index, term).await;
                if r.is_ok() {
                    applied = index;
                    last_snapshot = index;
                    applied_tx.send_replace(applied);
                }
                let _ = core
                    .send(Event::SnapshotInstalled {
                        index,
                        term,
                        result: r,
                    })
                    .await;
            }
            ApplyCmd::MaybeSnapshot { snapshot_index } => {
                last_snapshot = last_snapshot.max(snapshot_index);
                if applied.saturating_sub(last_snapshot) < cfg.snapshot_threshold {
                    continue;
                }
                let dir = cfg.data_dir.join("snapshots");
                let tmp = dir.join(format!("creating-{}.sqlite", std::process::id()));
                match sm.create_snapshot(tmp.clone()).await {
                    Ok((index, term)) => {
                        let file = format!("snap-{index:020}-{term}.sqlite");
                        let dst = dir.join(&file);
                        if let Err(e) = std::fs::rename(&tmp, &dst) {
                            tracing::error!("snapshot rename failed: {e}");
                            continue;
                        }
                        let size = std::fs::metadata(&dst).map(|m| m.len()).unwrap_or(0);
                        last_snapshot = index;
                        let _ = core
                            .send(Event::SnapshotCreated {
                                meta: SnapshotMeta {
                                    index,
                                    term,
                                    file,
                                    size,
                                },
                            })
                            .await;
                    }
                    Err(e) => {
                        tracing::warn!("snapshot creation failed: {e}");
                        let _ = std::fs::remove_file(&tmp);
                    }
                }
            }
        }
    }
}

fn quorum(n: usize) -> usize {
    n / 2 + 1
}

impl Core {
    fn reset_election_timer(&mut self) {
        let min = self.cfg.election_min.as_millis() as u64;
        let max = self.cfg.election_max.as_millis() as u64;
        let ms = rand::thread_rng().gen_range(min..=max);
        self.election_deadline = Instant::now() + Duration::from_millis(ms);
    }

    fn voters(&self) -> usize {
        self.cfg.voters.len()
    }

    fn is_standalone(&self) -> bool {
        self.cfg.voters.len() == 1
    }

    fn fatal(&mut self, what: String) -> ! {
        tracing::error!("FATAL raft storage error: {what}");
        self.fsync_errors += 1;
        self.broken = Some(what.clone());
        // 내구성을 보장할 수 없으므로 프로세스를 종료하고 재기동 복구에 맡긴다.
        std::process::exit(75);
    }

    fn start_leader_sync(&mut self) {
        if self.sync_inflight {
            self.sync_again = true;
            return;
        }
        let fault_dir = self.st.dir().to_path_buf();
        if jamsil_common::fault::take(&fault_dir, "crash_before_raft_fsync") {
            jamsil_common::fault::crash("crash_before_raft_fsync");
        }
        let upto = self.st.last_index();
        let f = match self.st.begin_async_sync() {
            Ok(Some(f)) => f,
            Ok(None) => {
                self.durable_index = upto;
                self.advance_commit();
                return;
            }
            Err(e) => self.fatal(format!("fsync failed: {e}")),
        };
        self.sync_inflight = true;
        let epoch = self.log_epoch;
        let tx = self.tx.clone();
        let fail = jamsil_common::fault::active(&fault_dir, "fail_raft_fsync");
        tokio::task::spawn_blocking(move || {
            let t0 = Instant::now();
            let r = if fail {
                Err("injected fsync failure (EIO)".to_string())
            } else {
                f.sync_data().map_err(|e| e.to_string())
            };
            let el = t0.elapsed();
            jamsil_common::metrics::histogram("jamsil_raft_fsync_duration_seconds", "", "seconds")
                .record_duration(el);
            if el > Duration::from_millis(50) {
                tracing::warn!("slow raft log fsync: {} ms", el.as_millis());
            }
            let _ = tx.blocking_send(Event::SyncDone {
                upto,
                epoch,
                result: r,
            });
        });
    }

    fn on_sync_done(&mut self, upto: u64, epoch: u64, result: Result<(), String>) {
        self.sync_inflight = false;
        if let Err(e) = result {
            self.fatal(format!("fsync failed: {e}"));
        }
        if epoch == self.log_epoch {
            self.durable_index = self.durable_index.max(upto.min(self.st.last_index()));
        }
        if self.sync_again {
            self.sync_again = false;
            self.start_leader_sync();
        }
        if self.role == Role::Leader {
            self.advance_commit();
        }
    }

    fn sync(&mut self) {
        let fault_dir = self.st.dir().to_path_buf();
        if jamsil_common::fault::take(&fault_dir, "crash_before_raft_fsync") {
            // T-06: 로그는 기록됐지만 fsync 전 — 이 엔트리는 어떤 노드에서도 성공 응답을 받으면 안 된다.
            jamsil_common::fault::crash("crash_before_raft_fsync");
        }
        let t0 = Instant::now();
        let r = if jamsil_common::fault::active(&fault_dir, "fail_raft_fsync") {
            Err(std::io::Error::other("injected fsync failure (EIO)"))
        } else if self.sync_inflight || self.durable_index < self.st.last_index() {
            // Leader 시절 시작한 비동기 fsync가 끝나지 않았을 수 있다: dirty 표시와 무관하게 동기화
            self.st.sync_force()
        } else {
            self.st.sync()
        };
        let el = t0.elapsed();
        jamsil_common::metrics::histogram("jamsil_raft_fsync_duration_seconds", "", "seconds")
            .record_duration(el);
        if el > Duration::from_millis(50) {
            tracing::warn!("slow raft log fsync: {} ms", el.as_millis());
        }
        if let Err(e) = r {
            self.fatal(format!("fsync failed: {e}"));
        }
        self.durable_index = self.st.last_index();
    }

    fn persist_hard(&mut self, hs: HardState) {
        if let Err(e) = self.st.save_hard_state(hs) {
            self.fatal(format!("vote state write failed: {e}"));
        }
    }

    fn term(&self) -> u64 {
        self.st.hard.term
    }

    async fn run(mut self, mut rx: mpsc::Receiver<Event>) {
        loop {
            let Some(first) = rx.recv().await else { break };
            self.live.touch();
            let started = Instant::now();
            // Tick은 모아서 배치의 맨 마지막에 한 번만 처리한다. 대형 Entry 처리로 코어가 멈춰 있는 동안
            // 큐에 쌓인 Heartbeat/ACK보다 Tick이 먼저 처리되어 "리더가 죽었다"고 오판하는 것을 막는다.
            let mut tick = false;
            match first {
                Event::Tick => tick = true,
                ev => self.handle(ev),
            }
            // 그룹 커밋: 쌓인 이벤트를 더 처리한 뒤 한 번만 fsync
            let mut n = 0;
            while n < 1024 {
                match rx.try_recv() {
                    Ok(Event::Tick) => tick = true,
                    Ok(ev) => {
                        self.handle(ev);
                        self.live.touch();
                        n += 1;
                    }
                    Err(_) => break,
                }
            }
            if self.need_sync {
                self.need_sync = false;
                if self.role == Role::Leader {
                    // Leader: 로컬 fsync는 별도 스레드에서 진행하고, 그동안 Follower 복제와 Heartbeat를
                    // 계속한다 (대형 Entry fsync가 코어를 막아 선거가 일어나는 것을 방지). Leader 자신의
                    // match는 fsync 완료(SyncDone) 후에만 durable_index로 반영된다.
                    self.start_leader_sync();
                    self.replicate_all(false);
                } else {
                    self.sync();
                }
            }
            let busy = started.elapsed();
            if busy > self.cfg.heartbeat * 3 {
                tracing::warn!("raft core was busy for {} ms (large entry or slow disk)", busy.as_millis());
                // 코어 자신이 멈춰 있었으므로 그동안 Heartbeat를 못 받은 것을 리더 장애로 보지 않는다.
                if self.role != Role::Leader {
                    self.reset_election_timer();
                }
            }
            if tick {
                self.tick();
            }
            self.publish_status();
        }
    }

    fn publish_status(&self) {
        let now = Instant::now();
        let quorum_available = match self.role {
            Role::Leader => self.has_quorum_contact(now),
            _ => {
                self.is_standalone()
                    || self
                        .last_leader_contact
                        .map(|t| now.duration_since(t) < self.cfg.election_max)
                        .unwrap_or(false)
            }
        };
        let s = RaftStatus {
            node_id: self.cfg.node_id,
            role: self.role,
            term: self.term(),
            leader_id: self.leader_id,
            commit_index: self.commit_index,
            last_log_index: self.st.last_index(),
            applied_index: self.applied_index,
            snapshot_index: self.st.snapshot.index,
            leader_ready: self.role == Role::Leader
                && self.leader_noop.is_none()
                && self.applier_ready,
            quorum_available,
            fsync_errors: self.fsync_errors,
            peers: self
                .peers
                .iter()
                .map(|(id, p)| PeerProgress {
                    node_id: *id,
                    match_index: p.match_index,
                    next_index: p.next_index,
                    last_contact_ms: p.last_ack.map(|t| now.duration_since(t).as_millis() as u64),
                    connected: self.transport.is_connected(*id),
                })
                .collect(),
        };
        self.status_tx.send_replace(Arc::new(s));
    }

    fn has_quorum_contact(&self, now: Instant) -> bool {
        let pings = self.ping_acks.lock();
        let n = 1 + self
            .peers
            .iter()
            .filter(|(id, p)| {
                let ping = pings.get(id).copied();
                p.last_ack
                    .max(ping)
                    .map(|t| now.duration_since(t) < self.cfg.election_max)
                    .unwrap_or(false)
            })
            .count();
        n >= quorum(self.voters())
    }

    fn handle(&mut self, ev: Event) {
        match ev {
            Event::Tick => self.tick(),
            Event::Peer { from, req, reply } => self.on_peer(from, req, reply),
            Event::AppendResult {
                peer,
                term,
                last_sent,
                heartbeat,
                resp,
            } => self.on_append_result(peer, term, last_sent, heartbeat, resp),
            Event::VoteResult {
                peer,
                term,
                pre_vote,
                resp,
            } => self.on_vote_result(peer, term, pre_vote, resp),
            Event::SnapshotResult {
                peer,
                term,
                snap_index,
                offset,
                done,
                resp,
            } => self.on_snapshot_result(peer, term, snap_index, offset, done, resp),
            Event::Propose { data, reply } => self.on_propose(data, reply),
            Event::ReadIndex { reply } => self.on_read_index(reply),
            Event::Applied {
                index,
                term,
                result,
            } => self.on_applied(index, term, result),
            Event::SnapshotCreated { meta } => self.on_snapshot_created(meta),
            Event::SnapshotInstalled {
                index,
                term,
                result,
            } => self.on_snapshot_installed(index, term, result),
            Event::SyncDone {
                upto,
                epoch,
                result,
            } => self.on_sync_done(upto, epoch, result),
            Event::ApplierReady { applied } => {
                self.applier_ready = true;
                self.applied_index = applied;
                self.apply_sent = applied.max(self.apply_sent);
                if self.commit_index < applied {
                    // Store에 적용된 것은 확정된 것이다.
                    self.commit_index = applied.min(self.st.last_index().max(applied));
                }
                self.send_to_applier();
            }
            Event::Status { reply } => {
                self.publish_status();
                let _ = reply.send((**self.status_tx.borrow()).clone());
            }
        }
    }

    // ------------------------------------------------------------------ timers

    fn tick(&mut self) {
        let now = Instant::now();
        match self.role {
            Role::Leader => {
                // Check-quorum: 과반수와 통신이 끊기면 강등 (Minority Leader가 쓰기를 받지 않도록)
                if !self.is_standalone() && !self.has_quorum_contact(now) {
                    let started = self.peers.values().all(|p| p.last_ack.is_none());
                    if !started || now >= self.election_deadline {
                        tracing::warn!(
                            "leader lost quorum contact; stepping down (term {})",
                            self.term()
                        );
                        self.become_follower(self.term(), None);
                        return;
                    }
                }
                self.replicate_all(true);
            }
            _ => {
                if now >= self.election_deadline && self.applier_ready && self.broken.is_none() {
                    if self.leader_alive_by_ping() {
                        // Leader 코어가 대형 Entry 처리로 느릴 뿐 살아 있다: 선거를 미룬다.
                        self.last_leader_contact = Some(now);
                        self.reset_election_timer();
                    } else {
                        self.start_election();
                    }
                }
            }
        }
        // 읽기 요청 타임아웃
        let mut i = 0;
        while i < self.reads.len() {
            if self.reads[i].deadline <= now {
                let r = self.reads.remove(i);
                let _ = r.reply.send(Err(DbError::no_quorum()));
            } else {
                i += 1;
            }
        }
    }

    // ---------------------------------------------------------------- elections

    fn start_election(&mut self) {
        if self.is_standalone() {
            let t = self.term() + 1;
            self.persist_hard(HardState {
                term: t,
                voted_for: self.cfg.node_id,
            });
            self.become_leader();
            return;
        }
        // PreVote 단계: 임기를 올리지 않고 과반수 동의 확인.
        // 실제 선거(Candidate)로 넘어가기 전까지는 알고 있던 Leader 정보를 유지한다
        // (불필요한 PreVote 중에도 이 노드로 접속한 클라이언트의 요청을 Leader로 전달할 수 있다).
        tracing::info!(
            "no heartbeat from leader {:?} for the election timeout; starting pre-vote (term {})",
            self.leader_id,
            self.term()
        );
        self.role = Role::PreCandidate;
        self.votes.clear();
        self.votes.insert(self.cfg.node_id);
        self.reset_election_timer();
        self.send_votes(true);
    }

    fn send_votes(&mut self, pre_vote: bool) {
        let term = if pre_vote {
            self.term() + 1
        } else {
            self.term()
        };
        let req = PeerRequest::RequestVote {
            term,
            candidate_id: self.cfg.node_id,
            last_log_index: self.st.last_index(),
            last_log_term: self.st.last_term(),
            pre_vote,
        };
        for &peer in self.peers.keys() {
            let t = self.transport.clone();
            let tx = self.tx.clone();
            let req = req.clone();
            let to = self.cfg.rpc_timeout;
            let my_term = self.term();
            tokio::spawn(async move {
                let resp = t.call(peer, &req, to).await;
                let _ = tx
                    .send(Event::VoteResult {
                        peer,
                        term: my_term,
                        pre_vote,
                        resp,
                    })
                    .await;
            });
        }
    }

    fn on_vote_result(
        &mut self,
        peer: NodeId,
        term: u64,
        pre_vote: bool,
        resp: Result<PeerResponse, RpcError>,
    ) {
        let Ok(PeerResponse::Vote {
            term: rterm,
            granted,
        }) = resp
        else {
            return;
        };
        if rterm > self.term() && (!pre_vote || !granted) {
            self.become_follower(rterm, None);
            return;
        }
        if term != self.term() {
            return;
        }
        let expected = if pre_vote {
            Role::PreCandidate
        } else {
            Role::Candidate
        };
        if self.role != expected || !granted {
            return;
        }
        self.votes.insert(peer);
        if self.votes.len() >= quorum(self.voters()) {
            if pre_vote {
                // 실제 선거
                let t = self.term() + 1;
                self.persist_hard(HardState {
                    term: t,
                    voted_for: self.cfg.node_id,
                });
                self.role = Role::Candidate;
                self.leader_id = None;
                self.votes.clear();
                self.votes.insert(self.cfg.node_id);
                self.reset_election_timer();
                tracing::info!("node {} starting election for term {t}", self.cfg.node_id);
                self.send_votes(false);
            } else {
                self.become_leader();
            }
        }
    }

    fn become_follower(&mut self, term: u64, leader: Option<NodeId>) {
        let was_leader = self.role == Role::Leader;
        if term > self.term() {
            self.persist_hard(HardState { term, voted_for: 0 });
        }
        if self.role != Role::Follower || self.leader_id != leader {
            if let Some(l) = leader {
                tracing::info!(
                    "node {} following leader {l} in term {term}",
                    self.cfg.node_id
                );
            }
        }
        self.role = Role::Follower;
        self.leader_id = leader;
        self.leader_noop = None;
        self.reset_election_timer();
        if was_leader {
            let _ = self.apply_tx.send(ApplyCmd::AbortAll);
            // 이미 로그에 기록된 제안은 확정 여부를 알 수 없다.
            for (_, (_, reply)) in std::mem::take(&mut self.proposals) {
                let _ = reply.send(Err(DbError::commit_unknown(
                    "leadership lost before the entry was applied",
                )));
            }
            for r in std::mem::take(&mut self.reads) {
                let _ = r
                    .reply
                    .send(Err(DbError::leader_changed("leadership lost")));
            }
            for r in std::mem::take(&mut self.deferred_reads) {
                let _ = r.send(Err(DbError::leader_changed("leadership lost")));
            }
        }
    }

    fn become_leader(&mut self) {
        tracing::info!(
            "node {} became leader for term {}",
            self.cfg.node_id,
            self.term()
        );
        self.role = Role::Leader;
        self.leader_id = Some(self.cfg.node_id);
        let last = self.st.last_index();
        let now = Instant::now();
        for p in self.peers.values_mut() {
            p.next_index = last + 1;
            p.match_index = 0;
            p.inflight = false;
            p.hb_inflight = false;
            p.snapshot = None;
            p.last_ack = None;
            p.last_sent = now - self.cfg.heartbeat;
        }
        self.reset_election_timer();
        let _ = self.apply_tx.send(ApplyCmd::AbortAll);
        // 현재 임기 확정을 위한 Noop
        let noop = bincode::serialize(&jamsil_common::ReplicatedCommand::Noop).unwrap();
        let idx = last + 1;
        if let Err(e) = self.st.append(idx, self.term(), Arc::new(noop)) {
            self.fatal(format!("append failed: {e}"));
        }
        self.leader_noop = Some(idx);
        self.need_sync = true;
    }

    // ------------------------------------------------------------- replication

    fn replicate_all(&mut self, heartbeat_only: bool) {
        if self.role != Role::Leader {
            return;
        }
        let ids: Vec<NodeId> = self.peers.keys().copied().collect();
        for id in ids {
            self.replicate(id, heartbeat_only);
        }
    }

    fn replicate(&mut self, peer: NodeId, heartbeat_only: bool) {
        let now = Instant::now();
        let last = self.st.last_index();
        let (next, inflight, last_sent) = {
            let p = &self.peers[&peer];
            (p.next_index, p.inflight, p.last_sent)
        };
        if inflight {
            // 대형 Entry 전송 중에도 Heartbeat는 별도로 보낸다 (Leader 유지·ReadIndex 확인).
            self.send_heartbeat(peer);
            return;
        }
        let has_new = next <= last;
        if heartbeat_only && !has_new && now.duration_since(last_sent) < self.cfg.heartbeat {
            return;
        }
        if !has_new
            && !heartbeat_only
            && now.duration_since(last_sent) < self.cfg.heartbeat
            && self.reads.is_empty()
        {
            return;
        }
        let prev = next - 1;
        let prev_term = match self.st.term(prev) {
            Some(t) => t,
            None => {
                self.send_snapshot(peer);
                return;
            }
        };
        let entries = if has_new {
            match self.st.entries(next, last, self.cfg.max_append_bytes) {
                Ok(e) => e,
                Err(_) => {
                    self.send_snapshot(peer);
                    return;
                }
            }
        } else {
            vec![]
        };
        let last_sent_index = entries.last().map(|e| e.index).unwrap_or(prev);
        if entries.iter().map(|e| e.data.len()).sum::<usize>() >= 8 << 20
            && jamsil_common::fault::take(self.st.dir(), "crash_mid_large_append")
        {
            // T-11: 대형 엔트리의 프레임 일부만 전송된 뒤 Leader 종료
            jamsil_ipc::frame::arm_abort_after_large_frames(4);
        }
        let req = PeerRequest::AppendEntries {
            term: self.term(),
            leader_id: self.cfg.node_id,
            prev_log_index: prev,
            prev_log_term: prev_term,
            entries: entries
                .into_iter()
                .map(|e| WireEntry {
                    index: e.index,
                    term: e.term,
                    data: (*e.data).clone(),
                })
                .collect(),
            leader_commit: self.commit_index,
            round: self.round,
        };
        let p = self.peers.get_mut(&peer).unwrap();
        p.inflight = true;
        p.last_sent = now;
        p.last_hb = now;
        let t = self.transport.clone();
        let tx = self.tx.clone();
        let term = self.term();
        // 대형 Entry는 전송 시간이 길 수 있다.
        let to = self
            .cfg
            .rpc_timeout
            .max(Duration::from_secs(if has_new { 60 } else { 0 }));
        tokio::spawn(async move {
            let resp = t.call(peer, &req, to).await;
            let _ = tx
                .send(Event::AppendResult {
                    peer,
                    term,
                    last_sent: last_sent_index,
                    heartbeat: false,
                    resp,
                })
                .await;
        });
    }

    fn send_heartbeat(&mut self, peer: NodeId) {
        let now = Instant::now();
        let (hb_inflight, last_hb, next) = {
            let p = &self.peers[&peer];
            (p.hb_inflight, p.last_hb, p.next_index)
        };
        if hb_inflight
            || (now.duration_since(last_hb) < self.cfg.heartbeat && self.reads.is_empty())
        {
            return;
        }
        let prev = next - 1;
        let Some(prev_term) = self.st.term(prev) else {
            return;
        };
        let req = PeerRequest::AppendEntries {
            term: self.term(),
            leader_id: self.cfg.node_id,
            prev_log_index: prev,
            prev_log_term: prev_term,
            entries: vec![],
            leader_commit: self
                .commit_index
                .min(prev.max(self.peers[&peer].match_index)),
            round: self.round,
        };
        let p = self.peers.get_mut(&peer).unwrap();
        p.hb_inflight = true;
        p.last_hb = now;
        let t = self.transport.clone();
        let tx = self.tx.clone();
        let term = self.term();
        let to = self.cfg.rpc_timeout;
        tokio::spawn(async move {
            let resp = t.call(peer, &req, to).await;
            let _ = tx
                .send(Event::AppendResult {
                    peer,
                    term,
                    last_sent: prev,
                    heartbeat: true,
                    resp,
                })
                .await;
        });
    }

    fn on_append_result(
        &mut self,
        peer: NodeId,
        term: u64,
        _last_sent: u64,
        heartbeat: bool,
        resp: Result<PeerResponse, RpcError>,
    ) {
        let Some(p) = self.peers.get_mut(&peer) else {
            return;
        };
        if heartbeat {
            p.hb_inflight = false;
        } else {
            p.inflight = false;
        }
        if self.role != Role::Leader || term != self.term() {
            return;
        }
        let resp = match resp {
            Ok(r) => r,
            Err(_) => {
                return;
            }
        };
        match resp {
            PeerResponse::Append {
                term: rterm,
                success,
                match_index,
                conflict_index,
                round,
            } => {
                if rterm > self.term() {
                    self.become_follower(rterm, None);
                    return;
                }
                let p = self.peers.get_mut(&peer).unwrap();
                p.last_ack = Some(Instant::now());
                if heartbeat {
                    // Heartbeat는 진행 상태를 바꾸지 않는다 (데이터 전송 결과가 결정).
                    if success && !p.inflight {
                        p.match_index = p.match_index.max(match_index);
                        p.next_index = p.next_index.max(p.match_index + 1);
                    }
                } else if success {
                    p.match_index = p.match_index.max(match_index);
                    p.next_index = p.match_index + 1;
                } else if conflict_index > 0 && conflict_index <= p.match_index {
                    // 이미 확인된 위치보다 앞에서 거부: 노드 데이터가 삭제·교체되어 로그를 잃었다
                    // (운영 복구 절차: 데이터 삭제 후 재기동). match를 낮추고 그 지점부터 다시 보낸다
                    // (필요하면 Snapshot). match를 낮추는 것은 commit을 늦출 뿐 안전성을 해치지 않는다.
                    tracing::warn!(
                        "peer {peer} lost acknowledged log (match {} -> {}); resending",
                        p.match_index,
                        conflict_index - 1
                    );
                    p.match_index = conflict_index - 1;
                    p.next_index = conflict_index;
                } else {
                    let ci = conflict_index.max(1);
                    p.next_index = ci
                        .min(p.next_index.saturating_sub(1).max(1))
                        .max(p.match_index + 1);
                }
                // ReadIndex 확인
                for r in self.reads.iter_mut() {
                    if round >= r.round {
                        r.acks.insert(peer);
                    }
                }
                self.resolve_reads();
                self.advance_commit();
                // 남은 엔트리 계속 전송
                self.replicate(peer, false);
            }
            PeerResponse::Error(e) => {
                tracing::debug!("append to {peer} failed: {e}");
            }
            _ => {}
        }
    }

    fn advance_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let mut matches: Vec<u64> = self.peers.values().map(|p| p.match_index).collect();
        matches.push(self.durable_index.min(self.st.last_index())); // 자신: fsync 완료분만
        matches.sort_unstable_by(|a, b| b.cmp(a));
        let n = matches[quorum(self.voters()) - 1];
        if n > self.commit_index && self.st.term(n) == Some(self.term()) {
            for i in self.commit_index + 1..=n {
                if let Some(t) = self.propose_times.remove(&i) {
                    let el = t.elapsed();
                    if el > Duration::from_millis(100) {
                        tracing::warn!(
                            "slow quorum: entry {i} proposed->committed took {} ms",
                            el.as_millis()
                        );
                    }
                    self.commit_times.insert(i, Instant::now());
                }
            }
            self.commit_index = n;
            self.send_to_applier();
        }
    }

    fn send_to_applier(&mut self) {
        if !self.applier_ready {
            return;
        }
        let upto = self.commit_index.min(self.st.last_index());
        if self.apply_sent >= upto {
            return;
        }
        // Apply 대기 메모리 상한 (RQ-11): 적용 중인 배치가 끝나야 다음 배치를 읽는다.
        // 대기열에는 항상 배치 하나(최대 APPLY_BATCH_BYTES, 단 Entry 하나는 그보다 클 수 있음)만 있다.
        if self.apply_sent > self.applied_index {
            return;
        }
        let from = self.apply_sent + 1;
        match self.st.entries(from, upto, APPLY_BATCH_BYTES) {
            Ok(es) => {
                if let Some(l) = es.last() {
                    self.apply_sent = l.index;
                }
                let _ = self.apply_tx.send(ApplyCmd::Entries(es));
            }
            Err(e) => {
                tracing::error!("cannot read committed entries {from}..{upto}: {e}");
            }
        }
    }

    fn on_applied(&mut self, index: u64, term: u64, result: Result<(), DbError>) {
        if let Some(t) = self.commit_times.remove(&index) {
            let el = t.elapsed();
            if el > Duration::from_millis(100) {
                tracing::warn!(
                    "slow apply: entry {index} committed->applied took {} ms",
                    el.as_millis()
                );
            }
        }
        match &result {
            Ok(()) => {
                self.applied_index = index;
            }
            Err(e) => {
                tracing::error!("store failed to apply committed entry {index}: {e}");
                // 이후 재적용을 위해 apply_sent를 되돌린다.
                self.apply_sent = self.applied_index;
                if self.role == Role::Leader {
                    self.become_follower(self.term(), None);
                }
                // 잠시 후 재시도
                let tx = self.tx.clone();
                let applied = self.applied_index;
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    let _ = tx.send(Event::ApplierReady { applied }).await;
                });
            }
        }
        if let Some((pterm, _)) = self.proposals.get(&index) {
            let pterm = *pterm;
            let (_, reply) = self.proposals.remove(&index).unwrap();
            let r = match result {
                Ok(()) if pterm == term => Ok(()),
                Ok(()) => Err(DbError::leader_changed(
                    "entry was replaced by a new leader",
                )),
                Err(e) => Err(DbError::commit_unknown(format!(
                    "committed but local apply failed: {}",
                    e.message
                ))),
            };
            let _ = reply.send(r);
        }
        if self.leader_noop == Some(index) && self.role == Role::Leader {
            self.leader_noop = None;
            tracing::info!(
                "leader {} ready (term {}, applied {index})",
                self.cfg.node_id,
                self.term()
            );
            for r in std::mem::take(&mut self.deferred_reads) {
                self.on_read_index(r);
            }
        }
        if index.is_multiple_of(64) || self.applied_index >= self.commit_index {
            let _ = self.apply_tx.send(ApplyCmd::MaybeSnapshot {
                snapshot_index: self.st.snapshot.index,
            });
        }
        self.send_to_applier();
    }

    fn on_propose(&mut self, data: Vec<u8>, reply: oneshot::Sender<Result<(), DbError>>) {
        if self.role != Role::Leader {
            let _ = reply.send(Err(DbError::leader_changed("this node is not the leader")));
            return;
        }
        if self.leader_noop.is_some() {
            let _ = reply.send(Err(DbError::unavailable("leader is not ready yet")));
            return;
        }
        if !self.is_standalone() && !self.has_quorum_contact(Instant::now()) {
            let _ = reply.send(Err(DbError::no_quorum()));
            return;
        }
        let idx = self.st.last_index() + 1;
        let term = self.term();
        if let Err(e) = self.st.append(idx, term, Arc::new(data)) {
            self.fatal(format!("append failed: {e}"));
        }
        self.proposals.insert(idx, (term, reply));
        self.propose_times.insert(idx, Instant::now());
        self.need_sync = true;
    }

    // -------------------------------------------------------------- read index

    fn on_read_index(&mut self, reply: oneshot::Sender<Result<u64, DbError>>) {
        match self.role {
            Role::Leader => {
                if self.leader_noop.is_some() {
                    self.deferred_reads.push(reply);
                    return;
                }
                let index = self.commit_index;
                if self.is_standalone() {
                    let _ = reply.send(Ok(index));
                    return;
                }
                self.round += 1;
                self.reads.push(PendingRead {
                    index,
                    round: self.round,
                    acks: HashSet::new(),
                    reply,
                    deadline: Instant::now() + self.cfg.election_max,
                });
                // 즉시 Heartbeat 라운드
                let ids: Vec<NodeId> = self.peers.keys().copied().collect();
                for id in ids {
                    if let Some(p) = self.peers.get_mut(&id) {
                        p.last_sent = Instant::now() - self.cfg.heartbeat;
                    }
                    self.replicate(id, true);
                }
            }
            _ => {
                let Some(leader) = self.leader_id else {
                    let _ = reply.send(Err(DbError::no_quorum()));
                    return;
                };
                let t = self.transport.clone();
                let to = self.cfg.election_max + self.cfg.rpc_timeout;
                tokio::spawn(async move {
                    let r = match t.call(leader, &PeerRequest::ReadIndex, to).await {
                        Ok(PeerResponse::ReadIndex(r)) => r,
                        Ok(PeerResponse::Error(e)) => Err(e),
                        Ok(_) => Err(DbError::internal("unexpected read index response")),
                        Err(_) => Err(DbError::no_quorum()),
                    };
                    let _ = reply.send(r);
                });
            }
        }
    }

    fn resolve_reads(&mut self) {
        let q = quorum(self.voters());
        let mut i = 0;
        while i < self.reads.len() {
            if self.reads[i].acks.len() + 1 >= q {
                let r = self.reads.remove(i);
                let _ = r.reply.send(Ok(r.index));
            } else {
                i += 1;
            }
        }
    }

    // --------------------------------------------------------------- snapshots

    fn send_snapshot(&mut self, peer: NodeId) {
        let Some(path) = self.st.snapshot_path() else {
            tracing::error!("peer {peer} needs a snapshot but none exists");
            return;
        };
        let meta = self.st.snapshot.clone();
        let p = self.peers.get_mut(&peer).unwrap();
        let offset = match p.snapshot {
            Some((idx, off)) if idx == meta.index => off,
            _ => 0,
        };
        p.snapshot = Some((meta.index, offset));
        p.inflight = true;
        p.last_sent = Instant::now();
        let chunk = self.cfg.snapshot_chunk;
        let term = self.term();
        let leader = self.cfg.node_id;
        let t = self.transport.clone();
        let tx = self.tx.clone();
        let to = self.cfg.rpc_timeout;
        tokio::spawn(async move {
            let read = (|| -> std::io::Result<Vec<u8>> {
                let mut f = std::fs::File::open(&path)?;
                f.seek(SeekFrom::Start(offset))?;
                let mut buf = vec![0u8; chunk];
                let mut n = 0;
                while n < chunk {
                    let r = f.read(&mut buf[n..])?;
                    if r == 0 {
                        break;
                    }
                    n += r;
                }
                buf.truncate(n);
                Ok(buf)
            })();
            let data = match read {
                Ok(d) => d,
                Err(e) => {
                    let _ = tx
                        .send(Event::SnapshotResult {
                            peer,
                            term,
                            snap_index: meta.index,
                            offset,
                            done: false,
                            resp: Err(RpcError::Io(e.to_string())),
                        })
                        .await;
                    return;
                }
            };
            let done = offset + data.len() as u64 >= meta.size;
            let req = PeerRequest::InstallSnapshot {
                term,
                leader_id: leader,
                last_included_index: meta.index,
                last_included_term: meta.term,
                offset,
                total_size: meta.size,
                data,
                done,
            };
            // 마지막 청크는 Follower의 RDB 교체를 기다린다.
            let timeout = if done {
                Duration::from_secs(1800)
            } else {
                to.max(Duration::from_secs(30))
            };
            let resp = t.call(peer, &req, timeout).await;
            let _ = tx
                .send(Event::SnapshotResult {
                    peer,
                    term,
                    snap_index: meta.index,
                    offset,
                    done,
                    resp,
                })
                .await;
        });
    }

    fn on_snapshot_result(
        &mut self,
        peer: NodeId,
        term: u64,
        snap_index: u64,
        _offset: u64,
        done: bool,
        resp: Result<PeerResponse, RpcError>,
    ) {
        let Some(p) = self.peers.get_mut(&peer) else {
            return;
        };
        p.inflight = false;
        if self.role != Role::Leader || term != self.term() {
            return;
        }
        match resp {
            Ok(PeerResponse::Snapshot {
                term: rterm,
                next_offset,
                installed,
            }) => {
                if rterm > self.term() {
                    self.become_follower(rterm, None);
                    return;
                }
                let p = self.peers.get_mut(&peer).unwrap();
                p.last_ack = Some(Instant::now());
                if installed {
                    tracing::info!("snapshot {snap_index} installed on node {peer}");
                    p.snapshot = None;
                    p.match_index = p.match_index.max(snap_index);
                    p.next_index = snap_index + 1;
                    self.advance_commit();
                } else {
                    p.snapshot = Some((snap_index, next_offset));
                    let _ = done;
                }
                self.replicate(peer, false);
            }
            Ok(PeerResponse::Error(e)) => {
                tracing::warn!("snapshot to {peer} failed: {e}");
                if let Some(p) = self.peers.get_mut(&peer) {
                    p.snapshot = None;
                }
            }
            _ => {
                if let Some(p) = self.peers.get_mut(&peer) {
                    p.snapshot = None;
                }
            }
        }
    }

    fn on_snapshot_created(&mut self, meta: SnapshotMeta) {
        tracing::info!(
            "snapshot created at index {} ({} bytes)",
            meta.index,
            meta.size
        );
        if meta.index <= self.st.snapshot.index {
            let _ = std::fs::remove_file(self.st.snapshots_dir().join(&meta.file));
            return;
        }
        let idx = meta.index;
        if let Err(e) = self.st.set_snapshot(meta) {
            tracing::error!("cannot record snapshot: {e}");
            return;
        }
        // Follower Catch-up을 위해 일부 로그는 남긴다. 더 뒤처진 Follower는 Snapshot으로 따라잡는다.
        let upto = idx.saturating_sub(self.cfg.snapshot_keep);
        if upto >= self.st.first_index() {
            if let Err(e) = self.st.compact(upto) {
                tracing::error!("log compaction failed: {e}");
            }
        }
    }

    fn on_snapshot_installed(&mut self, index: u64, term: u64, result: Result<(), DbError>) {
        let Some(reply) = self.installing.take() else {
            return;
        };
        match result {
            Ok(()) => {
                let keep_log = self.st.term(index) == Some(term);
                let file = format!("snap-{index:020}-{term}.sqlite");
                let size = std::fs::metadata(self.st.snapshots_dir().join(&file))
                    .map(|m| m.len())
                    .unwrap_or(0);
                if let Err(e) = self.st.set_snapshot(SnapshotMeta {
                    index,
                    term,
                    file,
                    size,
                }) {
                    self.fatal(format!("snapshot meta write failed: {e}"));
                }
                if !keep_log {
                    if let Err(e) = self.st.reset_to_snapshot() {
                        self.fatal(format!("log reset failed: {e}"));
                    }
                    self.log_epoch += 1;
                }
                self.commit_index = self.commit_index.max(index);
                self.applied_index = index;
                self.apply_sent = index;
                let _ = reply.send(PeerResponse::Snapshot {
                    term: self.term(),
                    next_offset: 0,
                    installed: true,
                });
                self.send_to_applier();
            }
            Err(e) => {
                tracing::error!("snapshot install failed: {e}");
                let _ = reply.send(PeerResponse::Error(e));
            }
        }
    }

    // ------------------------------------------------------------ peer inbound

    fn on_peer(&mut self, from: NodeId, req: PeerRequest, reply: oneshot::Sender<PeerResponse>) {
        match req {
            PeerRequest::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
                pre_vote,
            } => {
                let r = self.on_request_vote(
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                    pre_vote,
                );
                let _ = reply.send(r);
            }
            PeerRequest::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                round,
            } => {
                let r = self.on_append_entries(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    round,
                );
                let _ = reply.send(r);
            }
            PeerRequest::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                offset,
                total_size,
                data,
                done,
            } => {
                self.on_install_snapshot(
                    term,
                    leader_id,
                    last_included_index,
                    last_included_term,
                    offset,
                    total_size,
                    data,
                    done,
                    reply,
                );
            }
            PeerRequest::ReadIndex => {
                if self.role != Role::Leader {
                    let _ = reply.send(PeerResponse::ReadIndex(Err(DbError::leader_changed(
                        "not the leader",
                    ))));
                    return;
                }
                let (tx, rx) = oneshot::channel();
                self.on_read_index(tx);
                tokio::spawn(async move {
                    let r = rx
                        .await
                        .unwrap_or_else(|_| Err(DbError::leader_changed("read index dropped")));
                    let _ = reply.send(PeerResponse::ReadIndex(r));
                });
            }
            PeerRequest::Forward(_) => {
                let _ = reply.send(PeerResponse::Error(DbError::internal(
                    "forward must be handled by the node",
                )));
            }
            PeerRequest::Ping { .. } => {
                let _ = reply.send(PeerResponse::Pong { term: self.term() });
            }
        }
        let _ = from;
    }

    fn log_up_to_date(&self, last_index: u64, last_term: u64) -> bool {
        let my_term = self.st.last_term();
        last_term > my_term || (last_term == my_term && last_index >= self.st.last_index())
    }

    fn in_leader_lease(&self) -> bool {
        match self.role {
            Role::Leader => true,
            _ => {
                self.last_leader_contact
                    .map(|t| t.elapsed() < self.cfg.election_min)
                    .unwrap_or(false)
                    || self.leader_alive_by_ping()
            }
        }
    }

    /// 현재 Leader(같은 임기)가 최근 election_min 안에 Ping을 보냈고, 그 코어 정체가 한도 안인가.
    fn leader_alive_by_ping(&self) -> bool {
        match *self.live.leader_ping.lock() {
            Some((id, term, t)) => {
                Some(id) == self.leader_id
                    && term == self.term()
                    && t.elapsed() < self.cfg.election_min
            }
            None => false,
        }
    }

    fn on_request_vote(
        &mut self,
        term: u64,
        cand: NodeId,
        last_index: u64,
        last_term: u64,
        pre_vote: bool,
    ) -> PeerResponse {
        if pre_vote {
            let granted = term > self.term()
                && self.log_up_to_date(last_index, last_term)
                && !self.in_leader_lease();
            return PeerResponse::Vote {
                term: self.term(),
                granted,
            };
        }
        if term < self.term() {
            return PeerResponse::Vote {
                term: self.term(),
                granted: false,
            };
        }
        if term > self.term() {
            if self.in_leader_lease() && self.leader_id != Some(cand) {
                // 현재 Leader가 살아 있는데 분리됐던 노드가 임기를 올린 경우 무시
                return PeerResponse::Vote {
                    term: self.term(),
                    granted: false,
                };
            }
            self.become_follower(term, None);
        }
        let hs = self.st.hard;
        let can = hs.voted_for == 0 || hs.voted_for == cand;
        if can && self.log_up_to_date(last_index, last_term) {
            if hs.voted_for != cand {
                self.persist_hard(HardState {
                    term: self.term(),
                    voted_for: cand,
                });
            }
            self.reset_election_timer();
            return PeerResponse::Vote {
                term: self.term(),
                granted: true,
            };
        }
        PeerResponse::Vote {
            term: self.term(),
            granted: false,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append_entries(
        &mut self,
        term: u64,
        leader: NodeId,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<WireEntry>,
        leader_commit: u64,
        round: u64,
    ) -> PeerResponse {
        let reject = |s: &Self, conflict: u64| PeerResponse::Append {
            term: s.term(),
            success: false,
            match_index: 0,
            conflict_index: conflict,
            round,
        };
        if term < self.term() {
            return reject(self, 0);
        }
        if term > self.term() || self.role != Role::Follower || self.leader_id != Some(leader) {
            self.become_follower(term, Some(leader));
        }
        self.last_leader_contact = Some(Instant::now());
        self.reset_election_timer();
        if self.installing.is_some() {
            return reject(self, self.st.snapshot.index + 1);
        }

        let snap = self.st.snapshot.index;
        // prev 일치 검사
        if prev_index > self.st.last_index() {
            return reject(self, self.st.last_index() + 1);
        }
        if prev_index >= snap {
            match self.st.term(prev_index) {
                Some(t) if t == prev_term => {}
                Some(t) => {
                    // 충돌 term의 첫 index를 힌트로
                    let mut ci = prev_index;
                    while ci > self.st.first_index() && self.st.term(ci - 1) == Some(t) {
                        ci -= 1;
                    }
                    return reject(self, ci.max(self.commit_index + 1));
                }
                None => return reject(self, snap + 1),
            }
        }
        // 엔트리 기록
        let mut last_new = prev_index;
        let mut appended = false;
        for e in entries {
            if e.index <= snap {
                last_new = e.index;
                continue;
            }
            match self.st.term(e.index) {
                Some(t) if t == e.term => {
                    last_new = e.index;
                    continue;
                }
                Some(_) => {
                    if e.index <= self.commit_index {
                        tracing::error!("leader tried to overwrite committed entry {}", e.index);
                        return reject(self, self.commit_index + 1);
                    }
                    // 미확정 충돌 로그 폐기
                    if let Err(err) = self.st.truncate_from(e.index) {
                        self.fatal(format!("log truncate failed: {err}"));
                    }
                    self.log_epoch += 1;
                    self.durable_index = self.durable_index.min(self.st.last_index());
                }
                None => {}
            }
            if e.index != self.st.last_index() + 1 {
                break;
            }
            if let Err(err) = self.st.append(e.index, e.term, Arc::new(e.data)) {
                self.fatal(format!("append failed: {err}"));
            }
            appended = true;
            last_new = e.index;
        }
        if appended || self.durable_index < last_new {
            // Durable ACK: fsync 완료 후에만 성공 응답
            self.sync();
        }
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(last_new.max(self.commit_index));
            self.send_to_applier();
        }
        PeerResponse::Append {
            term: self.term(),
            success: true,
            match_index: last_new,
            conflict_index: 0,
            round,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_install_snapshot(
        &mut self,
        term: u64,
        leader: NodeId,
        index: u64,
        sterm: u64,
        offset: u64,
        total: u64,
        data: Vec<u8>,
        done: bool,
        reply: oneshot::Sender<PeerResponse>,
    ) {
        if term < self.term() {
            let _ = reply.send(PeerResponse::Snapshot {
                term: self.term(),
                next_offset: 0,
                installed: false,
            });
            return;
        }
        if term > self.term() || self.role != Role::Follower || self.leader_id != Some(leader) {
            self.become_follower(term, Some(leader));
        }
        self.last_leader_contact = Some(Instant::now());
        self.reset_election_timer();
        if index <= self.applied_index {
            let _ = reply.send(PeerResponse::Snapshot {
                term: self.term(),
                next_offset: 0,
                installed: true,
            });
            return;
        }
        if self.installing.is_some() {
            let _ = reply.send(PeerResponse::Error(DbError::unavailable(
                "snapshot install in progress",
            )));
            return;
        }
        let dir = self.st.snapshots_dir();
        let tmp = dir.join("incoming.sqlite.part");
        let r = (|| -> std::io::Result<u64> {
            if offset == 0 {
                let f = std::fs::File::create(&tmp)?;
                self.incoming = Some((index, sterm, f, 0));
            }
            match &mut self.incoming {
                Some((i, t, f, next)) if *i == index && *t == sterm && *next == offset => {
                    f.write_all(&data)?;
                    *next += data.len() as u64;
                    Ok(*next)
                }
                Some((_, _, _, next)) => Ok(if *next > 0 && offset != *next {
                    0
                } else {
                    *next
                }),
                None => Ok(0),
            }
        })();
        let next = match r {
            Ok(n) => n,
            Err(e) => {
                let _ = reply.send(PeerResponse::Error(DbError::io(format!(
                    "snapshot write failed: {e}"
                ))));
                self.incoming = None;
                return;
            }
        };
        if !done || next < total {
            let _ = reply.send(PeerResponse::Snapshot {
                term: self.term(),
                next_offset: next,
                installed: false,
            });
            return;
        }
        // 수신 완료: fsync 후 Store에 설치 요청
        let (_, _, f, _) = self.incoming.take().unwrap();
        if let Err(e) = f.sync_all() {
            let _ = reply.send(PeerResponse::Error(DbError::io(format!(
                "snapshot fsync failed: {e}"
            ))));
            return;
        }
        drop(f);
        let file = format!("snap-{index:020}-{sterm}.sqlite");
        let dst = dir.join(&file);
        if let Err(e) = std::fs::rename(&tmp, &dst) {
            let _ = reply.send(PeerResponse::Error(DbError::io(format!(
                "snapshot rename failed: {e}"
            ))));
            return;
        }
        tracing::info!(
            "received snapshot {index}/{sterm} ({total} bytes) from leader {leader}; installing"
        );
        self.installing = Some(reply);
        let _ = self.apply_tx.send(ApplyCmd::InstallSnapshot {
            path: dst,
            index,
            term: sterm,
        });
    }
}
