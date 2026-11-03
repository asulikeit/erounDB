//! 3노드 Raft: 선출, 복제, Leader 장애 후 재선출, 과반수 상실 시 쓰기 거부.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use jamsil_common::{DbError, ReplicatedCommand};
use jamsil_consensus::msg::PeerResponse;
use jamsil_consensus::{
    start, RaftConfig, RaftHandle, Role, StateMachine, Transport, TransportConfig,
};
use jamsil_ipc::rpc::BoxFut;

#[derive(Default)]
struct MemSm {
    applied: Mutex<Vec<(u64, Vec<u8>)>>,
}

impl StateMachine for MemSm {
    fn apply(&self, index: u64, _term: u64, data: Arc<Vec<u8>>) -> BoxFut<Result<(), DbError>> {
        let mut a = self.applied.lock();
        let last = a.last().map(|x| x.0).unwrap_or(0);
        if index > last {
            a.push((index, (*data).clone()));
        }
        Box::pin(async { Ok(()) })
    }
    fn applied_index(&self) -> BoxFut<Result<u64, DbError>> {
        let i = self.applied.lock().last().map(|x| x.0).unwrap_or(0);
        Box::pin(async move { Ok(i) })
    }
    fn create_snapshot(&self, _p: PathBuf) -> BoxFut<Result<(u64, u64), DbError>> {
        Box::pin(async { Err(DbError::unsupported("snapshot")) })
    }
    fn install_snapshot(&self, _p: PathBuf, _i: u64, _t: u64) -> BoxFut<Result<(), DbError>> {
        Box::pin(async { Err(DbError::unsupported("snapshot")) })
    }
    fn abort_all(&self) -> BoxFut<Result<(), DbError>> {
        Box::pin(async { Ok(()) })
    }
}

struct Node {
    handle: RaftHandle,
    sm: Arc<MemSm>,
    server: tokio::task::JoinHandle<()>,
    transport: Arc<Transport>,
}

async fn spawn_node(id: u64, ports: &HashMap<u64, u16>, dir: &std::path::Path) -> Node {
    let peers: HashMap<u64, String> = ports
        .iter()
        .map(|(k, p)| (*k, format!("127.0.0.1:{p}")))
        .collect();
    let transport = Transport::new(TransportConfig {
        node_id: id,
        cluster_id: "t".into(),
        secret: Some("s3cret".into()),
        peers,
        frame_size: 1 << 20,
        connect_timeout: Duration::from_millis(500),
    });
    let sm = Arc::new(MemSm::default());
    let cfg = RaftConfig {
        node_id: id,
        voters: vec![1, 2, 3],
        data_dir: dir.join(format!("n{id}")),
        heartbeat: Duration::from_millis(50),
        election_min: Duration::from_millis(300),
        election_max: Duration::from_millis(600),
        max_append_bytes: 1 << 20,
        snapshot_threshold: 1_000_000,
        snapshot_keep: 100,
        rpc_timeout: Duration::from_millis(500),
        snapshot_chunk: 1 << 20,
    };
    let handle = start(cfg, transport.clone(), sm.clone()).unwrap();
    let h2 = handle.clone();
    let t2 = transport.clone();
    let bind = format!("127.0.0.1:{}", ports[&id]).parse().unwrap();
    let server = tokio::spawn(async move {
        let h = h2.clone();
        let _ = t2
            .serve(
                bind,
                Arc::new(move |from, req| {
                    let h = h.clone();
                    Box::pin(async move { h.handle_peer(from, req).await }) as BoxFut<PeerResponse>
                }),
            )
            .await;
    });
    Node {
        handle,
        sm,
        server,
        transport,
    }
}

async fn wait_leader(nodes: &HashMap<u64, Node>) -> u64 {
    for _ in 0..100 {
        for (id, n) in nodes {
            let s = n.handle.status();
            if s.role == Role::Leader && s.leader_ready {
                return *id;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no leader elected");
}

fn cmd(i: u64) -> Vec<u8> {
    bincode::serialize(&ReplicatedCommand::Txn(jamsil_common::TxnPayload {
        tx_id: i as u128,
        request_id: None,
        schema_version: 1,
        ddl: vec![],
        changeset: i.to_le_bytes().to_vec(),
        tag: String::new(),
        checksum: 0,
    }))
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn elect_replicate_failover() {
    let d = tempfile::tempdir().unwrap();
    let base = 39000 + (std::process::id() % 1000) as u16 * 3;
    let ports: HashMap<u64, u16> = [(1, base), (2, base + 1), (3, base + 2)]
        .into_iter()
        .collect();
    let mut nodes = HashMap::new();
    for id in 1..=3 {
        nodes.insert(id, spawn_node(id, &ports, d.path()).await);
    }
    let leader = wait_leader(&nodes).await;
    for i in 0..50 {
        nodes[&leader].handle.propose(cmd(i)).await.unwrap();
    }
    // Follower Read Index
    let f = *nodes.keys().find(|k| **k != leader).unwrap();
    let ri = nodes[&f].handle.read_index().await.unwrap();
    assert!(ri >= 51, "read index {ri}");

    tokio::time::sleep(Duration::from_millis(500)).await;
    let seqs: Vec<Vec<(u64, Vec<u8>)>> = nodes
        .values()
        .map(|n| n.sm.applied.lock().clone())
        .collect();
    assert_eq!(seqs[0].len(), 51); // noop + 50
    assert_eq!(seqs[0], seqs[1]);
    assert_eq!(seqs[1], seqs[2]);

    // Leader 정지 (서버 중단 + 이벤트 루프는 살아 있지만 고립)
    let old = nodes.remove(&leader).unwrap();
    old.transport.set_blocked(&[1, 2, 3]);
    for n in nodes.values() {
        n.transport.set_blocked(&[leader]);
    }
    // 고립된 이전 Leader는 과반수 접촉을 잃고 스스로 강등돼야 한다 (Split Brain 방지)
    let r = tokio::time::timeout(Duration::from_secs(3), old.handle.propose(cmd(1000))).await;
    assert!(
        !matches!(r, Ok(Ok(()))),
        "isolated old leader accepted a write"
    );
    // 남은 두 노드에서 새 Leader
    let mut new_leader = 0;
    for _ in 0..100 {
        for (id, n) in &nodes {
            let s = n.handle.status();
            if s.role == Role::Leader && s.leader_ready {
                new_leader = *id;
            }
        }
        if new_leader != 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_ne!(new_leader, 0, "no new leader");
    assert_ne!(new_leader, leader);
    for i in 50..60 {
        nodes[&new_leader].handle.propose(cmd(i)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let seqs: Vec<Vec<(u64, Vec<u8>)>> = nodes
        .values()
        .map(|n| n.sm.applied.lock().clone())
        .collect();
    assert_eq!(seqs[0], seqs[1]);
    // 이전 Leader 임기의 모든 엔트리 보존
    for i in 0..50u64 {
        assert!(
            seqs[0].iter().any(|(_, d)| *d == cmd(i)),
            "lost acknowledged entry {i}"
        );
    }

    // 하나 더 정지 → 과반수 상실: 쓰기 실패
    let other = *nodes.keys().find(|k| **k != new_leader).unwrap();
    let o = nodes.remove(&other).unwrap();
    o.transport.set_blocked(&[1, 2, 3]);
    nodes[&new_leader].transport.set_blocked(&[leader, other]);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        nodes[&new_leader].handle.propose(cmd(99)),
    )
    .await;
    match r {
        Ok(Ok(())) => panic!("write succeeded without quorum"),
        _ => {}
    }
    // Minority에서 Linearizable Read도 거부
    assert!(nodes[&new_leader].handle.read_index().await.is_err());

    // 분리 해제 → 복구 후 모든 노드가 같은 로그로 수렴
    old.transport.set_blocked(&[]);
    o.transport.set_blocked(&[]);
    nodes[&new_leader].transport.set_blocked(&[]);
    nodes.insert(leader, old);
    nodes.insert(other, o);
    let l = wait_leader(&nodes).await;
    nodes[&l].handle.propose(cmd(2000)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    let seqs: Vec<Vec<(u64, Vec<u8>)>> = nodes
        .values()
        .map(|n| n.sm.applied.lock().clone())
        .collect();
    assert_eq!(seqs[0], seqs[1]);
    assert_eq!(seqs[1], seqs[2]);
    assert!(
        !seqs[0].iter().any(|(_, d)| *d == cmd(1000)),
        "minority write leaked into the log"
    );
    let _ = &nodes[&l].server;
}
