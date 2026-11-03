//! TCP 9787 노드 간 전송. 연결마다 Cluster ID / Node ID / HMAC 핸드셰이크 후 RPC.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use sha2::Sha256;
use tokio::net::{TcpListener, TcpStream};

use jamsil_common::limits::MAX_MESSAGE_BYTES;
use jamsil_common::NodeId;
use jamsil_ipc::frame::{write_message, FrameReader};
use jamsil_ipc::rpc::{serve_stream, BoxFut, Handler, RpcClient, RpcError};

use crate::msg::{Hello, HelloReply, PeerRequest, PeerResponse};

pub const PEER_PROTOCOL_VERSION: u32 = 2;

impl jamsil_ipc::rpc::RejectResponse for PeerResponse {
    fn rejected(e: jamsil_common::DbError) -> Option<Self> {
        Some(PeerResponse::Error(e))
    }
}

#[derive(Clone)]
pub struct TransportConfig {
    pub node_id: NodeId,
    pub cluster_id: String,
    pub secret: Option<String>,
    pub peers: HashMap<NodeId, String>,
    pub frame_size: usize,
    pub connect_timeout: Duration,
}

type PeerClient = RpcClient<PeerRequest, PeerResponse>;

struct PeerSlot {
    client: Option<PeerClient>,
    /// 마지막 연결 실패 시각 (실패 직후 짧은 Backoff)
    last_failure: Option<Instant>,
    /// 연결 시도 직렬화: 동시에 호출한 작업은 진행 중인 연결을 기다린다.
    connect_lock: Arc<tokio::sync::Mutex<()>>,
}

pub struct Transport {
    cfg: TransportConfig,
    slots: Mutex<HashMap<NodeId, PeerSlot>>,
    /// 장애 주입: 통신을 차단할 노드 집합 (양방향).
    blocked: Mutex<std::collections::HashSet<NodeId>>,
}

fn mac(secret: &Option<String>, msg: &str) -> String {
    match secret {
        None => String::new(),
        Some(s) => {
            let mut m = Hmac::<Sha256>::new_from_slice(s.as_bytes()).expect("hmac key");
            m.update(msg.as_bytes());
            hex::encode(m.finalize().into_bytes())
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Transport {
    pub fn new(cfg: TransportConfig) -> Arc<Transport> {
        let slots = cfg
            .peers
            .keys()
            .filter(|id| **id != cfg.node_id)
            .map(|id| {
                (
                    *id,
                    PeerSlot {
                        client: None,
                        last_failure: None,
                        connect_lock: Arc::new(tokio::sync::Mutex::new(())),
                    },
                )
            })
            .collect();
        Arc::new(Transport {
            cfg,
            slots: Mutex::new(slots),
            blocked: Mutex::new(Default::default()),
        })
    }

    /// 장애 주입용 네트워크 분리. 차단된 노드와의 송수신을 모두 실패시킨다.
    pub fn set_blocked(&self, peers: &[NodeId]) {
        let mut b = self.blocked.lock();
        b.clear();
        b.extend(peers.iter().copied());
        let mut slots = self.slots.lock();
        for p in peers {
            if let Some(s) = slots.get_mut(p) {
                s.client = None;
            }
        }
    }

    pub fn blocked(&self) -> Vec<NodeId> {
        self.blocked.lock().iter().copied().collect()
    }

    fn is_blocked(&self, peer: NodeId) -> bool {
        self.blocked.lock().contains(&peer)
    }

    pub fn is_connected(&self, peer: NodeId) -> bool {
        self.slots
            .lock()
            .get(&peer)
            .and_then(|s| s.client.as_ref())
            .map(|c| !c.is_closed())
            .unwrap_or(false)
    }

    async fn connect(&self, peer: NodeId) -> Result<PeerClient, RpcError> {
        let addr = self
            .cfg
            .peers
            .get(&peer)
            .ok_or(RpcError::Disconnected)?
            .clone();
        let fut = async {
            let mut s = TcpStream::connect(&addr)
                .await
                .map_err(|e| RpcError::Io(e.to_string()))?;
            s.set_nodelay(true).ok();
            let nonce: u64 = rand::random();
            let ts = now_ms();
            let hello = Hello {
                cluster_id: self.cfg.cluster_id.clone(),
                from: self.cfg.node_id,
                to: peer,
                protocol_version: PEER_PROTOCOL_VERSION,
                nonce,
                ts_ms: ts,
                mac: mac(
                    &self.cfg.secret,
                    &format!(
                        "{}|{}|{}|{}|{}",
                        self.cfg.cluster_id, self.cfg.node_id, peer, nonce, ts
                    ),
                ),
            };
            let body = bincode::serialize(&hello).map_err(|e| RpcError::Codec(e.to_string()))?;
            write_message(&mut s, 0, &body, self.cfg.frame_size)
                .await
                .map_err(|e| RpcError::Io(e.to_string()))?;
            let reply = {
                let mut r = FrameReader::new(&mut s, 64 * 1024, 64 * 1024);
                r.next()
                    .await
                    .map_err(|e| RpcError::Io(e.to_string()))?
                    .ok_or(RpcError::Disconnected)?
            };
            let reply: HelloReply =
                bincode::deserialize(&reply.1).map_err(|e| RpcError::Codec(e.to_string()))?;
            match reply {
                HelloReply::Ok { node_id, mac: m } => {
                    if node_id != peer {
                        return Err(RpcError::Io(format!(
                            "peer at {addr} is node {node_id}, expected {peer}"
                        )));
                    }
                    let want = mac(
                        &self.cfg.secret,
                        &format!(
                            "ok|{}|{}|{}|{}",
                            self.cfg.cluster_id, peer, self.cfg.node_id, nonce
                        ),
                    );
                    if want != m {
                        return Err(RpcError::Io("peer authentication failed".into()));
                    }
                }
                HelloReply::Rejected(why) => {
                    return Err(RpcError::Io(format!("peer rejected handshake: {why}")))
                }
            }
            Ok(RpcClient::from_stream(s, self.cfg.frame_size))
        };
        tokio::time::timeout(self.cfg.connect_timeout, fut)
            .await
            .map_err(|_| RpcError::Timeout)?
    }

    fn cached(&self, peer: NodeId) -> Result<(Option<PeerClient>, Arc<tokio::sync::Mutex<()>>), RpcError> {
        let mut slots = self.slots.lock();
        let slot = slots.get_mut(&peer).ok_or(RpcError::Disconnected)?;
        if let Some(c) = &slot.client {
            if !c.is_closed() {
                return Ok((Some(c.clone()), slot.connect_lock.clone()));
            }
            slot.client = None;
        }
        Ok((None, slot.connect_lock.clone()))
    }

    async fn client(&self, peer: NodeId) -> Result<PeerClient, RpcError> {
        let (c, lock) = self.cached(peer)?;
        if let Some(c) = c {
            return Ok(c);
        }
        let _g = lock.lock().await;
        // 기다리는 동안 다른 작업이 연결했을 수 있다.
        if let (Some(c), _) = self.cached(peer)? {
            return Ok(c);
        }
        if let Some(t) = self.slots.lock().get(&peer).and_then(|s| s.last_failure) {
            if t.elapsed() < Duration::from_millis(200) {
                return Err(RpcError::Disconnected);
            }
        }
        let r = self.connect(peer).await;
        let mut slots = self.slots.lock();
        let slot = slots.get_mut(&peer).unwrap();
        match r {
            Ok(c) => {
                slot.client = Some(c.clone());
                slot.last_failure = None;
                Ok(c)
            }
            Err(e) => {
                slot.last_failure = Some(Instant::now());
                tracing::debug!("connect to node {peer} failed: {e}");
                Err(e)
            }
        }
    }

    pub async fn call(
        &self,
        peer: NodeId,
        req: &PeerRequest,
        timeout: Duration,
    ) -> Result<PeerResponse, RpcError> {
        if self.is_blocked(peer) {
            return Err(RpcError::Disconnected);
        }
        let c = self.client(peer).await?;
        let r = c.call_timeout(req, timeout).await;
        if matches!(r, Err(RpcError::Disconnected)) {
            if let Some(s) = self.slots.lock().get_mut(&peer) {
                s.client = None;
            }
        }
        r
    }

    /// 수신 리스너. handler(from, req)
    pub async fn serve(
        self: Arc<Self>,
        bind: SocketAddr,
        handler: Arc<dyn Fn(NodeId, PeerRequest) -> BoxFut<PeerResponse> + Send + Sync>,
    ) -> std::io::Result<()> {
        let listener = TcpListener::bind(bind).await?;
        tracing::info!("cluster transport listening on {bind}");
        loop {
            let (s, addr) = listener.accept().await?;
            let me = self.clone();
            let h = handler.clone();
            tokio::spawn(async move {
                if let Err(e) = me.accept(s, addr, h).await {
                    tracing::warn!("rejected cluster connection from {addr}: {e}");
                }
            });
        }
    }

    async fn accept(
        self: &Arc<Self>,
        mut s: TcpStream,
        addr: SocketAddr,
        handler: Arc<dyn Fn(NodeId, PeerRequest) -> BoxFut<PeerResponse> + Send + Sync>,
    ) -> Result<(), String> {
        s.set_nodelay(true).ok();
        let hello = {
            let mut r = FrameReader::new(&mut s, 64 * 1024, 64 * 1024);
            match tokio::time::timeout(Duration::from_secs(10), r.next()).await {
                Ok(Ok(Some((_, b)))) => b,
                _ => return Err("handshake timeout or error".into()),
            }
        };
        let hello: Hello = bincode::deserialize(&hello).map_err(|e| e.to_string())?;
        let reject = |why: String| HelloReply::Rejected(why);
        let verdict = if hello.cluster_id != self.cfg.cluster_id {
            Err(reject(format!(
                "cluster id mismatch ({})",
                hello.cluster_id
            )))
        } else if hello.to != self.cfg.node_id {
            Err(reject(format!(
                "this is node {}, not {}",
                self.cfg.node_id, hello.to
            )))
        } else if !self.cfg.peers.contains_key(&hello.from) || hello.from == self.cfg.node_id {
            Err(reject(format!("unknown node {}", hello.from)))
        } else if hello.protocol_version != PEER_PROTOCOL_VERSION {
            Err(reject("protocol version mismatch".into()))
        } else if mac(
            &self.cfg.secret,
            &format!(
                "{}|{}|{}|{}|{}",
                hello.cluster_id, hello.from, hello.to, hello.nonce, hello.ts_ms
            ),
        ) != hello.mac
        {
            Err(reject("authentication failed".into()))
        } else if self.cfg.secret.is_some() && now_ms().abs_diff(hello.ts_ms) > 300_000 {
            Err(reject("handshake timestamp out of range".into()))
        } else {
            Ok(())
        };
        let reply = match &verdict {
            Ok(()) => HelloReply::Ok {
                node_id: self.cfg.node_id,
                mac: mac(
                    &self.cfg.secret,
                    &format!(
                        "ok|{}|{}|{}|{}",
                        self.cfg.cluster_id, self.cfg.node_id, hello.from, hello.nonce
                    ),
                ),
            },
            Err(r) => r.clone(),
        };
        let body = bincode::serialize(&reply).map_err(|e| e.to_string())?;
        write_message(&mut s, 0, &body, self.cfg.frame_size)
            .await
            .map_err(|e| e.to_string())?;
        if let Err(HelloReply::Rejected(why)) = verdict {
            return Err(why);
        }
        tracing::debug!("cluster connection from node {} ({addr})", hello.from);
        let from = hello.from;
        let me = self.clone();
        let h: Handler<PeerRequest, PeerResponse> = Arc::new(move |req| {
            if me.is_blocked(from) {
                return Box::pin(async {
                    PeerResponse::Error(jamsil_common::DbError::unavailable("partitioned"))
                }) as BoxFut<PeerResponse>;
            }
            handler(from, req)
        });
        serve_stream(s, h, self.cfg.frame_size).await;
        let _ = MAX_MESSAGE_BYTES;
        Ok(())
    }
}
