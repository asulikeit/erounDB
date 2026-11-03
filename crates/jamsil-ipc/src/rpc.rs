//! 다중화 요청/응답 RPC. 하나의 연결에서 여러 요청을 동시에 처리하며,
//! 응답은 프레임 헤더의 msg_id로 상관관계를 맞춘다.

use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot, Semaphore};

use jamsil_common::limits::{DEFAULT_TRANSPORT_FRAME, MAX_MESSAGE_BYTES};
use jamsil_common::DbError;

use crate::frame::{FrameReader, Incoming, InterleavedWriter};

/// 메모리 예산 부족으로 버린 요청에 대한 응답 (응답 타입이 오류를 표현할 수 있을 때만).
pub trait RejectResponse: Sized {
    fn rejected(e: DbError) -> Option<Self>;
}

impl<T> RejectResponse for Result<T, DbError> {
    fn rejected(e: DbError) -> Option<Self> {
        Some(Err(e))
    }
}

impl RejectResponse for () {
    fn rejected(_: DbError) -> Option<Self> {
        None
    }
}

impl RejectResponse for crate::msg::SupervisorResponse {
    fn rejected(_: DbError) -> Option<Self> {
        None
    }
}

const KIND_REQUEST: u8 = 0;
const KIND_RESPONSE: u8 = 1;
const KIND_NOTIFY: u8 = 2;

/// 연결당 동시에 처리하는 최대 요청 수 (Backpressure).
const MAX_INFLIGHT_PER_CONN: usize = 1024;

/// 이 프로세스가 처리 중인 IPC 요청 수 (서버 측 큐 깊이)
static SERVER_INFLIGHT: AtomicU64 = AtomicU64::new(0);
/// 이 프로세스가 응답을 기다리는 IPC 호출 수 (클라이언트 측 큐 깊이)
static CLIENT_PENDING: AtomicU64 = AtomicU64::new(0);
/// 연결당 동시 처리 한도에 걸려 수신을 멈춘 횟수
static INFLIGHT_STALLS: AtomicU64 = AtomicU64::new(0);

pub fn ipc_samples() -> Vec<jamsil_common::metrics::MetricSample> {
    use jamsil_common::metrics::MetricSample as M;
    vec![
        M::new("jamsil_ipc_queue_depth", "side=server", SERVER_INFLIGHT.load(Ordering::Relaxed) as f64, "count"),
        M::new("jamsil_ipc_queue_depth", "side=client", CLIENT_PENDING.load(Ordering::Relaxed) as f64, "count"),
        M::new(
            "jamsil_ipc_backpressure_events_total",
            "reason=ipc_inflight_limit",
            INFLIGHT_STALLS.load(Ordering::Relaxed) as f64,
            "count",
        ),
    ]
}

pub type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub type Handler<Req, Resp> = Arc<dyn Fn(Req) -> BoxFut<Resp> + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcError {
    Disconnected,
    Timeout,
    Codec(String),
    Io(String),
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::Disconnected => write!(f, "ipc peer disconnected"),
            RpcError::Timeout => write!(f, "ipc request timed out"),
            RpcError::Codec(e) => write!(f, "ipc codec error: {e}"),
            RpcError::Io(e) => write!(f, "ipc io error: {e}"),
        }
    }
}

impl std::error::Error for RpcError {}

impl From<RpcError> for DbError {
    fn from(e: RpcError) -> DbError {
        DbError::unavailable(format!("internal process unavailable: {e}"))
    }
}

pub fn encode<T: Serialize>(kind: u8, v: &T) -> Result<Vec<u8>, RpcError> {
    let size = bincode::serialized_size(v).map_err(|e| RpcError::Codec(e.to_string()))? as usize;
    let mut out = Vec::with_capacity(size + 1);
    out.push(kind);
    bincode::serialize_into(&mut out, v).map_err(|e| RpcError::Codec(e.to_string()))?;
    Ok(out)
}

pub fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<(u8, T), RpcError> {
    if body.is_empty() {
        return Err(RpcError::Codec("empty message".into()));
    }
    let v = bincode::deserialize(&body[1..]).map_err(|e| RpcError::Codec(e.to_string()))?;
    Ok((body[0], v))
}

/// 송신 루프: 대기 중인 메시지들의 프레임을 번갈아 기록한다.
async fn writer_loop<W: AsyncWrite + Unpin>(
    w: &mut W,
    rx: &mut mpsc::Receiver<Outgoing>,
    frame_size: usize,
) {
    let mut q = InterleavedWriter::new(frame_size);
    loop {
        if q.is_empty() {
            match rx.recv().await {
                Some(o) => q.push(o.id, o.body),
                None => return,
            }
        }
        while let Ok(o) = rx.try_recv() {
            q.push(o.id, o.body);
        }
        if q.write_one(w).await.is_err() {
            return;
        }
    }
}

struct Outgoing {
    id: u64,
    body: Vec<u8>,
}

struct ClientInner {
    tx: mpsc::Sender<Outgoing>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Vec<u8>>>>,
    next_id: AtomicU64,
    closed: AtomicBool,
}

/// 다중화 RPC 클라이언트. Clone은 같은 연결을 공유한다.
pub struct RpcClient<Req, Resp> {
    inner: Arc<ClientInner>,
    _p: PhantomData<fn(Req) -> Resp>,
}

impl<Req, Resp> Clone for RpcClient<Req, Resp> {
    fn clone(&self) -> Self {
        RpcClient {
            inner: self.inner.clone(),
            _p: PhantomData,
        }
    }
}

impl<Req, Resp> RpcClient<Req, Resp>
where
    Req: Serialize + Send + 'static,
    Resp: DeserializeOwned + Send + 'static,
{
    pub fn from_stream<S>(stream: S, frame_size: usize) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (rd, mut wr) = tokio::io::split(stream);
        let (tx, mut rx) = mpsc::channel::<Outgoing>(256);
        let inner = Arc::new(ClientInner {
            tx,
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            closed: AtomicBool::new(false),
        });
        let w_inner = inner.clone();
        tokio::spawn(async move {
            writer_loop(&mut wr, &mut rx, frame_size).await;
            w_inner.closed.store(true, Ordering::SeqCst);
            w_inner.pending.lock().clear();
        });
        let r_inner = inner.clone();
        tokio::spawn(async move {
            let mut reader = FrameReader::new(
                tokio::io::BufReader::with_capacity(256 * 1024, rd),
                MAX_MESSAGE_BYTES,
                frame_size.max(DEFAULT_TRANSPORT_FRAME),
            );
            while let Ok(Some((id, body))) = reader.next().await {
                if body.first() == Some(&KIND_RESPONSE) {
                    if let Some(tx) = r_inner.pending.lock().remove(&id) {
                        let _ = tx.send(body);
                    }
                }
            }
            r_inner.closed.store(true, Ordering::SeqCst);
            r_inner.pending.lock().clear();
        });
        RpcClient {
            inner,
            _p: PhantomData,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    pub async fn call(&self, req: &Req) -> Result<Resp, RpcError> {
        self.call_inner(req, None).await
    }

    pub async fn call_timeout(&self, req: &Req, timeout: Duration) -> Result<Resp, RpcError> {
        self.call_inner(req, Some(timeout)).await
    }

    async fn call_inner(&self, req: &Req, timeout: Option<Duration>) -> Result<Resp, RpcError> {
        if self.is_closed() {
            return Err(RpcError::Disconnected);
        }
        let body = encode(KIND_REQUEST, req)?;
        struct Pending;
        impl Drop for Pending {
            fn drop(&mut self) {
                CLIENT_PENDING.fetch_sub(1, Ordering::Relaxed);
            }
        }
        CLIENT_PENDING.fetch_add(1, Ordering::Relaxed);
        let _pending = Pending;
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.inner.pending.lock().insert(id, tx);
        if self.inner.tx.send(Outgoing { id, body }).await.is_err() {
            self.inner.pending.lock().remove(&id);
            return Err(RpcError::Disconnected);
        }
        let got = match timeout {
            Some(t) => match tokio::time::timeout(t, rx).await {
                Ok(r) => r,
                Err(_) => {
                    self.inner.pending.lock().remove(&id);
                    return Err(RpcError::Timeout);
                }
            },
            None => rx.await,
        };
        let body = got.map_err(|_| RpcError::Disconnected)?;
        let (_, resp) = decode::<Resp>(&body)?;
        Ok(resp)
    }

    /// 응답을 기다리지 않는 단방향 메시지 (로그 등).
    pub fn notify(&self, req: &Req) -> Result<(), RpcError> {
        if self.is_closed() {
            return Err(RpcError::Disconnected);
        }
        let body = encode(KIND_NOTIFY, req)?;
        self.inner
            .tx
            .try_send(Outgoing { id: 0, body })
            .map_err(|_| RpcError::Disconnected)
    }
}

pub async fn connect_unix<Req, Resp>(path: &Path) -> Result<RpcClient<Req, Resp>, RpcError>
where
    Req: Serialize + Send + 'static,
    Resp: DeserializeOwned + Send + 'static,
{
    let s = UnixStream::connect(path)
        .await
        .map_err(|e| RpcError::Io(e.to_string()))?;
    Ok(RpcClient::from_stream(s, DEFAULT_TRANSPORT_FRAME))
}

/// 연결 하나에 대한 서버 루프. 연결이 끊기면 반환.
pub async fn serve_stream<S, Req, Resp>(stream: S, handler: Handler<Req, Resp>, frame_size: usize)
where
    S: AsyncRead + AsyncWrite + Send + 'static,
    Req: DeserializeOwned + Send + 'static,
    Resp: Serialize + RejectResponse + Send + 'static,
{
    let (rd, mut wr) = tokio::io::split(stream);
    let (tx, mut rx) = mpsc::channel::<Outgoing>(256);
    let writer = tokio::spawn(async move {
        writer_loop(&mut wr, &mut rx, frame_size).await;
    });
    let sem = Arc::new(Semaphore::new(MAX_INFLIGHT_PER_CONN));
    let mut reader = FrameReader::new(
        tokio::io::BufReader::with_capacity(256 * 1024, rd),
        MAX_MESSAGE_BYTES,
        frame_size.max(DEFAULT_TRANSPORT_FRAME),
    )
    .with_budget();
    loop {
        let (id, body, guard) = match reader.next_incoming().await {
            Ok(Some(Incoming::Message { id, body, guard })) => (id, body, guard),
            Ok(Some(Incoming::Rejected { id, error })) => {
                tracing::warn!("ipc request {id} rejected: {}", error.message);
                if let Some(resp) = Resp::rejected(error) {
                    if let Ok(body) = encode(KIND_RESPONSE, &resp) {
                        let _ = tx.send(Outgoing { id, body }).await;
                    }
                }
                continue;
            }
            Ok(None) => break,
            Err(e) => {
                tracing::debug!("ipc connection closed: {e}");
                break;
            }
        };
        let (kind, req) = match decode::<Req>(&body) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("ipc decode error: {e}");
                break;
            }
        };
        drop(body);
        let permit = match sem.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                INFLIGHT_STALLS.fetch_add(1, Ordering::Relaxed);
                match sem.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                }
            }
        };
        let h = handler.clone();
        let tx = tx.clone();
        SERVER_INFLIGHT.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            let resp = h(req).await;
            SERVER_INFLIGHT.fetch_sub(1, Ordering::Relaxed);
            if kind == KIND_REQUEST {
                match encode(KIND_RESPONSE, &resp) {
                    Ok(body) => {
                        let _ = tx.send(Outgoing { id, body }).await;
                    }
                    Err(e) => tracing::error!("ipc encode error: {e}"),
                }
            }
            drop(permit);
            drop(guard);
        });
    }
    drop(tx);
    let _ = writer.await;
}

/// UDS 리스너. 기존 소켓 파일은 제거 후 0660 권한으로 생성한다.
pub async fn serve_unix<Req, Resp>(path: &Path, handler: Handler<Req, Resp>) -> std::io::Result<()>
where
    Req: DeserializeOwned + Send + 'static,
    Resp: Serialize + RejectResponse + Send + 'static,
{
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let _ = std::fs::remove_file(path);
    let listener = UnixListener::bind(path)?;
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o660))?;
    }
    loop {
        let (s, _) = listener.accept().await?;
        let h = handler.clone();
        tokio::spawn(serve_stream(s, h, DEFAULT_TRANSPORT_FRAME));
    }
}

/// 끊기면 다시 연결하는 UDS 클라이언트 래퍼.
pub struct UnixClient<Req, Resp> {
    path: std::path::PathBuf,
    cur: tokio::sync::Mutex<Option<RpcClient<Req, Resp>>>,
}

impl<Req, Resp> UnixClient<Req, Resp>
where
    Req: Serialize + Send + 'static,
    Resp: DeserializeOwned + Send + 'static,
{
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        UnixClient {
            path: path.into(),
            cur: tokio::sync::Mutex::new(None),
        }
    }

    pub async fn client(&self) -> Result<RpcClient<Req, Resp>, RpcError> {
        let mut g = self.cur.lock().await;
        if let Some(c) = g.as_ref() {
            if !c.is_closed() {
                return Ok(c.clone());
            }
        }
        let c = connect_unix::<Req, Resp>(&self.path).await?;
        *g = Some(c.clone());
        Ok(c)
    }

    pub async fn call(&self, req: &Req) -> Result<Resp, RpcError> {
        let c = self.client().await?;
        c.call(req).await
    }

    pub async fn call_timeout(&self, req: &Req, t: Duration) -> Result<Resp, RpcError> {
        let c = self.client().await?;
        c.call_timeout(req, t).await
    }

    pub async fn notify(&self, req: &Req) -> Result<(), RpcError> {
        let c = self.client().await?;
        c.notify(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl RejectResponse for u64 {
        fn rejected(_: DbError) -> Option<Self> {
            None
        }
    }

    #[tokio::test]
    async fn concurrent_calls() {
        let (a, b) = tokio::io::duplex(1 << 20);
        let h: Handler<u64, u64> = Arc::new(|x: u64| {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(100 - x)).await;
                x * 2
            })
        });
        tokio::spawn(serve_stream(b, h, 1 << 20));
        let c: RpcClient<u64, u64> = RpcClient::from_stream(a, 1 << 20);
        let mut js = vec![];
        for i in 0..50u64 {
            let c = c.clone();
            js.push(tokio::spawn(async move { (i, c.call(&i).await.unwrap()) }));
        }
        for j in js {
            let (i, r) = j.await.unwrap();
            assert_eq!(r, i * 2);
        }
    }
}
