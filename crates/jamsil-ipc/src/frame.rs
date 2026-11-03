//! 프레임 포맷 (little-endian):
//!
//! ```text
//! +-----------+--------+-----------+-------------+------------------------------+
//! | len: u32  | flags  | msg: u64  | total: u32  | payload[len]                 |
//! +-----------+--------+-----------+-------------+------------------------------+
//! flags bit0 = LAST. LAST 프레임 payload의 마지막 4바이트는 메시지 전체 본문의 CRC32.
//! total = 메시지 전체 본문 길이. 수신 측은 첫 프레임의 본문을 읽기 전에 이 크기만큼
//! 메모리 예산을 예약한다 (RQ-11).
//! ```
//!
//! 프레임은 전송 단위일 뿐 영속 Chunk가 아니다. 부분 프레임만 받은 메시지는 버려진다.

use std::collections::HashMap;
use std::io;

use jamsil_common::budget::{self, BudgetGuard};
use jamsil_common::DbError;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const HEADER_LEN: usize = 17;
pub const FLAG_LAST: u8 = 1;

pub fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn header(plen: usize, last: bool, id: u64, total: usize) -> [u8; HEADER_LEN] {
    let mut hdr = [0u8; HEADER_LEN];
    hdr[0..4].copy_from_slice(&(plen as u32).to_le_bytes());
    hdr[4] = if last { FLAG_LAST } else { 0 };
    hdr[5..13].copy_from_slice(&id.to_le_bytes());
    hdr[13..17].copy_from_slice(&(total as u32).to_le_bytes());
    hdr
}

/// 메시지를 프레임으로 나눠 기록한다. 호출자는 같은 스트림에 대한 동시 기록을 직렬화해야 한다.
pub async fn write_message<W: AsyncWrite + Unpin>(
    w: &mut W,
    msg_id: u64,
    body: &[u8],
    frame_size: usize,
) -> io::Result<()> {
    let crc = crc32fast::hash(body);
    let frame_size = frame_size.max(64);
    let mut off = 0usize;
    loop {
        let remaining = body.len() - off;
        let last = remaining <= frame_size;
        let n = if last { remaining } else { frame_size };
        let plen = if last { n + 4 } else { n };
        let hdr = header(plen, last, msg_id, body.len());
        w.write_all(&hdr).await?;
        w.write_all(&body[off..off + n]).await?;
        if last {
            w.write_all(&crc.to_le_bytes()).await?;
            break;
        }
        off += n;
    }
    w.flush().await
}

/// 시험 전용 (T-11): 0이 아니면 8MiB 이상 메시지의 프레임을 이 개수만큼 보낸 뒤 프로세스를 abort한다.
static ABORT_AFTER_LARGE_FRAMES: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

pub fn arm_abort_after_large_frames(n: u32) {
    ABORT_AFTER_LARGE_FRAMES.store(n, std::sync::atomic::Ordering::SeqCst);
}

/// 여러 메시지의 프레임을 번갈아 기록하는 송신기.
/// 대형 메시지(예: 32MiB TEXT가 포함된 Raft Entry)가 Heartbeat 같은 작은 메시지를 막지 않도록 한다.
pub struct InterleavedWriter {
    queue: std::collections::VecDeque<(u64, Vec<u8>, usize)>,
    frame_size: usize,
}

impl InterleavedWriter {
    pub fn new(frame_size: usize) -> Self {
        InterleavedWriter {
            queue: Default::default(),
            frame_size: frame_size.max(64),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// 작은 메시지는 앞에 둔다.
    pub fn push(&mut self, id: u64, body: Vec<u8>) {
        if body.len() <= self.frame_size {
            let pos = self
                .queue
                .iter()
                .position(|(_, b, _)| b.len() > self.frame_size)
                .unwrap_or(self.queue.len());
            self.queue.insert(pos, (id, body, 0));
        } else {
            self.queue.push_back((id, body, 0));
        }
    }

    /// 프레임 하나를 기록한다 (메시지가 끝나면 큐에서 제거, 아니면 뒤로 보낸다).
    pub async fn write_one<W: AsyncWrite + Unpin>(&mut self, w: &mut W) -> io::Result<()> {
        let Some((id, body, off)) = self.queue.pop_front() else {
            return Ok(());
        };
        let remaining = body.len() - off;
        let last = remaining <= self.frame_size;
        let n = if last { remaining } else { self.frame_size };
        let plen = if last { n + 4 } else { n };
        let hdr = header(plen, last, id, body.len());
        let mut buf = Vec::with_capacity(HEADER_LEN + plen);
        buf.extend_from_slice(&hdr);
        buf.extend_from_slice(&body[off..off + n]);
        if last {
            buf.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
        }
        w.write_all(&buf).await?;
        if body.len() >= 8 << 20 {
            use std::sync::atomic::Ordering;
            let left = ABORT_AFTER_LARGE_FRAMES.load(Ordering::SeqCst);
            if left > 0 {
                if left == 1 {
                    let _ = w.flush().await;
                    jamsil_common::fault::crash("crash_mid_large_append");
                }
                ABORT_AFTER_LARGE_FRAMES.store(left - 1, Ordering::SeqCst);
            }
        }
        if !last {
            self.queue.push_back((id, body, off + n));
        }
        if self.queue.is_empty() {
            w.flush().await?;
        }
        Ok(())
    }
}

/// 재조립 결과.
#[derive(Debug)]
pub enum Incoming {
    /// 완전한 메시지와 그 메모리 예약 (처리가 끝날 때까지 유지)
    Message { id: u64, body: Vec<u8>, guard: BudgetGuard },
    /// 메모리 예산 부족으로 본문을 버린 메시지
    Rejected { id: u64, error: DbError },
}

/// 프레임을 읽어 완전한 메시지로 재조립한다.
pub struct FrameReader<R> {
    inner: R,
    partial: HashMap<u64, (Vec<u8>, BudgetGuard)>,
    skipping: HashMap<u64, DbError>,
    partial_bytes: usize,
    max_message: usize,
    max_frame: usize,
    budget: Option<&'static budget::ByteBudget>,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub fn new(inner: R, max_message: usize, max_frame: usize) -> Self {
        FrameReader {
            inner,
            partial: HashMap::new(),
            skipping: HashMap::new(),
            partial_bytes: 0,
            max_message,
            max_frame,
            budget: None,
        }
    }

    /// 대형 메시지를 프로세스 메모리 예산(`jamsil_common::budget`)으로 제한한다.
    pub fn with_budget(self) -> Self {
        self.with_budget_from(budget::global())
    }

    pub fn with_budget_from(mut self, b: &'static budget::ByteBudget) -> Self {
        self.budget = Some(b);
        self
    }

    /// 다음 완전한 메시지. EOF면 None. (예산 미사용 리더 전용)
    pub async fn next(&mut self) -> io::Result<Option<(u64, Vec<u8>)>> {
        loop {
            match self.next_incoming().await? {
                None => return Ok(None),
                Some(Incoming::Message { id, body, .. }) => return Ok(Some((id, body))),
                Some(Incoming::Rejected { .. }) => continue,
            }
        }
    }

    pub async fn next_incoming(&mut self) -> io::Result<Option<Incoming>> {
        loop {
            let mut hdr = [0u8; HEADER_LEN];
            match self.inner.read_exact(&mut hdr).await {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
            let flags = hdr[4];
            let id = u64::from_le_bytes(hdr[5..13].try_into().unwrap());
            let total = u32::from_le_bytes(hdr[13..17].try_into().unwrap()) as usize;
            if len > self.max_frame + 4 {
                return Err(invalid(format!("frame too large: {len}")));
            }
            if total > self.max_message {
                return Err(invalid(format!(
                    "message too large: {total} > {} bytes",
                    self.max_message
                )));
            }
            let last = flags & FLAG_LAST != 0;
            if last && len < 4 {
                return Err(invalid("last frame without checksum"));
            }
            let body_len = if last { len - 4 } else { len };
            // 예산 부족으로 버리는 중인 메시지: 본문을 읽어 버린다.
            if let Some(err) = self.skipping.get(&id) {
                let err = err.clone();
                self.discard(len).await?;
                if last {
                    self.skipping.remove(&id);
                    return Ok(Some(Incoming::Rejected { id, error: err }));
                }
                continue;
            }
            let (mut buf, guard) = match self.partial.remove(&id) {
                Some(p) => p,
                None => {
                    // 새 메시지의 첫 프레임: 본문을 읽기 전에 전체 크기를 예약
                    let guard = if let Some(b) = self.budget {
                        match b.try_reserve(total) {
                            Ok(g) => g,
                            Err(e) => {
                                self.discard(len).await?;
                                if last {
                                    return Ok(Some(Incoming::Rejected { id, error: e }));
                                }
                                self.skipping.insert(id, e);
                                continue;
                            }
                        }
                    } else {
                        BudgetGuard::none()
                    };
                    (Vec::with_capacity(total.min(self.max_message)), guard)
                }
            };
            self.partial_bytes -= buf.len().min(self.partial_bytes);
            if buf.len() + body_len > self.max_message || buf.len() + body_len > total {
                return Err(invalid(format!(
                    "message larger than announced: > {total} bytes"
                )));
            }
            let start = buf.len();
            buf.resize(start + body_len, 0);
            self.inner.read_exact(&mut buf[start..]).await?;
            if last {
                let mut c = [0u8; 4];
                self.inner.read_exact(&mut c).await?;
                let want = u32::from_le_bytes(c);
                if crc32fast::hash(&buf) != want {
                    return Err(invalid("message checksum mismatch"));
                }
                return Ok(Some(Incoming::Message { id, body: buf, guard }));
            }
            self.partial_bytes += buf.len();
            if self.partial_bytes > self.max_message * 4 {
                return Err(invalid("too many partial messages in flight"));
            }
            self.partial.insert(id, (buf, guard));
        }
    }

    async fn discard(&mut self, mut n: usize) -> io::Result<()> {
        let mut scratch = [0u8; 8192];
        while n > 0 {
            let k = n.min(scratch.len());
            self.inner.read_exact(&mut scratch[..k]).await?;
            n -= k;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrip_large_message() {
        let (a, b) = tokio::io::duplex(1 << 16);
        let body: Vec<u8> = (0..3_000_123u32).map(|i| (i % 251) as u8).collect();
        let body2 = body.clone();
        let w = tokio::spawn(async move {
            let mut a = a;
            write_message(&mut a, 7, &body2, 1 << 20).await.unwrap();
            write_message(&mut a, 8, b"", 1 << 20).await.unwrap();
        });
        let mut r = FrameReader::new(b, 64 << 20, 1 << 20);
        let (id, got) = r.next().await.unwrap().unwrap();
        assert_eq!(id, 7);
        assert_eq!(got, body);
        let (id, got) = r.next().await.unwrap().unwrap();
        assert_eq!(id, 8);
        assert!(got.is_empty());
        w.await.unwrap();
    }

    #[tokio::test]
    async fn partial_message_is_never_delivered() {
        let (a, b) = tokio::io::duplex(1 << 16);
        tokio::spawn(async move {
            let mut a = a;
            // 첫 프레임만 쓰고 연결 종료
            let mut hdr = [0u8; HEADER_LEN];
            hdr[0..4].copy_from_slice(&(100u32).to_le_bytes());
            hdr[5..13].copy_from_slice(&1u64.to_le_bytes());
            hdr[13..17].copy_from_slice(&(200u32).to_le_bytes());
            a.write_all(&hdr).await.unwrap();
            a.write_all(&[0u8; 100]).await.unwrap();
        });
        let mut r = FrameReader::new(b, 1 << 20, 1 << 20);
        assert!(r.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn corrupted_checksum_rejected() {
        let (a, b) = tokio::io::duplex(1 << 16);
        tokio::spawn(async move {
            let mut a = a;
            let mut hdr = [0u8; HEADER_LEN];
            hdr[0..4].copy_from_slice(&(8u32).to_le_bytes());
            hdr[4] = FLAG_LAST;
            hdr[13..17].copy_from_slice(&(4u32).to_le_bytes());
            a.write_all(&hdr).await.unwrap();
            a.write_all(&[1, 2, 3, 4, 0, 0, 0, 0]).await.unwrap();
        });
        let mut r = FrameReader::new(b, 1 << 20, 1 << 20);
        assert!(r.next().await.is_err());
    }

    #[tokio::test]
    async fn over_budget_message_is_rejected_without_blocking_others() {
        let budget = budget::ByteBudget::leaked(2 << 20);
        let (a, b) = tokio::io::duplex(1 << 16);
        let big: Vec<u8> = vec![7u8; 3 << 20];
        tokio::spawn(async move {
            let mut a = a;
            write_message(&mut a, 1, &big, 1 << 20).await.unwrap();
            write_message(&mut a, 2, b"small", 1 << 20).await.unwrap();
        });
        let mut r = FrameReader::new(b, 64 << 20, 1 << 20).with_budget_from(budget);
        match r.next_incoming().await.unwrap().unwrap() {
            Incoming::Rejected { id, error } => {
                assert_eq!(id, 1);
                assert_eq!(error.sqlstate, "54000");
            }
            other => panic!("{other:?}"),
        }
        match r.next_incoming().await.unwrap().unwrap() {
            Incoming::Message { id, body, .. } => {
                assert_eq!(id, 2);
                assert_eq!(body, b"small");
            }
            other => panic!("{other:?}"),
        }
    }
}
