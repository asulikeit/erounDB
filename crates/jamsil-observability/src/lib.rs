//! tracing 초기화와 jamsil-log 프로세스 전달.
//!
//! 일반 로그는 비동기·손실 허용 경로다 (설계서 3.2: jamsil-log는 WAL이 아니다).
//! 전달 큐가 가득 차거나 jamsil-log가 없으면 버린다. stderr(journald) 출력은 항상 유지한다.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use jamsil_common::Config;
use jamsil_ipc::msg::{LogCategory, LogRecord, LogRequest};

pub static DROPPED_LOGS: AtomicU64 = AtomicU64::new(0);

/// 감사 로그 (접속/인증/DDL 등).
#[macro_export]
macro_rules! audit {
    ($($arg:tt)*) => { tracing::info!(target: "audit", $($arg)*) };
}

/// 쿼리 로그.
#[macro_export]
macro_rules! query_log {
    ($($arg:tt)*) => { tracing::info!(target: "query", $($arg)*) };
}

struct MsgVisitor(String);

impl Visit for MsgVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            if !self.0.is_empty() {
                self.0.insert(0, ' ');
            }
            self.0.insert_str(0, &format!("{value:?}"));
        } else {
            self.0.push_str(&format!(" {}={:?}", field.name(), value));
        }
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            if !self.0.is_empty() {
                self.0.insert(0, ' ');
            }
            self.0.insert_str(0, value);
        } else {
            self.0.push_str(&format!(" {}={}", field.name(), value));
        }
    }
}

struct ForwardLayer {
    tx: SyncSender<LogRecord>,
    process: String,
    pid: u32,
}

impl<S: Subscriber> Layer<S> for ForwardLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        let target = meta.target();
        let category = if target == "audit" {
            LogCategory::Audit
        } else if target == "query" {
            LogCategory::Query
        } else if target.starts_with("jamsil_consensus")
            || target.starts_with("jamsil_sync")
            || target == "cluster"
        {
            LogCategory::Cluster
        } else {
            LogCategory::System
        };
        let mut v = MsgVisitor(String::new());
        event.record(&mut v);
        let rec = LogRecord {
            ts_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            process: self.process.clone(),
            pid: self.pid,
            level: meta.level().to_string(),
            category,
            target: target.to_string(),
            message: v.0,
        };
        if let Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) =
            self.tx.try_send(rec)
        {
            DROPPED_LOGS.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn frame(body: &[u8]) -> Vec<u8> {
    // jamsil-ipc frame: len u32 | flags u8 (LAST) | msg u64 | total u32 | body | crc32
    let mut out = Vec::with_capacity(body.len() + 21);
    out.extend_from_slice(&((body.len() + 4) as u32).to_le_bytes());
    out.push(1);
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(body);
    out.extend_from_slice(&crc32fast::hash(body).to_le_bytes());
    out
}

fn forwarder(rx: Receiver<LogRecord>, path: PathBuf) {
    let mut conn: Option<UnixStream> = None;
    let mut last_try = Instant::now() - Duration::from_secs(10);
    loop {
        let first = match rx.recv() {
            Ok(r) => r,
            Err(_) => return,
        };
        let mut batch = vec![first];
        while batch.len() < 512 {
            match rx.try_recv() {
                Ok(r) => batch.push(r),
                Err(_) => break,
            }
        }
        if conn.is_none() && last_try.elapsed() > Duration::from_secs(1) {
            last_try = Instant::now();
            conn = UnixStream::connect(&path).ok();
            if let Some(c) = &conn {
                let _ = c.set_write_timeout(Some(Duration::from_millis(500)));
            }
        }
        let Some(c) = conn.as_mut() else {
            DROPPED_LOGS.fetch_add(batch.len() as u64, Ordering::Relaxed);
            continue;
        };
        let mut body = vec![2u8]; // KIND_NOTIFY
        if bincode::serialize_into(&mut body, &LogRequest::Records(batch)).is_err() {
            continue;
        }
        if c.write_all(&frame(&body)).is_err() {
            conn = None;
        }
    }
}

pub struct LogGuard;

/// 프로세스 공통 로깅 초기화.
pub fn init(process: &str, cfg: Option<&Config>) -> LogGuard {
    let level = cfg
        .map(|c| c.logging.level.clone())
        .unwrap_or_else(|| "info".into());
    let filter = EnvFilter::try_from_env("JAMSIL_LOG").unwrap_or_else(|_| EnvFilter::new(level));
    let json = cfg.map(|c| c.logging.format == "json").unwrap_or(false);
    // 쿼리 로그는 jamsil-log(query.log)로만 보낸다: stderr(journald)에 문장마다 쓰지 않는다.
    let not_query = || tracing_subscriber::filter::filter_fn(|m| m.target() != "query");
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(true);
    let forward = match cfg {
        Some(c) if process != "jamsil-log" => {
            let (tx, rx) = sync_channel(8192);
            let path = c.log_socket();
            std::thread::Builder::new()
                .name("log-forward".into())
                .spawn(move || forwarder(rx, path))
                .ok();
            Some(ForwardLayer {
                tx,
                process: process.to_string(),
                pid: std::process::id(),
            })
        }
        _ => None,
    };
    let reg = tracing_subscriber::registry().with(filter).with(forward);
    if json {
        let _ = reg
            .with(fmt_layer.json().with_filter(not_query()))
            .try_init();
    } else {
        let _ = reg.with(fmt_layer.with_filter(not_query())).try_init();
    }
    tracing::info!("{process} {}", jamsil_common::build_info());
    LogGuard
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn record(n: u64) -> LogRecord {
        LogRecord {
            ts_unix_ms: n,
            process: "test".into(),
            pid: 1,
            level: "INFO".into(),
            category: LogCategory::Audit,
            target: "audit".into(),
            message: format!("record {n}"),
        }
    }

    fn notify_body(batch: Vec<LogRecord>) -> Vec<u8> {
        let mut body = vec![2u8]; // KIND_NOTIFY (forwarder와 동일)
        bincode::serialize_into(&mut body, &LogRequest::Records(batch)).unwrap();
        body
    }

    /// 0.1.0-3~7 회귀: 프레임 헤더에 total 필드가 없어 jamsil-log가 모든 로그를 버렸다.
    /// jamsil-ipc의 실제 리더로 forwarder가 만든 프레임을 해석한다.
    #[tokio::test]
    async fn forwarder_frame_is_readable_by_ipc_reader() {
        let body = notify_body(vec![record(1), record(2)]);
        let bytes = frame(&body);
        let mut rd = jamsil_ipc::frame::FrameReader::new(&bytes[..], 1 << 20, 1 << 20);
        let (_id, got) = rd.next().await.unwrap().expect("one message");
        assert_eq!(got, body);
        assert!(rd.next().await.unwrap().is_none(), "no trailing bytes");
    }

    /// forwarder → 실제 serve_unix 서버(jamsil-log와 같은 방식)까지 레코드가 도착해야 한다.
    #[tokio::test]
    async fn forwarder_delivers_records_to_unix_server() {
        let dir = std::env::temp_dir().join(format!("jamsil-obs-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("log.sock");
        let (got_tx, got_rx) = std::sync::mpsc::channel::<Vec<LogRecord>>();
        let got_tx = std::sync::Mutex::new(got_tx);
        let handler: jamsil_ipc::rpc::Handler<LogRequest, ()> = Arc::new(move |req| {
            let LogRequest::Records(r) = req;
            let _ = got_tx.lock().unwrap().send(r);
            Box::pin(async {})
        });
        let s2 = sock.clone();
        tokio::spawn(async move {
            let _ = jamsil_ipc::serve_unix(&s2, handler).await;
        });
        for _ in 0..100 {
            if sock.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let (tx, rx) = sync_channel(16);
        let p = sock.clone();
        std::thread::spawn(move || forwarder(rx, p));
        tx.send(record(7)).unwrap();
        let got = tokio::task::spawn_blocking(move || got_rx.recv_timeout(Duration::from_secs(5)))
            .await
            .unwrap()
            .expect("jamsil-log side received the batch");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].message, "record 7");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
