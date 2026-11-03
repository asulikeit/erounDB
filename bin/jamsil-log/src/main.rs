//! jamsil-log: 일반·클러스터·쿼리·감사 로그 집계 (WAL 아님, 손실 허용).

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::mpsc;

use jamsil_ipc::msg::{LogCategory, LogRecord, LogRequest};
use jamsil_ipc::rpc::Handler;

const ROTATE_BYTES: u64 = 128 * 1024 * 1024;
const KEEP_FILES: usize = 5;

struct Sink {
    path: PathBuf,
    w: BufWriter<File>,
    size: u64,
}

impl Sink {
    fn open(path: PathBuf) -> std::io::Result<Sink> {
        let f = OpenOptions::new().create(true).append(true).open(&path)?;
        let size = f.metadata()?.len();
        Ok(Sink {
            path,
            w: BufWriter::new(f),
            size,
        })
    }

    fn write(&mut self, line: &str) -> std::io::Result<()> {
        if self.size + line.len() as u64 > ROTATE_BYTES {
            self.w.flush()?;
            for i in (1..KEEP_FILES).rev() {
                let from = rotated(&self.path, i);
                let _ = std::fs::rename(&from, rotated(&self.path, i + 1));
            }
            std::fs::rename(&self.path, rotated(&self.path, 1))?;
            *self = Sink::open(self.path.clone())?;
        }
        self.w.write_all(line.as_bytes())?;
        self.w.write_all(b"\n")?;
        self.size += line.len() as u64 + 1;
        Ok(())
    }
}

fn rotated(p: &Path, i: usize) -> PathBuf {
    PathBuf::from(format!("{}.{i}", p.display()))
}

fn line(r: &LogRecord) -> String {
    serde_json::json!({
        "ts": r.ts_unix_ms,
        "process": r.process,
        "pid": r.pid,
        "level": r.level,
        "target": r.target,
        "msg": r.message,
    })
    .to_string()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cfg = jamsil_common::cli::load_config_or_exit("jamsil-log");
    let _g = jamsil_observability::init("jamsil-log", Some(&cfg));
    let dir = cfg.logging.directory.clone();
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("jamsil-log: cannot create {}: {e}", dir.display());
        std::process::exit(jamsil_common::cli::EXIT_PERMANENT);
    }
    let (tx, mut rx) = mpsc::channel::<Vec<LogRecord>>(1024);
    let d2 = dir.clone();
    let writer = std::thread::spawn(move || {
        let mut sinks: Vec<Option<Sink>> = vec![None, None, None, None];
        let names = ["system.log", "cluster.log", "query.log", "audit.log"];
        while let Some(batch) = rx.blocking_recv() {
            for r in &batch {
                let i = match r.category {
                    LogCategory::System => 0,
                    LogCategory::Cluster => 1,
                    LogCategory::Query => 2,
                    LogCategory::Audit => 3,
                };
                if sinks[i].is_none() {
                    sinks[i] = Sink::open(d2.join(names[i])).ok();
                }
                if let Some(s) = sinks[i].as_mut() {
                    if s.write(&line(r)).is_err() {
                        sinks[i] = None;
                    }
                }
            }
            for s in sinks.iter_mut().flatten() {
                let _ = s.w.flush();
            }
        }
    });
    let handler: Handler<LogRequest, ()> = Arc::new(move |req| {
        let tx = tx.clone();
        Box::pin(async move {
            match req {
                LogRequest::Records(r) => {
                    let _ = tx.try_send(r);
                }
            }
        })
    });
    tracing::info!("jamsil-log writing to {}", dir.display());
    let sock = cfg.log_socket();
    tokio::select! {
        r = jamsil_ipc::serve_unix(&sock, handler) => {
            if let Err(e) = r {
                eprintln!("jamsil-log: {e}");
                std::process::exit(1);
            }
        }
        _ = shutdown_signal() => {}
    }
    let _ = std::fs::remove_file(&sock);
    drop(writer);
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut t = signal(SignalKind::terminate()).unwrap();
    let mut i = signal(SignalKind::interrupt()).unwrap();
    tokio::select! { _ = t.recv() => {}, _ = i.recv() => {} }
}
