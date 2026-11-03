//! jamsild: 노드 Supervisor (설계서 3.3).
//!
//! 시작 순서: 설정·Lock 검사 → jamsil-log → jamsil-store(로컬 복구) → jamsil-sync(Raft 복구/선출)
//! → jamsil-search(별도 Ready) → jamsil-sql(SQL Ready). 종료는 역순.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::process::{Child, Command};

use jamsil_common::cli::EXIT_PERMANENT;
use jamsil_common::config::Mode;
use jamsil_common::Config;
use jamsil_ipc::msg::{
    ProcessStatus, StoreRequest, StoreResult, SupervisorRequest, SupervisorResponse, SyncRequest,
    SyncResult,
};
use jamsil_ipc::rpc::{Handler, UnixClient};

const ORDER: [&str; 5] = [
    "jamsil-log",
    "jamsil-store",
    "jamsil-sync",
    "jamsil-search",
    "jamsil-sql",
];

#[derive(Default)]
struct ProcState {
    pid: Option<u32>,
    restarts: u32,
    state: String,
    started: Option<Instant>,
    permanent_failure: bool,
}

struct Supervisor {
    cfg: Config,
    config_path: PathBuf,
    bin_dir: PathBuf,
    procs: Mutex<HashMap<&'static str, ProcState>>,
    shutting_down: std::sync::atomic::AtomicBool,
}

fn usage() -> ! {
    eprintln!(
        "usage:\n  jamsild --config <file>             run the node supervisor\n  jamsild hash-password <password>    print a SCRAM-SHA-256 secret for security.users\n  jamsild status --config <file>      show process status\n  jamsild version"
    );
    std::process::exit(2);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("hash-password") => {
            let Some(p) = args.get(2) else { usage() };
            println!(
                "{}",
                jamsil_protocol::scram::ScramSecret::generate(p).to_pg_string()
            );
        }
        Some("version") | Some("--version") => {
            println!("jamsilDB {}", jamsil_common::build_info());
        }
        Some("status") => {
            let cfg = jamsil_common::cli::load_config_or_exit("jamsild");
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let c: UnixClient<SupervisorRequest, SupervisorResponse> = UnixClient::new(cfg.supervisor_socket());
                match c.call_timeout(&SupervisorRequest::Status, Duration::from_secs(5)).await {
                    Ok(SupervisorResponse::Status { node_id, processes }) => {
                        println!("node {node_id}");
                        for p in processes {
                            println!(
                                "  {:<14} {:<10} pid={:<8} restarts={}",
                                p.name,
                                p.state,
                                p.pid.map(|x| x.to_string()).unwrap_or("-".into()),
                                p.restarts
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("jamsild is not running: {e}");
                        std::process::exit(1);
                    }
                }
                let sync: UnixClient<SyncRequest, SyncResult> = UnixClient::new(cfg.sync_socket());
                match sync.call_timeout(&SyncRequest::Status, Duration::from_secs(5)).await {
                    Ok(Ok(jamsil_ipc::msg::SyncResponse::Status(s))) => {
                        println!(
                            "raft: role={} term={} leader={:?} commit={} last_log={} snapshot={} store_applied={} quorum={} ready={}",
                            s.role, s.term, s.leader_id, s.commit_index, s.last_log_index, s.snapshot_index,
                            s.store_applied_index, s.quorum_available, s.leader_ready
                        );
                        for p in s.peers {
                            println!(
                                "  peer {} {} connected={} match={} next={} last_contact_ms={:?}",
                                p.node_id, p.address, p.connected, p.match_index, p.next_index, p.last_contact_ms
                            );
                        }
                    }
                    other => println!("raft: unavailable ({other:?})"),
                }
                let store: UnixClient<StoreRequest, StoreResult> = UnixClient::new(cfg.store_socket());
                match store.call_timeout(&StoreRequest::Status, Duration::from_secs(5)).await {
                    Ok(Ok(jamsil_ipc::msg::StoreResponse::Status(s))) => println!(
                        "store: applied={} schema_version={} db_bytes={} wal_bytes={} outbox_seq={} quarantined={:?}",
                        s.applied_index, s.schema_version, s.db_bytes, s.wal_bytes, s.outbox_seq, s.quarantined
                    ),
                    other => println!("store: unavailable ({other:?})"),
                }
            });
        }
        Some("--config") | Some("-c") => run(),
        Some(a) if a.starts_with("--config=") => run(),
        _ => usage(),
    }
}

fn run() {
    let cfg = jamsil_common::cli::load_config_or_exit("jamsild");
    let config_path = cfg.source_path.clone().unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let _g = jamsil_observability::init("jamsild", Some(&cfg));
        if let Err(e) = prepare_dirs(&cfg) {
            tracing::error!("{e}");
            eprintln!("jamsild: {e}");
            std::process::exit(EXIT_PERMANENT);
        }
        let _lock = match lock_data_dir(&cfg) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("jamsild: {e}");
                std::process::exit(EXIT_PERMANENT);
            }
        };
        if let Err(e) = check_metadata(&cfg) {
            tracing::error!("{e}");
            eprintln!("jamsild: {e}");
            std::process::exit(EXIT_PERMANENT);
        }
        let bin_dir = cfg.server.bin_dir.clone().unwrap_or_else(|| {
            std::env::current_exe()
                .ok()
                .and_then(|p| p.parent().map(|p| p.to_path_buf()))
                .unwrap_or_else(|| ".".into())
        });
        let sup = Arc::new(Supervisor {
            cfg: cfg.clone(),
            config_path,
            bin_dir,
            procs: Mutex::new(HashMap::new()),
            shutting_down: std::sync::atomic::AtomicBool::new(false),
        });
        // 상태 소켓
        let s2 = sup.clone();
        let handler: Handler<SupervisorRequest, SupervisorResponse> = Arc::new(move |_req| {
            let s = s2.clone();
            Box::pin(async move {
                let procs = s.procs.lock();
                SupervisorResponse::Status {
                    node_id: s.cfg.server.node_id,
                    processes: ORDER
                        .iter()
                        .filter_map(|n| {
                            procs.get(n).map(|p| ProcessStatus {
                                name: n.to_string(),
                                pid: p.pid,
                                running: p.pid.is_some(),
                                restarts: p.restarts,
                                state: p.state.clone(),
                            })
                        })
                        .collect(),
                }
            })
        });
        let sock = cfg.supervisor_socket();
        tokio::spawn(async move {
            let _ = jamsil_ipc::serve_unix(&sock, handler).await;
        });

        tracing::info!(
            "jamsild starting node {} ({:?}) data_dir={}",
            cfg.server.node_id,
            cfg.server.mode,
            cfg.data_dir().display()
        );
        let mut handles = vec![];
        for name in ORDER {
            if name == "jamsil-search" && !cfg.search.enabled {
                continue;
            }
            let (tx, rx) = tokio::sync::oneshot::channel();
            handles.push(tokio::spawn(sup.clone().keep_running(name, tx)));
            // 준비 대기 (search는 SQL Ready를 막지 않는다)
            if name != "jamsil-search" {
                // 준비되지 않은 채로 오래 걸리면 systemd에 실패를 알려 원인을 바로 볼 수 있게 한다
                // (Store는 큰 DB 복구에 오래 걸릴 수 있어 더 길게 기다린다).
                let limit = Duration::from_secs(if name == "jamsil-store" { 900 } else { 120 });
                if tokio::time::timeout(limit, rx).await.is_err() {
                    tracing::error!("{name} did not become ready within {limit:?}; giving up (see the messages above)");
                    sd_notify(&format!("STATUS={name} did not become ready"));
                    sup.shutdown().await;
                    std::process::exit(1);
                }
            }
        }
        sd_notify("READY=1");
        tracing::info!(
            "node {} SQL endpoint ready on port {}",
            cfg.server.node_id,
            cfg.network.port
        );
        shutdown_signal().await;
        sd_notify("STOPPING=1");
        sup.shutdown().await;
        for h in handles {
            h.abort();
        }
        let _ = std::fs::remove_file(cfg.supervisor_socket());
    });
}

fn prepare_dirs(cfg: &Config) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let d = cfg.data_dir();
    for sub in ["metadata", "rdb", "raft", "search", "tmp", "tls"] {
        std::fs::create_dir_all(d.join(sub))
            .map_err(|e| format!("cannot create {}: {e}", d.join(sub).display()))?;
    }
    std::fs::set_permissions(d, std::fs::Permissions::from_mode(0o700))
        .map_err(|e| format!("chmod {}: {e}", d.display()))?;
    std::fs::create_dir_all(cfg.run_dir())
        .map_err(|e| format!("cannot create {}: {e}", cfg.run_dir().display()))?;
    std::fs::create_dir_all(&cfg.logging.directory)
        .map_err(|e| format!("cannot create {}: {e}", cfg.logging.directory.display()))?;
    Ok(())
}

struct DirLock(#[allow(dead_code)] std::fs::File);

fn lock_data_dir(cfg: &Config) -> Result<DirLock, String> {
    use std::os::fd::AsRawFd;
    let p = cfg.metadata_dir().join("lock");
    let f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&p)
        .map_err(|e| format!("{}: {e}", p.display()))?;
    let r = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if r != 0 {
        return Err(format!(
            "data directory {} is in use by another jamsild",
            cfg.data_dir().display()
        ));
    }
    let _ = std::fs::write(&p, format!("{}\n", std::process::id()));
    Ok(DirLock(f))
}

/// cluster.meta: Cluster ID / Node ID / 포맷 버전이 바뀐 데이터 디렉터리로는 기동하지 않는다.
fn check_metadata(cfg: &Config) -> Result<(), String> {
    let p = cfg.metadata_dir().join("cluster.meta");
    let want = serde_json::json!({
        "cluster_id": cfg.cluster.cluster_id,
        "node_id": cfg.server.node_id,
        "mode": if cfg.server.mode == Mode::Cluster { "cluster" } else { "standalone" },
        "format_version": jamsil_common::FORMAT_VERSION,
    });
    match std::fs::read_to_string(&p) {
        Ok(s) => {
            let have: serde_json::Value =
                serde_json::from_str(&s).map_err(|e| format!("corrupted {}: {e}", p.display()))?;
            for k in ["cluster_id", "node_id", "mode", "format_version"] {
                if have.get(k) != want.get(k) {
                    return Err(format!(
                        "data directory metadata mismatch for {k}: on disk {}, configured {} — refusing to start",
                        have.get(k).cloned().unwrap_or_default(),
                        want[k]
                    ));
                }
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let tmp = p.with_extension("tmp");
            std::fs::write(&tmp, serde_json::to_vec_pretty(&want).unwrap())
                .map_err(|e| e.to_string())?;
            std::fs::File::open(&tmp)
                .and_then(|f| f.sync_all())
                .map_err(|e| e.to_string())?;
            std::fs::rename(&tmp, &p).map_err(|e| e.to_string())?;
            let node = cfg.metadata_dir().join("node.meta");
            let _ = std::fs::write(node, format!("{{\"node_id\": {}}}\n", cfg.server.node_id));
            Ok(())
        }
        Err(e) => Err(format!("{}: {e}", p.display())),
    }
}

fn sd_notify(msg: &str) {
    let Ok(path) = std::env::var("NOTIFY_SOCKET") else {
        return;
    };
    let path = if let Some(abs) = path.strip_prefix('@') {
        format!("\0{abs}")
    } else {
        path
    };
    if let Ok(s) = std::os::unix::net::UnixDatagram::unbound() {
        let _ = s.send_to(msg.as_bytes(), Path::new(&path));
    }
}

impl Supervisor {
    fn spawn(&self, name: &str) -> std::io::Result<Child> {
        let mut cmd = Command::new(self.bin_dir.join(name));
        cmd.arg("--config")
            .arg(&self.config_path)
            .env("JAMSIL_SUPERVISED", "1")
            .kill_on_drop(true);
        unsafe {
            cmd.pre_exec(|| {
                // Supervisor가 죽으면 자식도 종료 (고아 프로세스 방지)
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
                Ok(())
            });
        }
        cmd.spawn()
    }

    async fn wait_ready(&self, name: &str) -> bool {
        let deadline =
            Instant::now() + Duration::from_secs(if name == "jamsil-store" { 600 } else { 60 });
        while Instant::now() < deadline {
            let ok = match name {
                "jamsil-log" => self.cfg.log_socket().exists(),
                "jamsil-store" => {
                    let c: UnixClient<StoreRequest, StoreResult> =
                        UnixClient::new(self.cfg.store_socket());
                    matches!(
                        c.call_timeout(
                            &StoreRequest::Hello {
                                protocol_version: jamsil_ipc::msg::IPC_PROTOCOL_VERSION
                            },
                            Duration::from_secs(2)
                        )
                        .await,
                        Ok(Ok(_))
                    )
                }
                "jamsil-sync" => {
                    let c: UnixClient<SyncRequest, SyncResult> =
                        UnixClient::new(self.cfg.sync_socket());
                    matches!(
                        c.call_timeout(&SyncRequest::Status, Duration::from_secs(2))
                            .await,
                        Ok(Ok(_))
                    )
                }
                "jamsil-sql" => {
                    let host = self
                        .cfg
                        .network
                        .listen_ipv4
                        .clone()
                        .unwrap_or_else(|| "127.0.0.1".into());
                    let host = if host == "0.0.0.0" {
                        "127.0.0.1".to_string()
                    } else {
                        host
                    };
                    tokio::net::TcpStream::connect((host.as_str(), self.cfg.network.port))
                        .await
                        .is_ok()
                }
                _ => true,
            };
            if ok {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    fn set(&self, name: &'static str, f: impl FnOnce(&mut ProcState)) {
        let mut m = self.procs.lock();
        f(m.entry(name).or_default());
    }

    /// 자식 프로세스 하나를 계속 실행 (Crash 재시작, 영구 실패는 중지).
    async fn keep_running(
        self: Arc<Self>,
        name: &'static str,
        ready: tokio::sync::oneshot::Sender<()>,
    ) {
        let mut ready = Some(ready);
        let mut backoff = Duration::from_millis(500);
        loop {
            if self.shutting_down.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            let mut child = match self.spawn(name) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("cannot start {name}: {e}");
                    self.set(name, |p| p.state = format!("spawn-failed: {e}"));
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            let pid = child.id();
            self.set(name, |p| {
                p.pid = pid;
                p.state = "starting".into();
                p.started = Some(Instant::now());
            });
            tracing::info!("started {name} pid={}", pid.unwrap_or(0));
            let started = Instant::now();
            let ready_fut = self.wait_ready(name);
            tokio::pin!(ready_fut);
            let status = tokio::select! {
                st = child.wait() => st,
                ok = &mut ready_fut => {
                    if ok {
                        self.set(name, |p| p.state = "ready".into());
                        if let Some(r) = ready.take() {
                            let _ = r.send(());
                        }
                    } else {
                        tracing::error!("{name} did not become ready in time");
                    }
                    child.wait().await
                }
            };
            self.set(name, |p| p.pid = None);
            if self.shutting_down.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            let code = status.as_ref().ok().and_then(|s| s.code());
            if code == Some(EXIT_PERMANENT) {
                tracing::error!(
                    "{name} failed permanently (exit {EXIT_PERMANENT}); not restarting"
                );
                self.set(name, |p| {
                    p.state = "failed".into();
                    p.permanent_failure = true;
                });
                if name == "jamsil-store" || name == "jamsil-sync" || name == "jamsil-log" {
                    // 잘못된 데이터로 서비스하지 않는다: SQL 엔드포인트를 먼저 내리고 노드 전체를 영구 실패로 종료
                    tracing::error!("node is not serving SQL because {name} cannot recover");
                    self.shutdown().await;
                    sd_notify("STATUS=permanent failure");
                    std::process::exit(EXIT_PERMANENT);
                }
                if let Some(r) = ready.take() {
                    let _ = r.send(());
                }
                return;
            }
            tracing::warn!("{name} exited ({status:?}); restarting in {backoff:?}");
            self.set(name, |p| {
                p.restarts += 1;
                p.state = "restarting".into();
            });
            if started.elapsed() > Duration::from_secs(60) {
                backoff = Duration::from_millis(500);
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    }

    async fn shutdown(&self) {
        tracing::info!("graceful shutdown: stopping SQL first");
        self.shutting_down
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let timeout = Duration::from_millis(self.cfg.server.shutdown_timeout_ms);
        for name in ORDER.iter().rev() {
            let pid = self.procs.lock().get(name).and_then(|p| p.pid);
            let Some(pid) = pid else { continue };
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
            let deadline = Instant::now() + timeout;
            loop {
                let alive = unsafe { libc::kill(pid as i32, 0) } == 0;
                if !alive || self.procs.lock().get(name).and_then(|p| p.pid).is_none() {
                    break;
                }
                if Instant::now() > deadline {
                    tracing::warn!("{name} did not stop in time; killing");
                    unsafe {
                        libc::kill(pid as i32, libc::SIGKILL);
                    }
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            tracing::info!("stopped {name}");
        }
    }
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut t = signal(SignalKind::terminate()).unwrap();
    let mut i = signal(SignalKind::interrupt()).unwrap();
    tokio::select! { _ = t.recv() => {}, _ = i.recv() => {} }
}
