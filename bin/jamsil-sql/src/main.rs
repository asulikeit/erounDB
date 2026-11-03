//! jamsil-sql: TCP 9786 PostgreSQL Wire Protocol 엔드포인트.
//! 모든 노드가 클라이언트 연결을 받는다 (Active Endpoint). 쓰기는 jamsil-sync를 통해 Leader로 간다.

mod auth;
mod session;
mod vcatalog;

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, Semaphore};
use tokio_rustls::TlsAcceptor;

use jamsil_common::config::AccessPolicy;
use jamsil_common::{Config, Id128};
use jamsil_ipc::msg::{
    SearchRequest, SearchResult, StoreRequest, StoreResult, SyncRequest, SyncResult,
};
use jamsil_ipc::rpc::UnixClient;

pub struct SessionInfo {
    pub user: String,
    pub client_addr: String,
    pub application_name: String,
    pub started_at: String,
    pub state: String,
    pub query: String,
    pub query_started: Option<Instant>,
    pub last_latency_ms: u64,
}

pub struct ServerCtx {
    pub cfg: Config,
    pub store: UnixClient<StoreRequest, StoreResult>,
    pub sync: UnixClient<SyncRequest, SyncResult>,
    pub search: UnixClient<SearchRequest, SearchResult>,
    pub users: auth::UserDb,
    pub tls: Option<TlsAcceptor>,
    pub access: AccessPolicy,
    pub conn_limit: Arc<Semaphore>,
    pub cancels: Mutex<HashMap<(i32, i32), Arc<Notify>>>,
    pub sessions: Mutex<HashMap<Id128, SessionInfo>>,
    pub rate: Mutex<HashMap<IpAddr, (Instant, u32)>>,
    pub queries: AtomicU64,
    pub start_time: String,
}

impl ServerCtx {
    fn rate_ok(&self, ip: IpAddr) -> bool {
        let limit = self.cfg.network.connect_rate_per_ip;
        if limit == 0 {
            return true;
        }
        let mut m = self.rate.lock();
        let now = Instant::now();
        if m.len() > 100_000 {
            m.retain(|_, v| now.duration_since(v.0) < Duration::from_secs(5));
        }
        let e = m.entry(ip).or_insert((now, 0));
        if now.duration_since(e.0) > Duration::from_secs(1) {
            *e = (now, 0);
        }
        e.1 += 1;
        e.1 <= limit
    }
}

#[tokio::main]
async fn main() {
    let cfg = jamsil_common::cli::load_config_or_exit("jamsil-sql");
    let _g = jamsil_observability::init("jamsil-sql", Some(&cfg));
    for k in ["kind=read", "kind=write", "kind=ddl", "kind=other"] {
        jamsil_common::metrics::histogram("jamsil_sql_duration_seconds", k, "seconds");
    }
    let users = match auth::UserDb::from_config(&cfg) {
        Ok(u) => u,
        Err(e) => {
            tracing::error!("{e}");
            std::process::exit(jamsil_common::cli::EXIT_PERMANENT);
        }
    };
    let tls = if cfg.security.tls_enabled {
        let (cert, key) = match (&cfg.security.cert_file, &cfg.security.key_file) {
            (Some(c), Some(k)) if c.exists() && k.exists() => (c.clone(), k.clone()),
            _ => match jamsil_protocol::tls::ensure_self_signed(
                &cfg.data_dir().join("tls"),
                vec!["localhost".into(), cfg.server.name.clone()],
            ) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!("TLS setup failed: {e}");
                    std::process::exit(jamsil_common::cli::EXIT_PERMANENT);
                }
            },
        };
        match jamsil_protocol::tls::acceptor(&cert, &key) {
            Ok(a) => Some(a),
            Err(e) => {
                tracing::error!("TLS setup failed: {e}");
                std::process::exit(jamsil_common::cli::EXIT_PERMANENT);
            }
        }
    } else {
        None
    };
    let ctx = Arc::new(ServerCtx {
        store: UnixClient::new(cfg.store_socket()),
        sync: UnixClient::new(cfg.sync_socket()),
        search: UnixClient::new(cfg.search_socket()),
        users,
        tls,
        access: AccessPolicy::from_config(&cfg.network.access),
        conn_limit: Arc::new(Semaphore::new(cfg.network.max_connections)),
        cancels: Mutex::new(HashMap::new()),
        sessions: Mutex::new(HashMap::new()),
        rate: Mutex::new(HashMap::new()),
        queries: AtomicU64::new(0),
        start_time: session::now_text(),
        cfg: cfg.clone(),
    });

    let mut binds: Vec<SocketAddr> = vec![];
    if let Some(v4) = &cfg.network.listen_ipv4 {
        if let Ok(ip) = v4.parse::<IpAddr>() {
            binds.push(SocketAddr::new(ip, cfg.network.port));
        }
    }
    if let Some(v6) = &cfg.network.listen_ipv6 {
        if let Ok(ip) = v6.parse::<IpAddr>() {
            binds.push(SocketAddr::new(ip, cfg.network.port));
        }
    }
    let mut listeners = vec![];
    for b in binds {
        let sock = if b.is_ipv6() {
            tokio::net::TcpSocket::new_v6()
        } else {
            tokio::net::TcpSocket::new_v4()
        };
        let l = sock.and_then(|s| {
            s.set_reuseaddr(true)?;
            if b.is_ipv6() {
                // IPv4와 같은 포트를 따로 바인드하기 위해 v6only
                let raw = std::os::fd::AsRawFd::as_raw_fd(&s);
                let one: libc::c_int = 1;
                unsafe {
                    libc::setsockopt(
                        raw,
                        libc::IPPROTO_IPV6,
                        libc::IPV6_V6ONLY,
                        &one as *const libc::c_int as *const libc::c_void,
                        std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                    );
                }
            }
            s.bind(b)?;
            s.listen(1024)
        });
        match l {
            Ok(l) => {
                tracing::info!("jamsil-sql listening on {b}");
                listeners.push(l);
            }
            Err(e) => {
                tracing::error!("cannot listen on {b}: {e}");
                let permanent = matches!(e.kind(), std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::InvalidInput | std::io::ErrorKind::PermissionDenied);
                if permanent {
                    eprintln!("jamsil-sql: cannot listen on {b}: {e} (check network.listen_ipv4/listen_ipv6)");
                }
                std::process::exit(if permanent { jamsil_common::cli::EXIT_PERMANENT } else { 1 });
            }
        }
    }
    for l in listeners {
        let ctx = ctx.clone();
        tokio::spawn(accept_loop(l, ctx));
    }
    shutdown_signal().await;
    tracing::info!("jamsil-sql shutting down: refusing new connections");
}

async fn accept_loop(l: TcpListener, ctx: Arc<ServerCtx>) {
    loop {
        let (s, addr) = match l.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!("accept failed: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let ctx = ctx.clone();
        tokio::spawn(async move { handle_conn(s, addr, ctx).await });
    }
}

async fn handle_conn(s: TcpStream, addr: SocketAddr, ctx: Arc<ServerCtx>) {
    let _ = s.set_nodelay(true);
    let ip = addr.ip();
    if !ctx.rate_ok(ip) {
        jamsil_observability::audit!("connection rate limit exceeded for {ip}");
        return;
    }
    session::run(s, addr, ctx).await;
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut t = signal(SignalKind::terminate()).unwrap();
    let mut i = signal(SignalKind::interrupt()).unwrap();
    tokio::select! { _ = t.recv() => {}, _ = i.recv() => {} }
}
