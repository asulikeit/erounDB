//! jamsil-store: SQLite 데이터·WAL·로컬 Recovery 소유 프로세스. RDB 파일은 이 프로세스만 수정한다.

use std::sync::Arc;
use std::time::Duration;

use jamsil_common::{DbError, DbResult};
use jamsil_ipc::msg::{
    SearchHit, SearchRequest, SearchResponse, SearchResult, StoreRequest, StoreResult,
};
use jamsil_ipc::rpc::{BoxFut, Handler, UnixClient};
use jamsil_storage::{Engine, EngineOptions, SearchProvider};

struct SearchClient {
    c: Arc<UnixClient<SearchRequest, SearchResult>>,
}

impl SearchProvider for SearchClient {
    fn search(
        &self,
        index_name: String,
        field: Option<String>,
        query: String,
        limit: usize,
        min_visible: u64,
        timeout_ms: u64,
    ) -> BoxFut<DbResult<Vec<SearchHit>>> {
        let req = SearchRequest::Query {
            index_name,
            field,
            query,
            limit,
            min_visible_index: min_visible,
            timeout_ms,
        };
        let c = self.c.clone();
        Box::pin(async move {
            match c
                .call_timeout(&req, Duration::from_millis(timeout_ms + 30_000))
                .await
            {
                Ok(Ok(SearchResponse::Hits(h))) => Ok(h),
                Ok(Ok(_)) => Err(DbError::internal("unexpected search response")),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(DbError::unavailable(format!(
                    "search process unavailable: {e}"
                ))),
            }
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cfg = jamsil_common::cli::load_config_or_exit("jamsil-store");
    let _g = jamsil_observability::init("jamsil-store", Some(&cfg));
    let opts = EngineOptions {
        db_path: cfg.sqlite_path(),
        cluster_id: if cfg.server.mode == jamsil_common::config::Mode::Cluster {
            cfg.cluster.cluster_id.clone()
        } else {
            format!("standalone:{}", cfg.cluster.cluster_id)
        },
        database: cfg.database.name.clone(),
        default_user: cfg
            .security
            .users
            .first()
            .map(|u| u.name.clone())
            .unwrap_or_else(|| "jamsil".into()),
        synchronous: cfg.storage.synchronous.clone(),
        reader_pool_size: cfg.storage.reader_pool_size,
        sqlite_max_length: cfg.sqlite_max_length(),
        text_max_bytes: cfg.text_max_bytes(),
        max_mutation_bytes: cfg.max_mutation_bytes(),
        write_lock_timeout: Duration::from_millis(cfg.storage.write_lock_timeout_ms),
        idle_tx_timeout: Duration::from_millis(cfg.storage.idle_tx_timeout_ms),
        search_max_candidates: cfg.search.max_candidates,
        search_committed_timeout_ms: cfg.search.committed_timeout_ms,
        integrity_check: true,
        principals: jamsil_common::acl::principals(&cfg),
    };
    let engine = match Engine::open(opts) {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("store recovery failed: {e}");
            eprintln!("jamsil-store: {e}");
            std::process::exit(if e.fatal {
                jamsil_common::cli::EXIT_PERMANENT
            } else {
                1
            });
        }
    };
    if cfg.search.enabled {
        engine.set_search_provider(Arc::new(SearchClient {
            c: Arc::new(UnixClient::new(cfg.search_socket())),
        }));
    }
    let e2 = engine.clone();
    let handler: Handler<StoreRequest, StoreResult> = Arc::new(move |req| {
        let e = e2.clone();
        Box::pin(async move { e.handle(req).await })
    });
    let sock = cfg.store_socket();
    tracing::info!("jamsil-store ready on {}", sock.display());
    tokio::select! {
        r = jamsil_ipc::serve_unix(&sock, handler) => {
            if let Err(e) = r {
                tracing::error!("store socket error: {e}");
                std::process::exit(1);
            }
        }
        _ = shutdown_signal() => {
            tracing::info!("jamsil-store shutting down");
        }
    }
    let _ = std::fs::remove_file(&sock);
    engine.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut t = signal(SignalKind::terminate()).unwrap();
    let mut i = signal(SignalKind::interrupt()).unwrap();
    tokio::select! { _ = t.recv() => {}, _ = i.recv() => {} }
}
