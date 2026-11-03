//! jamsil-search: 검색 인덱스 소유 프로세스. Store의 Outbox(Change Feed)를 순서대로 소비해 색인한다.
//! 이 프로세스가 중단돼도 SQL 일반 기능은 유지되고 TEXT_MATCH만 실패한다 (설계서 3.3).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use jamsil_common::{Config, DbError};
use jamsil_ipc::msg::{
    ChangeOp, OutboxBatch, SearchRequest, SearchResponse, SearchResult, SearchStatus, StoreRequest,
    StoreResponse, StoreResult,
};
use jamsil_ipc::rpc::{Handler, UnixClient};
use jamsil_search_engine::{Checkpoint, Indexer, IndexerConfig, SearchShared};

struct State {
    shared: Arc<SearchShared>,
    visible: watch::Sender<u64>,
    store_applied: AtomicU64,
    outbox_seq: AtomicU64,
    /// 색인 Writer들에 할당된 메모리 합계
    writer_memory: AtomicU64,
    state: parking_lot::Mutex<String>,
    rebuild: AtomicBool,
}

type Store = UnixClient<StoreRequest, StoreResult>;

fn store_call(
    rt: &tokio::runtime::Handle,
    store: &Store,
    req: StoreRequest,
) -> Result<StoreResponse, DbError> {
    match rt.block_on(store.call_timeout(&req, Duration::from_secs(120))) {
        Ok(r) => r,
        Err(e) => Err(DbError::unavailable(format!("store unavailable: {e}"))),
    }
}

fn read_batch(
    rt: &tokio::runtime::Handle,
    store: &Store,
    after: u64,
    cfg: &Config,
    max_events: usize,
) -> Result<OutboxBatch, DbError> {
    match store_call(
        rt,
        store,
        StoreRequest::OutboxRead {
            after_seq: after,
            max_events,
            max_bytes: cfg.search.indexing.batch_bytes_mib * 1024 * 1024,
        },
    )? {
        StoreResponse::Outbox(b) => Ok(b),
        _ => Err(DbError::internal("unexpected store response")),
    }
}

/// 한 인덱스 전체 색인 (RDB Row Scan).
fn scan_index(
    rt: &tokio::runtime::Handle,
    store: &Store,
    ix: &mut Indexer,
    name: &str,
    cfg: &Config,
) -> Result<u64, DbError> {
    ix.mark_building(name);
    let mut after = None;
    let mut n = 0u64;
    loop {
        let req = StoreRequest::ScanRows {
            index_name: name.to_string(),
            after_pk: after.clone(),
            limit: 512,
            max_bytes: cfg.search.indexing.batch_bytes_mib * 1024 * 1024,
        };
        let b = match store_call(rt, store, req)? {
            StoreResponse::Rows(b) => b,
            _ => return Err(DbError::internal("unexpected store response")),
        };
        for (pk, texts) in &b.rows {
            ix.upsert(name, pk, texts)?;
            n += 1;
        }
        if let Some((pk, _)) = b.rows.last() {
            after = Some(pk.clone());
        }
        if b.done || b.rows.is_empty() {
            break;
        }
    }
    tracing::info!("search index {name}: indexed {n} rows from RDB");
    Ok(n)
}

/// 전체 재색인: 신규 세대로 모든 인덱스를 다시 만든다 (Snapshot 설치·손상·세대 변경).
fn full_rebuild(
    rt: &tokio::runtime::Handle,
    store: &Store,
    ix: &mut Indexer,
    cfg: &Config,
) -> Result<Checkpoint, DbError> {
    let head = read_batch(rt, store, u64::MAX >> 1, cfg, 0)?;
    tracing::warn!(
        "full search rebuild: epoch {} applied_index {} outbox seq {}",
        head.rebuild_epoch,
        head.applied_index,
        head.max_seq
    );
    ix.reset_all();
    for def in &head.indexes {
        ix.ensure_index(def, true)?;
        match scan_index(rt, store, ix, &def.name, cfg) {
            Ok(_) => {}
            Err(e) if e.code == "JDB-4101" => ix.drop_index(&def.name),
            Err(e) => return Err(e),
        }
    }
    let ck = Checkpoint {
        seq: head.max_seq,
        visible_index: head.applied_index,
        epoch: head.rebuild_epoch,
        indexes: Default::default(),
    };
    ix.commit(ck.clone())?;
    ix.gc_dirs();
    Ok(ck)
}

fn indexer_loop(rt: tokio::runtime::Handle, cfg: Config, st: Arc<State>) {
    let store: Store = UnixClient::new(cfg.store_socket());
    let icfg = IndexerConfig {
        root: cfg.search_dir(),
        memory_budget_bytes: cfg.search.indexing.max_memory_mib * 1024 * 1024,
        threads: cfg.search.indexing.workers,
        max_tokens_per_doc: cfg.search.indexing.max_tokens_per_doc,
        max_token_bytes: cfg.search.indexing.max_token_bytes,
    };
    let mut ix = match Indexer::open(icfg, st.shared.clone()) {
        Ok(i) => i,
        Err(e) => {
            tracing::error!("cannot open search indexes: {e}");
            std::process::exit(1);
        }
    };
    let interval = Duration::from_millis(cfg.search.indexing.commit_interval_ms.max(10));
    let max_events = cfg.search.indexing.batch_events.max(1);
    // 기동: Store 대기 후 인덱스 열기 / 필요 시 재색인
    let mut progress;
    loop {
        match read_batch(&rt, &store, u64::MAX >> 1, &cfg, 0) {
            Ok(head) => {
                let r = (|| -> Result<u64, DbError> {
                    if head.rebuild_epoch != ix.ckpt.epoch || head.max_seq < ix.ckpt.seq {
                        let ck = full_rebuild(&rt, &store, &mut ix, &cfg)?;
                        return Ok(ck.seq);
                    }
                    // 정의와 디렉터리 동기화
                    let existing: Vec<String> = ix.ckpt.indexes.keys().cloned().collect();
                    for def in &head.indexes {
                        if ix.ensure_index(def, false)? {
                            scan_index(&rt, &store, &mut ix, &def.name, &cfg)?;
                        }
                    }
                    for name in existing {
                        if !head.indexes.iter().any(|d| d.name == name) {
                            ix.drop_index(&name);
                        }
                    }
                    if ix.any_dirty() {
                        let ck = Checkpoint {
                            epoch: head.rebuild_epoch,
                            ..ix.ckpt.clone()
                        };
                        ix.commit(ck)?;
                    }
                    ix.gc_dirs();
                    Ok(ix.ckpt.seq)
                })();
                match r {
                    Ok(seq) => {
                        progress = seq;
                        break;
                    }
                    Err(e) => {
                        tracing::error!("search startup failed: {e}");
                        std::thread::sleep(Duration::from_secs(2));
                    }
                }
            }
            Err(e) => {
                tracing::info!("waiting for store: {e}");
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
    *st.state.lock() = "ready".into();
    st.visible.send_replace(ix.ckpt.visible_index);
    let mut pending_visible = ix.ckpt.visible_index;
    let mut dirty_since: Option<Instant> = None;
    tracing::info!(
        "jamsil-search ready: seq {} visible {}",
        ix.ckpt.seq,
        ix.ckpt.visible_index
    );
    loop {
        if st.rebuild.swap(false, Ordering::SeqCst) {
            *st.state.lock() = "rebuilding".into();
            match full_rebuild(&rt, &store, &mut ix, &cfg) {
                Ok(ck) => {
                    progress = ck.seq;
                    pending_visible = ck.visible_index;
                    st.visible.send_replace(ck.visible_index);
                }
                Err(e) => tracing::error!("rebuild failed: {e}"),
            }
            *st.state.lock() = "ready".into();
        }
        let batch = match read_batch(&rt, &store, progress, &cfg, max_events) {
            Ok(b) => b,
            Err(e) => {
                tracing::debug!("outbox read failed: {e}");
                *st.state.lock() = "store-unavailable".into();
                std::thread::sleep(Duration::from_millis(500));
                continue;
            }
        };
        if batch.rebuild_epoch != ix.ckpt.epoch {
            st.rebuild.store(true, Ordering::SeqCst);
            continue;
        }
        *st.state.lock() = "ready".into();
        st.store_applied
            .store(batch.applied_index, Ordering::Relaxed);
        let r = (|| -> Result<(), DbError> {
            for ev in &batch.events {
                match ev.op {
                    ChangeOp::IndexCreated => {
                        if let Some(def) = batch.indexes.iter().find(|d| d.name == ev.index_name) {
                            ix.ensure_index(def, true)?;
                            match scan_index(&rt, &store, &mut ix, &def.name, &cfg) {
                                Err(e) if e.code == "JDB-4101" => ix.drop_index(&def.name),
                                r => {
                                    r?;
                                }
                            }
                        }
                    }
                    ChangeOp::IndexDropped => {
                        let still = batch.indexes.iter().any(|d| {
                            d.name == ev.index_name
                                && ix.has_index(&d.name)
                                && ix
                                    .defs()
                                    .iter()
                                    .any(|x| x.name == d.name && x.created_index == d.created_index)
                        });
                        if !still {
                            ix.drop_index(&ev.index_name);
                        }
                    }
                    ChangeOp::Upsert => match &ev.texts {
                        Some(t) => ix.upsert(&ev.index_name, &ev.pk, t)?,
                        None => ix.delete(&ev.index_name, &ev.pk),
                    },
                    ChangeOp::Delete => ix.delete(&ev.index_name, &ev.pk),
                }
                progress = ev.seq;
            }
            Ok(())
        })();
        if let Err(e) = r {
            tracing::error!("indexing failed: {e}; scheduling rebuild");
            st.rebuild.store(true, Ordering::SeqCst);
            continue;
        }
        // 이 배치까지 처리했을 때 반영이 보장되는 RDB index
        let candidate = if batch.drained {
            batch.applied_index
        } else {
            batch
                .events
                .last()
                .map(|e| e.raft_index.saturating_sub(1))
                .unwrap_or(pending_visible)
        };
        pending_visible = pending_visible.max(candidate);
        if ix.any_dirty() {
            let since = *dirty_since.get_or_insert_with(Instant::now);
            if batch.drained && since.elapsed() >= interval || since.elapsed() >= interval * 4 {
                let ck = Checkpoint {
                    seq: progress,
                    visible_index: pending_visible,
                    epoch: batch.rebuild_epoch,
                    indexes: Default::default(),
                };
                match ix.commit(ck) {
                    Ok(()) => {
                        dirty_since = None;
                        st.writer_memory.store(ix.allocated_budget() as u64, Ordering::Relaxed);
                        st.visible.send_replace(pending_visible);
                        st.outbox_seq.store(progress, Ordering::Relaxed);
                        let _ =
                            store_call(&rt, &store, StoreRequest::OutboxAck { upto_seq: progress });
                    }
                    Err(e) => {
                        tracing::error!("search commit failed: {e}; scheduling rebuild");
                        st.rebuild.store(true, Ordering::SeqCst);
                    }
                }
            }
        } else {
            // 미반영 변경이 없으면 현재 진행 위치까지 검색 결과가 최신이다.
            st.visible.send_replace(pending_visible);
            if progress > ix.ckpt.seq && batch.drained {
                // 다른 테이블 이벤트만 있었던 경우 등: 체크포인트를 가볍게 전진
                let ck = Checkpoint {
                    seq: progress,
                    visible_index: pending_visible,
                    epoch: batch.rebuild_epoch,
                    indexes: Default::default(),
                };
                if ix.commit(ck).is_ok() {
                    let _ = store_call(&rt, &store, StoreRequest::OutboxAck { upto_seq: progress });
                }
            }
        }
        if batch.drained {
            std::thread::sleep(Duration::from_millis(if dirty_since.is_some() {
                20
            } else {
                50
            }));
        }
    }
}

#[tokio::main]
async fn main() {
    let cfg = jamsil_common::cli::load_config_or_exit("jamsil-search");
    let _g = jamsil_observability::init("jamsil-search", Some(&cfg));
    jamsil_common::metrics::histogram("jamsil_search_query_duration_seconds", "", "seconds");
    let (vtx, _) = watch::channel(0u64);
    let st = Arc::new(State {
        shared: Arc::new(SearchShared::default()),
        visible: vtx,
        store_applied: AtomicU64::new(0),
        outbox_seq: AtomicU64::new(0),
        writer_memory: AtomicU64::new(0),
        state: parking_lot::Mutex::new("starting".into()),
        rebuild: AtomicBool::new(false),
    });
    let rt = tokio::runtime::Handle::current();
    let (c2, s2) = (cfg.clone(), st.clone());
    std::thread::Builder::new()
        .name("search-indexer".into())
        .spawn(move || indexer_loop(rt, c2, s2))
        .unwrap();

    let s3 = st.clone();
    let handler: Handler<SearchRequest, SearchResult> = Arc::new(move |req| {
        let st = s3.clone();
        Box::pin(async move {
            match req {
                SearchRequest::Status => Ok(SearchResponse::Status(SearchStatus {
                    state: st.state.lock().clone(),
                    visible_index: *st.visible.borrow(),
                    store_applied_index: st.store_applied.load(Ordering::Relaxed),
                    outbox_seq: st.outbox_seq.load(Ordering::Relaxed),
                    indexes: st.shared.status(),
                    metrics: {
                        use jamsil_common::metrics::MetricSample as M;
                        let mut m = jamsil_common::metrics::process_samples();
                        m.extend(jamsil_ipc::rpc::ipc_samples());
                        m.push(M::new(
                            "jamsil_search_writer_memory_bytes",
                            "",
                            st.writer_memory.load(Ordering::Relaxed) as f64,
                            "bytes",
                        ));
                        m
                    },
                })),
                SearchRequest::Rebuild => {
                    st.rebuild.store(true, Ordering::SeqCst);
                    Ok(SearchResponse::Ok)
                }
                SearchRequest::Query {
                    index_name,
                    field,
                    query,
                    limit,
                    min_visible_index,
                    timeout_ms,
                } => {
                    if min_visible_index > 0 {
                        let mut rx = st.visible.subscribe();
                        let deadline =
                            tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
                        loop {
                            let v = *rx.borrow_and_update();
                            if v >= min_visible_index {
                                break;
                            }
                            match tokio::time::timeout_at(deadline, rx.changed()).await {
                                Ok(Ok(())) => {}
                                _ => {
                                    return Err(DbError::search_lag_timeout(
                                        min_visible_index,
                                        *st.visible.borrow(),
                                    ))
                                }
                            }
                        }
                    }
                    let h = st.shared.get(&index_name).ok_or_else(|| {
                        DbError::search_index_missing(format!(
                            "{index_name} is not loaded on this node"
                        ))
                    })?;
                    let t0 = std::time::Instant::now();
                    let hits = tokio::task::spawn_blocking(move || h.search(&query, field.as_deref(), limit))
                        .await
                        .map_err(|e| DbError::internal(e.to_string()))?;
                    jamsil_common::metrics::histogram("jamsil_search_query_duration_seconds", "", "seconds")
                        .record_duration(t0.elapsed());
                    let hits = hits?;
                    Ok(SearchResponse::Hits(hits))
                }
            }
        })
    });
    let sock = cfg.search_socket();
    tracing::info!("jamsil-search serving {}", sock.display());
    tokio::select! {
        r = jamsil_ipc::serve_unix(&sock, handler) => {
            if let Err(e) = r {
                tracing::error!("search socket error: {e}");
                std::process::exit(1);
            }
        }
        _ = shutdown_signal() => {}
    }
    let _ = std::fs::remove_file(&sock);
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut t = signal(SignalKind::terminate()).unwrap();
    let mut i = signal(SignalKind::interrupt()).unwrap();
    tokio::select! { _ = t.recv() => {}, _ = i.recv() => {} }
}
