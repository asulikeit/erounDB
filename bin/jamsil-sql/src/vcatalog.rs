//! `jamsil_catalog.*` 시스템 뷰 (설계서 12.3). 질의 시점에 각 프로세스 상태를 모아 가상 테이블로 전달한다.

use std::time::Duration;

use jamsil_common::{id_to_string, Value};
use jamsil_ipc::msg::{
    SearchRequest, SearchResponse, StoreRequest, StoreResponse, SyncRequest, SyncResponse,
    VirtualTable,
};
use jamsil_sql::types::oid;

use crate::ServerCtx;

fn t(s: impl Into<String>) -> Value {
    Value::Text(s.into())
}
fn i(n: u64) -> Value {
    Value::Int(n as i64)
}
fn b(x: bool) -> Value {
    Value::Int(x as i64)
}

pub async fn build(ctx: &ServerCtx, sql_lower: &str) -> Vec<VirtualTable> {
    let to = Duration::from_secs(5);
    let cluster = match ctx.sync.call_timeout(&SyncRequest::Status, to).await {
        Ok(Ok(SyncResponse::Status(s))) => Some(s),
        _ => None,
    };
    let store = match ctx.store.call_timeout(&StoreRequest::Status, to).await {
        Ok(Ok(StoreResponse::Status(s))) => Some(s),
        _ => None,
    };
    let search = if ctx.cfg.search.enabled {
        match ctx.search.call_timeout(&SearchRequest::Status, to).await {
            Ok(Ok(SearchResponse::Status(s))) => Some(s),
            _ => None,
        }
    } else {
        None
    };
    let me = ctx.cfg.server.node_id;
    let mut out = vec![];

    // cluster_nodes
    let mut rows = vec![];
    if let Some(c) = &cluster {
        rows.push(vec![
            i(me),
            t(&c.role),
            i(c.term),
            t(if store.as_ref().map(|s| s.ready).unwrap_or(false) {
                "up"
            } else {
                "store-unavailable"
            }),
            b(store.as_ref().map(|s| s.ready).unwrap_or(false) && c.quorum_available),
            Value::Int(0),
            t(ctx.cfg.peer_address(me).unwrap_or("local")),
            c.leader_id.map(i).unwrap_or(Value::Null),
        ]);
        for p in &c.peers {
            let role = if c.leader_id == Some(p.node_id) {
                "leader"
            } else {
                "follower"
            };
            rows.push(vec![
                i(p.node_id),
                t(role),
                i(c.term),
                t(
                    if p.connected || p.last_contact_ms.map(|x| x < 5000).unwrap_or(false) {
                        "up"
                    } else if c.role != "leader" && c.leader_id != Some(p.node_id) {
                        // Follower는 다른 Follower와 직접 통신하지 않아 상태를 알 수 없다.
                        "n/a (follower view)"
                    } else {
                        "unreachable"
                    },
                ),
                Value::Null,
                p.last_contact_ms.map(i).unwrap_or(Value::Null),
                t(&p.address),
                c.leader_id.map(i).unwrap_or(Value::Null),
            ]);
        }
    }
    out.push(VirtualTable {
        name: "jamsil_catalog_cluster_nodes".into(),
        columns: vec![
            ("node_id".into(), oid::INT8),
            ("role".into(), oid::TEXT),
            ("term".into(), oid::INT8),
            ("status".into(), oid::TEXT),
            ("sql_ready".into(), oid::BOOL),
            ("last_heartbeat_ms".into(), oid::INT8),
            ("address".into(), oid::TEXT),
            ("leader_id".into(), oid::INT8),
        ],
        rows,
    });

    // replication_status
    let mut rows = vec![];
    if let Some(c) = &cluster {
        rows.push(vec![
            i(me),
            i(c.commit_index),
            i(c.store_applied_index),
            i(c.last_log_index),
            i(c.commit_index.saturating_sub(c.store_applied_index)),
            i(c.fsync_errors),
            i(c.last_log_index),
            i(c.snapshot_index),
        ]);
        for p in &c.peers {
            rows.push(vec![
                i(p.node_id),
                i(c.commit_index),
                Value::Null,
                i(p.match_index),
                i(c.last_log_index.saturating_sub(p.match_index)),
                Value::Null,
                Value::Null,
                Value::Null,
            ]);
        }
    }
    out.push(VirtualTable {
        name: "jamsil_catalog_replication_status".into(),
        columns: vec![
            ("node_id".into(), oid::INT8),
            ("commit_index".into(), oid::INT8),
            ("applied_index".into(), oid::INT8),
            ("match_index".into(), oid::INT8),
            ("lag_entries".into(), oid::INT8),
            ("fsync_errors".into(), oid::INT8),
            ("last_log_index".into(), oid::INT8),
            ("snapshot_index".into(), oid::INT8),
        ],
        rows,
    });

    // storage_status
    let mut rows = vec![];
    if let Some(s) = &store {
        rows.push(vec![
            i(me),
            i(s.db_bytes),
            i(s.wal_bytes),
            i(s.text_bytes_estimated),
            t(match &s.quarantined {
                Some(q) => format!("quarantined: {q}"),
                None => "ok".into(),
            }),
            i(s.applied_index),
            i(s.schema_version),
            i(s.outbox_seq),
            s.writer_owner.clone().map(t).unwrap_or(Value::Null),
        ]);
    }
    out.push(VirtualTable {
        name: "jamsil_catalog_storage_status".into(),
        columns: vec![
            ("node_id".into(), oid::INT8),
            ("bytes_used".into(), oid::INT8),
            ("wal_bytes".into(), oid::INT8),
            ("text_bytes_estimated".into(), oid::INT8),
            ("recovery_state".into(), oid::TEXT),
            ("applied_index".into(), oid::INT8),
            ("schema_version".into(), oid::INT8),
            ("outbox_seq".into(), oid::INT8),
            ("writer_owner".into(), oid::TEXT),
        ],
        rows,
    });

    // search_indexes
    let mut rows = vec![];
    if let Some(s) = &search {
        for ix in &s.indexes {
            rows.push(vec![
                t(&ix.name),
                t(&ix.table),
                t(ix.columns.join(",")),
                t(&ix.analyzer),
                i(store.as_ref().map(|x| x.schema_version).unwrap_or(0)),
                t(format!("{}-v1", ix.analyzer)),
                i(s.visible_index),
                i(s.store_applied_index.saturating_sub(s.visible_index)),
                t(&ix.state),
                i(ix.docs),
            ]);
        }
    }
    out.push(VirtualTable {
        name: "jamsil_catalog_search_indexes".into(),
        columns: vec![
            ("index_id".into(), oid::TEXT),
            ("table_name".into(), oid::TEXT),
            ("columns".into(), oid::TEXT),
            ("analyzer".into(), oid::TEXT),
            ("schema_version".into(), oid::INT8),
            ("analyzer_version".into(), oid::TEXT),
            ("visible_commit_index".into(), oid::INT8),
            ("index_lag_entries".into(), oid::INT8),
            ("state".into(), oid::TEXT),
            ("docs".into(), oid::INT8),
        ],
        rows,
    });

    // replica_checksum (요청 시에만 계산: 전체 데이터 스캔)
    if sql_lower.contains("replica_checksum") {
        let mut rows = vec![];
        if let Ok(Ok(StoreResponse::Checksum {
            applied_index,
            checksum,
        })) = ctx
            .store
            .call_timeout(&StoreRequest::Checksum, Duration::from_secs(600))
            .await
        {
            rows.push(vec![i(me), i(applied_index), t(checksum)]);
        }
        out.push(VirtualTable {
            name: "jamsil_catalog_replica_checksum".into(),
            columns: vec![
                ("node_id".into(), oid::INT8),
                ("applied_index".into(), oid::INT8),
                ("checksum".into(), oid::TEXT),
            ],
            rows,
        });
    }

    // metrics (설계서 12.4 / RQ-12): (node_id, process, name, labels, value, unit)
    {
        use jamsil_common::metrics::MetricSample as M;
        let mut rows: Vec<(&str, M)> = vec![];
        let mut sql = vec![
            M::new("jamsil_sql_connections", "", ctx.sessions.lock().len() as f64, "count"),
            M::new(
                "jamsil_sql_queries_total",
                "",
                ctx.queries.load(std::sync::atomic::Ordering::Relaxed) as f64,
                "count",
            ),
            M::new(
                "jamsil_log_dropped_total",
                "",
                jamsil_observability::DROPPED_LOGS.load(std::sync::atomic::Ordering::Relaxed) as f64,
                "count",
            ),
        ];
        sql.extend(jamsil_common::metrics::process_samples());
        sql.extend(jamsil_ipc::rpc::ipc_samples());
        rows.extend(sql.into_iter().map(|m| ("jamsil-sql", m)));
        if let Some(c) = &cluster {
            let mut v = vec![
                M::new("jamsil_raft_leader", "", c.leader_id.map(|x| x as f64).unwrap_or(f64::NAN), "node"),
                M::new("jamsil_raft_term", "", c.term as f64, "count"),
                M::new("jamsil_raft_commit_index", "", c.commit_index as f64, "index"),
                M::new("jamsil_raft_applied_index", "", c.store_applied_index as f64, "index"),
                M::new("jamsil_raft_quorum_available", "", c.quorum_available as u8 as f64, "bool"),
                M::new("jamsil_raft_snapshot_index", "", c.snapshot_index as f64, "index"),
            ];
            v.extend(c.metrics.iter().cloned());
            rows.extend(v.into_iter().map(|m| ("jamsil-sync", m)));
        }
        if let Some(s) = &store {
            let mut v = vec![
                M::new("jamsil_storage_db_bytes", "", s.db_bytes as f64, "bytes"),
                M::new("jamsil_storage_wal_bytes", "", s.wal_bytes as f64, "bytes"),
                M::new("jamsil_storage_reads_total", "", s.queries_total as f64, "count"),
                M::new("jamsil_storage_writes_total", "", s.writes_total as f64, "count"),
                M::new("jamsil_text_size_rejected_total", "", s.text_rejected_total as f64, "count"),
                M::new("jamsil_store_quarantined", "", s.quarantined.is_some() as u8 as f64, "bool"),
            ];
            v.extend(s.metrics.iter().cloned());
            rows.extend(v.into_iter().map(|m| ("jamsil-store", m)));
        }
        if let Some(s) = &search {
            let mut v = vec![
                M::new("jamsil_search_visible_index", "", s.visible_index as f64, "index"),
                M::new(
                    "jamsil_search_lag_entries",
                    "",
                    s.store_applied_index.saturating_sub(s.visible_index) as f64,
                    "entries",
                ),
            ];
            v.extend(s.metrics.iter().cloned());
            rows.extend(v.into_iter().map(|m| ("jamsil-search", m)));
        }
        out.push(VirtualTable {
            name: "jamsil_catalog_metrics".into(),
            columns: vec![
                ("node_id".into(), oid::INT8),
                ("process".into(), oid::TEXT),
                ("name".into(), oid::TEXT),
                ("labels".into(), oid::TEXT),
                ("value".into(), oid::FLOAT8),
                ("unit".into(), oid::TEXT),
            ],
            rows: rows
                .into_iter()
                .map(|(p, m)| {
                    vec![
                        i(me),
                        t(p),
                        t(m.name),
                        t(m.labels),
                        if m.value.is_nan() { Value::Null } else { Value::Real(m.value) },
                        t(m.unit),
                    ]
                })
                .collect(),
        });
    }

    // active_sessions
    let rows: Vec<Vec<Value>> = ctx
        .sessions
        .lock()
        .iter()
        .map(|(id, s)| {
            vec![
                t(id_to_string(*id)),
                t(&s.state),
                i(me),
                t(&s.user),
                t(&s.client_addr),
                t(&s.application_name),
                t(&s.started_at),
                i(s.query_started
                    .map(|q| q.elapsed().as_millis() as u64)
                    .unwrap_or(s.last_latency_ms)),
                t(&s.query),
            ]
        })
        .collect();
    out.push(VirtualTable {
        name: "jamsil_catalog_active_sessions".into(),
        columns: vec![
            ("session_id".into(), oid::TEXT),
            ("state".into(), oid::TEXT),
            ("node_id".into(), oid::INT8),
            ("user_name".into(), oid::TEXT),
            ("client_addr".into(), oid::TEXT),
            ("application_name".into(), oid::TEXT),
            ("started_at".into(), oid::TIMESTAMPTZ),
            ("query_latency_ms".into(), oid::INT8),
            ("current_query".into(), oid::TEXT),
        ],
        rows,
    });
    out
}
