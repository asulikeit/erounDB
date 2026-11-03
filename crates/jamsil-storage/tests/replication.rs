//! G-02: SQLite 미확정 실행 → Patchset → (Raft 확정 가정) → 원자 Apply.

use std::sync::Arc;
use std::time::Duration;

use jamsil_common::{new_id, ReplicatedCommand, TxnPayload, Value};
use jamsil_ipc::msg::{SearchConsistency, Statement, StoreRequest, StoreResponse};
use jamsil_storage::{Engine, EngineOptions};

fn opts(dir: &std::path::Path) -> EngineOptions {
    EngineOptions {
        db_path: dir.join("rdb/main.sqlite"),
        cluster_id: "test".into(),
        database: "jamsil".into(),
        default_user: "jamsil".into(),
        synchronous: "NORMAL".into(),
        reader_pool_size: 2,
        sqlite_max_length: 64 << 20,
        text_max_bytes: 32 << 20,
        max_mutation_bytes: 64 << 20,
        write_lock_timeout: Duration::from_secs(5),
        idle_tx_timeout: Duration::from_secs(60),
        search_max_candidates: 1000,
        search_committed_timeout_ms: 1000,
        integrity_check: true,
        principals: Default::default(),
    }
}

fn st(sql: &str, params: Vec<Value>) -> Statement {
    Statement {
        sql: sql.into(),
        params,
        param_types: vec![],
        user: String::new(),
    }
}

async fn exec(
    e: &Arc<Engine>,
    txn: u128,
    sql: &str,
    params: Vec<Value>,
) -> Result<jamsil_common::QueryResult, jamsil_common::DbError> {
    exec_ext(e, txn, sql, params, None, None).await
}

async fn exec_ext(
    e: &Arc<Engine>,
    txn: u128,
    sql: &str,
    params: Vec<Value>,
    request_id: Option<&str>,
    expect_applied: Option<u64>,
) -> Result<jamsil_common::QueryResult, jamsil_common::DbError> {
    match e
        .handle(StoreRequest::TxExec {
            txn,
            stmt: st(sql, params),
            lock_timeout_ms: 0,
            request_id: request_id.map(|s| s.to_string()),
            expect_applied,
        })
        .await?
    {
        StoreResponse::Result(r) => Ok(r),
        _ => panic!(),
    }
}

async fn prepare(e: &Arc<Engine>, txn: u128) -> Result<Option<TxnPayload>, jamsil_common::DbError> {
    Ok(prepare_cmd(e, txn, None).await?.map(|c| c.payload().unwrap().clone()))
}

async fn prepare_cmd(
    e: &Arc<Engine>,
    txn: u128,
    request_id: Option<&str>,
) -> Result<Option<ReplicatedCommand>, jamsil_common::DbError> {
    match e
        .handle(StoreRequest::TxPrepare {
            txn,
            request_id: request_id.map(|s| s.to_string()),
        })
        .await?
    {
        StoreResponse::Prepared(p) => Ok(p),
        _ => panic!(),
    }
}

async fn apply(e: &Arc<Engine>, index: u64, cmd: ReplicatedCommand) {
    match e
        .handle(StoreRequest::Apply {
            index,
            term: 1,
            cmd,
        })
        .await
        .unwrap()
    {
        StoreResponse::Applied { index: i } => assert_eq!(i, index),
        _ => panic!(),
    }
}

async fn query(e: &Arc<Engine>, sql: &str) -> jamsil_common::QueryResult {
    match e
        .handle(StoreRequest::Query {
            session: 1,
            stmt: st(sql, vec![]),
            min_applied: 0,
            wait_timeout_ms: 1000,
            search_consistency: SearchConsistency::Eventual,
            virtual_tables: vec![],
            snapshot: None,
        })
        .await
        .unwrap()
    {
        StoreResponse::Result(r) => r,
        _ => panic!(),
    }
}

async fn checksum(e: &Arc<Engine>) -> (u64, String) {
    match e.handle(StoreRequest::Checksum).await.unwrap() {
        StoreResponse::Checksum {
            applied_index,
            checksum,
        } => (applied_index, checksum),
        _ => panic!(),
    }
}

/// Leader에서 실행+Prepare+자기 Apply, Follower에는 같은 Entry 적용.
async fn commit(
    leader: &Arc<Engine>,
    follower: &Arc<Engine>,
    index: &mut u64,
    stmts: &[(&str, Vec<Value>)],
) -> Option<TxnPayload> {
    let txn = new_id();
    for (s, p) in stmts {
        exec(leader, txn, s, p.clone()).await.unwrap();
    }
    let p = prepare(leader, txn).await.unwrap()?;
    *index += 1;
    let cmd = ReplicatedCommand::Txn(p.clone());
    apply(leader, *index, cmd.clone()).await;
    apply(follower, *index, cmd).await;
    Some(p)
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_follower_patchset_replication() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let l = Engine::open(opts(d1.path())).unwrap();
    let f = Engine::open(opts(d2.path())).unwrap();
    let mut idx = 0u64;
    idx += 1;
    apply(&l, idx, ReplicatedCommand::Noop).await;
    apply(&f, idx, ReplicatedCommand::Noop).await;

    commit(&l, &f, &mut idx, &[("CREATE TABLE documents (id BIGINT PRIMARY KEY, title VARCHAR(200), content TEXT, at TIMESTAMPTZ DEFAULT now())", vec![])]).await;
    commit(
        &l,
        &f,
        &mut idx,
        &[(
            "CREATE SEARCH INDEX idx_doc ON documents(content) WITH (analyzer = 'korean')",
            vec![],
        )],
    )
    .await;
    let emoji = "한글 👨‍👩‍👧‍👦 emoji ✔️".to_string();
    commit(
        &l,
        &f,
        &mut idx,
        &[
            (
                "INSERT INTO documents (id, title, content) VALUES ($1, $2, $3)",
                vec![
                    Value::Int(1),
                    Value::Text("제목".into()),
                    Value::Text(emoji.clone()),
                ],
            ),
            (
                "INSERT INTO documents (id, title, content) VALUES (2, 'b', 'bbb')",
                vec![],
            ),
            ("UPDATE documents SET title = 'bb' WHERE id = 2", vec![]),
        ],
    )
    .await;
    commit(
        &l,
        &f,
        &mut idx,
        &[("DELETE FROM documents WHERE id = 2", vec![])],
    )
    .await;

    // 동일 checksum, 동일 데이터 (비결정 DEFAULT now() 값도 Leader 값으로 복제)
    let a = checksum(&l).await;
    let b = checksum(&f).await;
    assert_eq!(a, b);
    let r = query(&f, "SELECT id, content, at FROM documents ORDER BY id").await;
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][1], Value::Text(emoji));
    let rl = query(&l, "SELECT at FROM documents").await;
    assert_eq!(rl.rows[0][0], r.rows[0][2]);

    // 변경 없는 트랜잭션은 복제 대상 없음
    let txn = new_id();
    exec(
        &l,
        txn,
        "UPDATE documents SET title = title WHERE id = 999",
        vec![],
    )
    .await
    .unwrap();
    assert!(prepare(&l, txn).await.unwrap().is_none());

    // Outbox: IndexCreated + Upsert(1) + Upsert(2) + Delete(2)
    match f
        .handle(StoreRequest::OutboxRead {
            after_seq: 0,
            max_events: 100,
            max_bytes: 1 << 20,
        })
        .await
        .unwrap()
    {
        StoreResponse::Outbox(b) => {
            let ops: Vec<_> = b.events.iter().map(|e| e.op).collect();
            use jamsil_ipc::msg::ChangeOp::*;
            assert_eq!(
                ops,
                vec![IndexCreated, Upsert, Upsert, Delete],
                "{:?}",
                b.events
            );
            assert!(b.drained);
            assert_eq!(b.applied_index, idx);
        }
        _ => panic!(),
    }

    // TEXT LIKE 금지
    let txn = new_id();
    let e = exec(
        &l,
        txn,
        "SELECT id FROM documents WHERE content LIKE '%a%'",
        vec![],
    )
    .await
    .unwrap_err();
    assert_eq!(e.code, "JDB-4102");
    l.handle(StoreRequest::TxAbort { txn }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn text_32mib_boundary_and_abort() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let l = Engine::open(opts(d1.path())).unwrap();
    let f = Engine::open(opts(d2.path())).unwrap();
    let mut idx = 0;
    commit(
        &l,
        &f,
        &mut idx,
        &[(
            "CREATE TABLE t (id INT PRIMARY KEY, body TEXT, b2 TEXT)",
            vec![],
        )],
    )
    .await;

    // 정확히 33,554,432 bytes: 성공 (한글 3바이트 + 1바이트 패딩으로 정확히 맞춤)
    let mut s = "가".repeat((32 << 20) / 3);
    while s.len() < (32 << 20) {
        s.push('a');
    }
    assert_eq!(s.len(), 33_554_432);
    commit(
        &l,
        &f,
        &mut idx,
        &[(
            "INSERT INTO t (id, body) VALUES (1, $1)",
            vec![Value::Text(s.clone())],
        )],
    )
    .await;
    let r = query(&f, "SELECT octet_length(body), body FROM t WHERE id = 1").await;
    assert_eq!(r.rows[0][0], Value::Int(33_554_432));
    assert_eq!(r.rows[0][1], Value::Text(s.clone()));

    // 1 byte 초과: JDB-3101, 아무것도 남지 않음
    let mut big = s.clone();
    big.push('x');
    let txn = new_id();
    exec(
        &l,
        txn,
        "INSERT INTO t (id, body) VALUES (2, $1)",
        vec![Value::Text(big)],
    )
    .await
    .unwrap();
    let e = prepare(&l, txn).await.unwrap_err();
    assert_eq!(e.code, "JDB-3101");
    assert_eq!(
        query(&l, "SELECT count(*) FROM t").await.rows[0][0],
        Value::Int(1)
    );

    // 한 Row에 TEXT 두 개 32MiB → SQLite Row 64MiB 상한 JDB-3103
    let txn = new_id();
    let e = exec(
        &l,
        txn,
        "INSERT INTO t (id, body, b2) VALUES (3, $1, $1)",
        vec![Value::Text(s.clone())],
    )
    .await
    .unwrap_err();
    assert_eq!(e.code, "JDB-3103", "{e}");
    l.handle(StoreRequest::TxAbort { txn }).await.unwrap();
    // 두 Row 각각 32MiB → 트랜잭션 Mutation 64MiB 초과 JDB-3102
    let txn = new_id();
    exec(
        &l,
        txn,
        "INSERT INTO t (id, body) VALUES (3, $1)",
        vec![Value::Text(s.clone())],
    )
    .await
    .unwrap();
    exec(
        &l,
        txn,
        "INSERT INTO t (id, body) VALUES (4, $1)",
        vec![Value::Text(s.clone())],
    )
    .await
    .unwrap();
    let e = prepare(&l, txn).await.unwrap_err();
    assert_eq!(e.code, "JDB-3102", "{e}");
    assert_eq!(
        query(&l, "SELECT count(*) FROM t").await.rows[0][0],
        Value::Int(1)
    );

    // 32MiB UPDATE: patchset은 old 값을 포함하지 않으므로 허용
    let s2 = s.replace('a', "b");
    commit(
        &l,
        &f,
        &mut idx,
        &[(
            "UPDATE t SET body = $1 WHERE id = 1",
            vec![Value::Text(s2.clone())],
        )],
    )
    .await;
    assert_eq!(checksum(&l).await, checksum(&f).await);

    // Prepared 후 Raft 확정 전 Abort (Leader 강등) → 데이터 없음
    let txn = new_id();
    exec(&l, txn, "INSERT INTO t (id, body) VALUES (9, 'x')", vec![])
        .await
        .unwrap();
    prepare(&l, txn).await.unwrap().unwrap();
    l.handle(StoreRequest::AbortAll).await.unwrap();
    assert_eq!(
        query(&l, "SELECT count(*) FROM t WHERE id = 9").await.rows[0][0],
        Value::Int(0)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_recovers_applied_index_and_snapshot_install() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let mut idx = 0;
    {
        let l = Engine::open(opts(d1.path())).unwrap();
        let f = Engine::open(opts(d2.path())).unwrap();
        commit(
            &l,
            &f,
            &mut idx,
            &[(
                "CREATE TABLE k (id BIGSERIAL PRIMARY KEY, v VARCHAR(10))",
                vec![],
            )],
        )
        .await;
        for i in 0..20 {
            commit(
                &l,
                &f,
                &mut idx,
                &[(&format!("INSERT INTO k (v) VALUES ('v{i}')"), vec![])],
            )
            .await;
        }
        l.shutdown();
        f.shutdown();
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let l = Engine::open(opts(d1.path())).unwrap();
    assert_eq!(l.applied_index(), idx);
    // 중복 Apply는 효과 없음 (exactly-once effect)
    let r = l
        .handle(StoreRequest::Apply {
            index: idx,
            term: 1,
            cmd: ReplicatedCommand::Noop,
        })
        .await
        .unwrap();
    assert!(matches!(r, StoreResponse::Applied { .. }));
    // 갭은 거부
    assert!(l
        .handle(StoreRequest::Apply {
            index: idx + 5,
            term: 1,
            cmd: ReplicatedCommand::Noop
        })
        .await
        .is_err());

    // Snapshot 생성 → 새 노드에 설치
    let snap = d1.path().join("snap.sqlite");
    let info = match l
        .handle(StoreRequest::CreateSnapshot {
            path: snap.display().to_string(),
        })
        .await
        .unwrap()
    {
        StoreResponse::Snapshot(s) => s,
        _ => panic!(),
    };
    assert_eq!(info.index, idx);
    let d3 = tempfile::tempdir().unwrap();
    let n = Engine::open(opts(d3.path())).unwrap();
    n.handle(StoreRequest::InstallSnapshot {
        path: info.path.clone(),
        index: info.index,
        term: info.term,
    })
    .await
    .unwrap();
    assert_eq!(n.applied_index(), idx);
    assert_eq!(checksum(&n).await, checksum(&l).await);
    assert_eq!(
        query(&n, "SELECT count(*) FROM k").await.rows[0][0],
        Value::Int(20)
    );
}

/// RQ-01: 멱등 요청은 Writer 안에서 판정되고, 결과(RETURNING 포함)가 복제·재생된다.
#[tokio::test(flavor = "multi_thread")]
async fn idempotent_request_replays_recorded_result() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let l = Engine::open(opts(d1.path())).unwrap();
    let f = Engine::open(opts(d2.path())).unwrap();
    let mut idx = 0u64;
    commit(&l, &f, &mut idx, &[("CREATE TABLE r (id BIGSERIAL PRIMARY KEY, v TEXT)", vec![])]).await;

    let ins = "INSERT INTO r (v) VALUES ('a') RETURNING id";
    let run = |txn: u128| {
        let l = l.clone();
        async move {
            let r = exec_ext(&l, txn, ins, vec![], Some("req-1"), None).await?;
            let c = prepare_cmd(&l, txn, Some("req-1")).await?;
            Ok::<_, jamsil_common::DbError>((r, c))
        }
    };
    let (r1, c1) = run(new_id()).await.unwrap();
    let c1 = c1.expect("first execution replicates");
    assert!(matches!(c1, ReplicatedCommand::TxnWithRequest { .. }));
    idx += 1;
    apply(&l, idx, c1.clone()).await;
    apply(&f, idx, c1).await;
    assert_eq!(r1.rows.len(), 1);

    // 재시도 (Leader/Follower 어느 쪽이 새 Leader가 되어도 같은 결과)
    for e in [&l, &f] {
        let txn = new_id();
        let r = exec_ext(e, txn, ins, vec![], Some("req-1"), None).await.unwrap();
        assert_eq!(r.rows, r1.rows, "replayed RETURNING must match");
        assert!(prepare_cmd(e, txn, Some("req-1")).await.unwrap().is_none());
    }
    // 같은 키 + 다른 문장 → 22023
    let e = exec_ext(&l, new_id(), "INSERT INTO r (v) VALUES ('b')", vec![], Some("req-1"), None)
        .await
        .unwrap_err();
    assert_eq!(e.sqlstate, "22023");
    let n = query(&l, "SELECT count(*) FROM r").await;
    assert_eq!(n.rows[0][0], Value::Int(1));
    assert_eq!(checksum(&l).await, checksum(&f).await);
}

/// RQ-01: 동시 요청 64개가 같은 키를 써도 효과는 한 번.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn idempotent_request_concurrent() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let l = Engine::open(opts(d1.path())).unwrap();
    let f = Engine::open(opts(d2.path())).unwrap();
    let mut idx = 0u64;
    commit(&l, &f, &mut idx, &[("CREATE TABLE c (id INTEGER PRIMARY KEY, n INTEGER)", vec![])]).await;
    commit(&l, &f, &mut idx, &[("INSERT INTO c VALUES (1, 0)", vec![])]).await;
    let idx = Arc::new(tokio::sync::Mutex::new(idx));
    let mut hs = vec![];
    for _ in 0..64 {
        let l = l.clone();
        let idx = idx.clone();
        hs.push(tokio::spawn(async move {
            let txn = new_id();
            let r = exec_ext(&l, txn, "UPDATE c SET n = n + 1 WHERE id = 1", vec![], Some("same"), None).await;
            if r.is_err() {
                let _ = l.handle(StoreRequest::TxAbort { txn }).await;
                return r.map(|_| ());
            }
            if let Some(cmd) = prepare_cmd(&l, txn, Some("same")).await? {
                // Raft 확정 + Apply를 직렬로 흉내낸다 (Writer 소유 중이므로 다른 요청은 대기)
                let mut g = idx.lock().await;
                *g += 1;
                apply(&l, *g, cmd).await;
            }
            Ok(())
        }));
    }
    for h in hs {
        h.await.unwrap().unwrap();
    }
    let n = query(&l, "SELECT n FROM c WHERE id = 1").await;
    assert_eq!(n.rows[0][0], Value::Int(1), "effect must happen exactly once");
}

/// RQ-07: 오류 후 ROLLBACK TO SAVEPOINT로 복구하고 COMMIT, 복제본 수렴.
#[tokio::test(flavor = "multi_thread")]
async fn rollback_to_savepoint_after_error() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let l = Engine::open(opts(d1.path())).unwrap();
    let f = Engine::open(opts(d2.path())).unwrap();
    let mut idx = 0u64;
    commit(&l, &f, &mut idx, &[("CREATE TABLE sp (id INTEGER PRIMARY KEY, v INTEGER)", vec![])]).await;
    let txn = new_id();
    exec(&l, txn, "INSERT INTO sp VALUES (1, 1)", vec![]).await.unwrap();
    exec(&l, txn, "SAVEPOINT keep", vec![]).await.unwrap();
    exec(&l, txn, "INSERT INTO sp VALUES (2, 2)", vec![]).await.unwrap();
    assert!(exec(&l, txn, "INSERT INTO sp VALUES (1, 9)", vec![]).await.is_err());
    assert_eq!(exec(&l, txn, "SELECT 1", vec![]).await.unwrap_err().sqlstate, "25P02");
    exec(&l, txn, "ROLLBACK TO SAVEPOINT keep", vec![]).await.unwrap();
    exec(&l, txn, "INSERT INTO sp VALUES (3, 3)", vec![]).await.unwrap();
    let p = prepare(&l, txn).await.unwrap().unwrap();
    idx += 1;
    let cmd = ReplicatedCommand::Txn(p);
    apply(&l, idx, cmd.clone()).await;
    apply(&f, idx, cmd).await;
    for e in [&l, &f] {
        let r = query(e, "SELECT id FROM sp ORDER BY id").await;
        assert_eq!(r.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
    }
    assert_eq!(checksum(&l).await, checksum(&f).await);
}

/// RQ-07: savepoint 이후의 DDL은 ROLLBACK TO로 복제 대상에서도 빠진다.
#[tokio::test(flavor = "multi_thread")]
async fn rollback_to_savepoint_discards_ddl() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let l = Engine::open(opts(d1.path())).unwrap();
    let f = Engine::open(opts(d2.path())).unwrap();
    let mut idx = 0u64;
    let txn = new_id();
    exec(&l, txn, "CREATE TABLE keep_t (id INTEGER PRIMARY KEY)", vec![]).await.unwrap();
    exec(&l, txn, "SAVEPOINT s1", vec![]).await.unwrap();
    exec(&l, txn, "CREATE TABLE drop_t (id INTEGER PRIMARY KEY)", vec![]).await.unwrap();
    exec(&l, txn, "ROLLBACK TO s1", vec![]).await.unwrap();
    let p = prepare(&l, txn).await.unwrap().unwrap();
    assert_eq!(p.ddl.len(), 1, "{:?}", p.ddl);
    idx += 1;
    let cmd = ReplicatedCommand::Txn(p);
    apply(&l, idx, cmd.clone()).await;
    apply(&f, idx, cmd).await;
    assert_eq!(checksum(&l).await, checksum(&f).await);
}

/// RQ-06: 고정 Snapshot은 다른 COMMIT 이후에도 같은 값을 보고, 첫 쓰기는 40001.
#[tokio::test(flavor = "multi_thread")]
async fn pinned_snapshot_repeatable_read() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let l = Engine::open(opts(d1.path())).unwrap();
    let f = Engine::open(opts(d2.path())).unwrap();
    let mut idx = 0u64;
    commit(&l, &f, &mut idx, &[("CREATE TABLE rr (id INTEGER PRIMARY KEY, v INTEGER)", vec![])]).await;
    commit(&l, &f, &mut idx, &[("INSERT INTO rr VALUES (1, 0)", vec![])]).await;
    let snap = new_id();
    let at = match f
        .handle(StoreRequest::SnapshotOpen { snapshot: snap, min_applied: idx, wait_timeout_ms: 1000 })
        .await
        .unwrap()
    {
        StoreResponse::Applied { index } => index,
        _ => panic!(),
    };
    assert_eq!(at, idx);
    let q = |e: Arc<Engine>, sql: &'static str| async move {
        match e
            .handle(StoreRequest::Query {
                session: 7,
                stmt: st(sql, vec![]),
                min_applied: 0,
                wait_timeout_ms: 1000,
                search_consistency: SearchConsistency::Eventual,
                virtual_tables: vec![],
                snapshot: Some(snap),
            })
            .await
            .unwrap()
        {
            StoreResponse::Result(r) => r,
            _ => panic!(),
        }
    };
    assert_eq!(q(f.clone(), "SELECT v FROM rr").await.rows[0][0], Value::Int(0));
    commit(&l, &f, &mut idx, &[("UPDATE rr SET v = 1 WHERE id = 1", vec![]), ("INSERT INTO rr VALUES (2, 2)", vec![])]).await;
    // non-repeatable read / phantom 없음
    assert_eq!(q(f.clone(), "SELECT v FROM rr WHERE id = 1").await.rows[0][0], Value::Int(0));
    assert_eq!(q(f.clone(), "SELECT count(*) FROM rr").await.rows[0][0], Value::Int(1));
    // 읽기→쓰기 승격: Snapshot 이후 COMMIT이 있었으므로 직렬화 실패
    let txn = new_id();
    let e = exec_ext(&l, txn, "UPDATE rr SET v = v + 10 WHERE id = 1", vec![], None, Some(at))
        .await
        .unwrap_err();
    assert_eq!(e.sqlstate, "40001");
    // 변경이 없었다면 승격 성공
    let txn = new_id();
    exec_ext(&l, txn, "UPDATE rr SET v = v + 10 WHERE id = 1", vec![], None, Some(idx)).await.unwrap();
    let _ = l.handle(StoreRequest::TxAbort { txn }).await;
    f.handle(StoreRequest::SnapshotClose { snapshot: snap }).await.unwrap();
}

/// R2-03: 고정 Snapshot에서 TEXT_MATCH/TEXT_SCORE는 현재 검색 인덱스와 과거 행을 섞으므로 JDB-4105로 거부하고,
/// 거부 뒤에도 같은 Snapshot의 일반 조회는 그대로 고정되어 있다.
#[tokio::test(flavor = "multi_thread")]
async fn pinned_snapshot_rejects_text_match() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let l = Engine::open(opts(d1.path())).unwrap();
    let f = Engine::open(opts(d2.path())).unwrap();
    let mut idx = 0u64;
    commit(&l, &f, &mut idx, &[("CREATE TABLE docs (id BIGINT PRIMARY KEY, body TEXT)", vec![])]).await;
    commit(&l, &f, &mut idx, &[("CREATE SEARCH INDEX docs_ix ON docs(body) WITH (analyzer = 'standard')", vec![])]).await;
    commit(&l, &f, &mut idx, &[("INSERT INTO docs VALUES (1, 'alpha'), (2, 'beta')", vec![])]).await;
    let snap = new_id();
    f.handle(StoreRequest::SnapshotOpen { snapshot: snap, min_applied: idx, wait_timeout_ms: 1000 })
        .await
        .unwrap();
    let q = |sql: &'static str| {
        let f = f.clone();
        async move {
            f.handle(StoreRequest::Query {
                session: 9,
                stmt: st(sql, vec![]),
                min_applied: 0,
                wait_timeout_ms: 1000,
                search_consistency: SearchConsistency::Committed,
                virtual_tables: vec![],
                snapshot: Some(snap),
            })
            .await
        }
    };
    commit(&l, &f, &mut idx, &[("UPDATE docs SET body = CASE WHEN id = 1 THEN 'beta' ELSE 'alpha' END", vec![])]).await;
    for sql in [
        "SELECT id FROM docs WHERE TEXT_MATCH(body, 'alpha')",
        "SELECT id, TEXT_SCORE(body) FROM docs WHERE TEXT_MATCH(body, 'alpha') ORDER BY 2 DESC",
        "SELECT count(*) FROM docs WHERE id IN (SELECT id FROM docs WHERE TEXT_MATCH(body, 'alpha'))",
    ] {
        let e = q(sql).await.unwrap_err();
        assert_eq!((e.code.as_str(), e.sqlstate.as_str()), ("JDB-4105", "0A000"), "{sql}");
    }
    match q("SELECT body FROM docs WHERE id = 1").await.unwrap() {
        StoreResponse::Result(r) => assert_eq!(r.rows[0][0], Value::Text("alpha".into())),
        _ => panic!(),
    }
    f.handle(StoreRequest::SnapshotClose { snapshot: snap }).await.unwrap();
}

/// RQ-01: 명시적 트랜잭션 COMMIT 키 — 같은 내용 재시도는 한 번만, 다른 내용에 재사용하면 22023.
#[tokio::test(flavor = "multi_thread")]
async fn explicit_transaction_request_id() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let l = Engine::open(opts(d1.path())).unwrap();
    let f = Engine::open(opts(d2.path())).unwrap();
    let mut idx = 0u64;
    commit(&l, &f, &mut idx, &[("CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)", vec![])]).await;
    commit(&l, &f, &mut idx, &[("INSERT INTO t VALUES (1, 0)", vec![])]).await;
    let run = |sql: &'static str| {
        let l = l.clone();
        async move {
            let txn = new_id();
            exec(&l, txn, sql, vec![]).await.unwrap();
            prepare_cmd(&l, txn, Some("tx-key")).await
        }
    };
    let c = run("UPDATE t SET n = n + 1 WHERE id = 1").await.unwrap().unwrap();
    idx += 1;
    apply(&l, idx, c).await;
    // 같은 트랜잭션 재시도: 효과 없음
    assert!(run("UPDATE t SET n = n + 1 WHERE id = 1").await.unwrap().is_none());
    // 다른 트랜잭션에 같은 키: 거부
    assert_eq!(run("UPDATE t SET n = n + 5 WHERE id = 1").await.unwrap_err().sqlstate, "22023");
    assert_eq!(query(&l, "SELECT n FROM t").await.rows[0][0], Value::Int(1));
}
