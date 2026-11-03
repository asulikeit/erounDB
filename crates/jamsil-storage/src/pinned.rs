//! 트랜잭션 수명 Snapshot (REPEATABLE READ / SERIALIZABLE, RQ-06).
//!
//! 트랜잭션마다 전용 스레드가 Connection 하나를 소유하고 `BEGIN` 후 첫 읽기로 WAL Snapshot을
//! 고정한다. 같은 트랜잭션의 읽기 문장은 모두 이 Snapshot에서 실행되며, COMMIT/ROLLBACK 또는
//! 유휴 시간 초과 시 해제된다.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc::{channel, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rusqlite::Connection;
use tokio::sync::oneshot;

use jamsil_common::{DbError, DbResult, Id128};

use crate::catalog::read_state;
use crate::convert::map_err;
use crate::engine::{open_connection, Shared};

type Job = Box<dyn FnOnce(DbResult<&Connection>) + Send>;

/// 동시에 고정할 수 있는 Snapshot 수 (WAL 체크포인트 지연·파일 핸들 보호).
const MAX_PINNED: usize = 256;

pub struct PinnedSnapshots {
    shared: Arc<Shared>,
    path: PathBuf,
    idle: Duration,
    map: Arc<Mutex<HashMap<Id128, Sender<Job>>>>,
}

fn expired() -> DbError {
    DbError::new(
        "",
        "40001",
        "the transaction snapshot is no longer available (idle timeout or store recovery)",
    )
    .with_hint("retry the transaction")
}

impl PinnedSnapshots {
    pub fn new(shared: Arc<Shared>, path: PathBuf, idle: Duration) -> PinnedSnapshots {
        PinnedSnapshots {
            shared,
            path,
            idle,
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn len(&self) -> usize {
        self.map.lock().len()
    }

    /// Snapshot을 고정하고 그 시점의 applied_index를 돌려준다. 이미 있으면 그대로 재사용.
    pub async fn open(&self, id: Id128) -> DbResult<u64> {
        if self.map.lock().contains_key(&id) {
            return self.run(id, |c| Ok(read_state(c)?.applied_index)).await;
        }
        if self.map.lock().len() >= MAX_PINNED {
            return Err(DbError::new(
                "",
                "53000",
                format!("too many open transaction snapshots (limit {MAX_PINNED})"),
            )
            .with_hint("finish idle REPEATABLE READ/SERIALIZABLE transactions"));
        }
        let (tx, rx) = channel::<Job>();
        let (ready_tx, ready_rx) = oneshot::channel::<DbResult<u64>>();
        let shared = self.shared.clone();
        let path = self.path.clone();
        let idle = self.idle;
        let map = self.map.clone();
        self.map.lock().insert(id, tx);
        let spawned = std::thread::Builder::new()
            .name("jamsil-snapshot".into())
            .spawn(move || {
                let generation = *shared.gate.read();
                let conn = match open_connection(&shared, &path, false).and_then(|c| {
                    c.execute_batch("BEGIN").map_err(map_err)?;
                    // 첫 읽기 시점에 WAL Snapshot이 고정된다.
                    let idx = read_state(&c)?.applied_index;
                    Ok((c, idx))
                }) {
                    Ok((c, idx)) => {
                        let _ = ready_tx.send(Ok(idx));
                        c
                    }
                    Err(e) => {
                        map.lock().remove(&id);
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };
                loop {
                    match rx.recv_timeout(idle) {
                        Ok(job) => {
                            let gate = shared.gate.read();
                            if *gate != generation {
                                // Snapshot 설치로 파일이 교체됨: 이 Snapshot은 더 이상 유효하지 않다.
                                job(Err(expired()));
                                drop(gate);
                                break;
                            }
                            job(Ok(&conn));
                        }
                        Err(RecvTimeoutError::Timeout) => break,
                        Err(RecvTimeoutError::Disconnected) => break,
                    }
                }
                map.lock().remove(&id);
                let _ = conn.execute_batch("ROLLBACK");
            });
        if let Err(e) = spawned {
            self.map.lock().remove(&id);
            return Err(DbError::internal(e.to_string()));
        }
        ready_rx
            .await
            .map_err(|_| DbError::internal("snapshot thread exited"))?
    }

    /// 고정된 Snapshot에서 클로저를 실행한다.
    pub async fn run<T, F>(&self, id: Id128, f: F) -> DbResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> DbResult<T> + Send + 'static,
    {
        let tx = self.map.lock().get(&id).cloned().ok_or_else(expired)?;
        let (otx, orx) = oneshot::channel();
        let job: Job = Box::new(move |c| {
            let _ = otx.send(c.and_then(f));
        });
        tx.send(job).map_err(|_| expired())?;
        orx.await.map_err(|_| expired())?
    }

    pub fn close(&self, id: Id128) {
        // Sender를 버리면 스레드가 ROLLBACK 후 종료한다.
        self.map.lock().remove(&id);
    }
}
