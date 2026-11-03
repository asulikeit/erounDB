//! WAL Snapshot Reader Pool. 각 스레드가 자신의 Connection을 소유한다.

use std::path::PathBuf;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::Connection;
use tokio::sync::oneshot;

use jamsil_common::{DbError, DbResult};

use crate::engine::{open_connection, Shared};

type Job = Box<dyn FnOnce(&Connection) + Send>;

pub struct ReaderPool {
    tx: Sender<Job>,
}

impl ReaderPool {
    pub fn start(shared: Arc<Shared>, db_path: PathBuf, n: usize) -> ReaderPool {
        let (tx, rx) = channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        for i in 0..n.max(1) {
            let rx = rx.clone();
            let shared = shared.clone();
            let path = db_path.clone();
            std::thread::Builder::new()
                .name(format!("jamsil-reader-{i}"))
                .spawn(move || reader_loop(shared, path, rx))
                .expect("spawn reader");
        }
        ReaderPool { tx }
    }

    /// Reader 스레드에서 클로저를 실행한다.
    pub async fn run<T, F>(&self, f: F) -> DbResult<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> DbResult<T> + Send + 'static,
    {
        let (otx, orx) = oneshot::channel();
        let job: Job = Box::new(move |c| {
            let _ = otx.send(f(c));
        });
        self.tx
            .send(job)
            .map_err(|_| DbError::unavailable("reader pool stopped"))?;
        orx.await
            .map_err(|_| DbError::internal("reader job dropped"))?
    }
}

fn reader_loop(shared: Arc<Shared>, path: PathBuf, rx: Arc<Mutex<Receiver<Job>>>) {
    let mut conn: Option<(u64, Connection)> = None;
    loop {
        let job = {
            let g = rx.lock();
            match g.recv() {
                Ok(j) => j,
                Err(_) => return,
            }
        };
        // Snapshot 설치 중에는 파일 교체가 끝날 때까지 대기한다.
        let gate = shared.gate.read();
        let generation = *gate;
        let need_open = match &conn {
            Some((g, _)) => *g != generation,
            None => true,
        };
        if need_open {
            conn = None;
            match open_connection(&shared, &path, false) {
                Ok(c) => conn = Some((generation, c)),
                Err(e) => {
                    tracing::error!("reader cannot open database: {e}");
                    drop(gate);
                    // 실패를 알리기 위해 빈 메모리 DB로 실행 (작업 쪽에서 오류가 난다)
                    if let Ok(mem) = Connection::open_in_memory() {
                        job(&mem);
                    }
                    continue;
                }
            }
        }
        let c = &conn.as_ref().unwrap().1;
        job(c);
        // 혹시 남은 트랜잭션 정리
        if !c.is_autocommit() {
            let _ = c.execute_batch("ROLLBACK");
        }
        drop(gate);
    }
}
