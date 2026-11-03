//! 시험 전용 장애 주입 지점 (RQ-14).
//!
//! 환경변수 `JAMSIL_FAULT_INJECTION=1`일 때만 동작하며, 해당 프로세스 데이터 디렉터리에
//! 표식 파일을 만들면 정확한 지점에서 장애를 일으킨다. 운영 환경에서는 환경변수를 설정하지 않는다.
//!
//! | 표식 (위치) | 지점 | 시험 |
//! |---|---|---|
//! | `raft/crash_before_raft_fsync` | Raft 로그 append 후 fsync 전 프로세스 abort | T-06 |
//! | `raft/fail_raft_fsync` | Raft fsync가 EIO로 실패 (표식이 있는 동안) | T-15 |
//! | `rdb/crash_before_apply_commit` | Raft 확정 후 SQLite COMMIT 직전 abort | T-07 |
//! | `rdb/crash_after_apply_commit` | SQLite COMMIT 직후, Apply 완료 보고 전 abort | T-08 |
//! | `rdb/sqlite_full` | Apply 시 SQLite 파일 증가 불가 (SQLITE_FULL, 표식이 있는 동안) | T-15 |
//! | `raft/crash_after_commit_ack` | Leader가 쓰기 성공 응답을 보낸 직후 jamsil-sync abort | T-09 |
//! | `raft/crash_mid_large_append` | 8MiB 이상 AppendEntries를 프레임 4개만 보낸 뒤 abort | T-11 |

use std::path::Path;
use std::sync::OnceLock;

pub fn enabled() -> bool {
    static E: OnceLock<bool> = OnceLock::new();
    *E.get_or_init(|| std::env::var("JAMSIL_FAULT_INJECTION").ok().as_deref() == Some("1"))
}

/// 1회성 표식: 있으면 지우고 true.
pub fn take(dir: &Path, name: &str) -> bool {
    if !enabled() {
        return false;
    }
    let f = dir.join(name);
    if f.exists() {
        let _ = std::fs::remove_file(&f);
        eprintln!("fault injection: {name}");
        return true;
    }
    false
}

/// 지속 표식: 있는 동안 true (지우지 않음).
pub fn active(dir: &Path, name: &str) -> bool {
    enabled() && dir.join(name).exists()
}

/// 표식을 남긴 뒤 즉시 프로세스를 죽인다 (SIGABRT, 종료 처리 없음).
pub fn crash(name: &str) -> ! {
    eprintln!("fault injection: crashing at {name}");
    std::process::abort()
}
