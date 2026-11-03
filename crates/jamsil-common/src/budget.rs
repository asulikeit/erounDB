//! 대형 값 메모리 예산 (설계서 8.3, 10.6, 14.3 Bounded Memory / RQ-11).
//!
//! 프로세스마다 하나의 예산(`storage.limits.large_value_budget_mib`)을 두고, 1MiB를 넘는 메시지를
//! **본문을 읽거나 복사하기 전에** 총 바이트만큼 예약한다. 예약은 처리가 끝날 때(Guard drop) 반납된다.
//!
//! * PG 수신(세션별 태스크): 예산이 빌 때까지 `wait`만큼 기다린 뒤 실패 (backpressure).
//! * 프로세스 간 IPC 재조립: 기다리지 않고 즉시 거부 (한 연결을 공유하는 다른 요청을 막지 않기 위해).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use tokio::sync::{Semaphore, SemaphorePermit};

use crate::limits::MIB;
use crate::DbError;

/// 이 크기 이상인 메시지만 예산을 쓴다 (작은 메시지는 개수 상한으로 충분).
pub const LARGE_MESSAGE_THRESHOLD: usize = MIB;

impl std::fmt::Debug for ByteBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ByteBudget").field("stats", &self.stats()).finish()
    }
}

pub struct ByteBudget {
    sem: Semaphore,
    total_kib: usize,
    inflight: AtomicU64,
    peak: AtomicU64,
    waits: AtomicU64,
    rejects: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BudgetStats {
    pub total_bytes: u64,
    pub inflight_bytes: u64,
    pub peak_bytes: u64,
    /// 예산 부족으로 대기한 횟수 (backpressure 이벤트)
    pub waits: u64,
    /// 예산 부족으로 거부한 횟수
    pub rejects: u64,
}

#[derive(Debug)]
pub struct BudgetGuard {
    permit: Option<SemaphorePermit<'static>>,
    bytes: u64,
    budget: Option<&'static ByteBudget>,
}

impl Drop for BudgetGuard {
    fn drop(&mut self) {
        if let (Some(_), Some(b)) = (self.permit.take(), self.budget) {
            b.inflight.fetch_sub(self.bytes, Ordering::Relaxed);
        }
    }
}

impl BudgetGuard {
    pub fn none() -> BudgetGuard {
        BudgetGuard {
            permit: None,
            bytes: 0,
            budget: None,
        }
    }
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

static GLOBAL: OnceLock<ByteBudget> = OnceLock::new();

/// 프로세스 시작 시 한 번 설정 (이후 호출은 무시).
pub fn init(total_bytes: usize) {
    let _ = GLOBAL.set(ByteBudget::new(total_bytes));
}

pub fn global() -> &'static ByteBudget {
    GLOBAL.get_or_init(|| ByteBudget::new(512 * MIB))
}

pub fn stats() -> BudgetStats {
    global().stats()
}

fn kib(bytes: usize) -> usize {
    bytes.div_ceil(1024)
}

impl ByteBudget {
    /// 프로세스 전역이 아닌 별도 예산 (시험용).
    pub fn leaked(total_bytes: usize) -> &'static ByteBudget {
        Box::leak(Box::new(ByteBudget::new(total_bytes)))
    }

    fn new(total_bytes: usize) -> ByteBudget {
        let total_kib = kib(total_bytes).clamp(1, Semaphore::MAX_PERMITS);
        ByteBudget {
            sem: Semaphore::new(total_kib),
            total_kib,
            inflight: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            waits: AtomicU64::new(0),
            rejects: AtomicU64::new(0),
        }
    }

    pub fn stats(&self) -> BudgetStats {
        BudgetStats {
            total_bytes: self.total_kib as u64 * 1024,
            inflight_bytes: self.inflight.load(Ordering::Relaxed),
            peak_bytes: self.peak.load(Ordering::Relaxed),
            waits: self.waits.load(Ordering::Relaxed),
            rejects: self.rejects.load(Ordering::Relaxed),
        }
    }

    fn granted(&'static self, p: SemaphorePermit<'static>, bytes: usize) -> BudgetGuard {
        let now = self.inflight.fetch_add(bytes as u64, Ordering::Relaxed) + bytes as u64;
        self.peak.fetch_max(now, Ordering::Relaxed);
        BudgetGuard {
            permit: Some(p),
            bytes: bytes as u64,
            budget: Some(self),
        }
    }

    fn too_large(&self, bytes: usize) -> DbError {
        self.rejects.fetch_add(1, Ordering::Relaxed);
        DbError::new(
            "",
            "54000",
            format!(
                "message of {bytes} bytes exceeds the large value memory budget ({} bytes)",
                self.total_kib * 1024
            ),
        )
        .with_hint("raise storage.limits.large_value_budget_mib")
    }

    fn exhausted(&self, bytes: usize) -> DbError {
        self.rejects.fetch_add(1, Ordering::Relaxed);
        DbError::new(
            "",
            "53200",
            format!(
                "large value memory budget exhausted: {bytes} bytes requested, {} of {} bytes in use",
                self.inflight.load(Ordering::Relaxed),
                self.total_kib * 1024
            ),
        )
        .with_hint("retry later or reduce concurrent large requests")
    }

    /// 기다리지 않고 예약. 부족하면 53200.
    pub fn try_reserve(&'static self, bytes: usize) -> Result<BudgetGuard, DbError> {
        if bytes < LARGE_MESSAGE_THRESHOLD {
            return Ok(BudgetGuard::none());
        }
        let k = kib(bytes);
        if k > self.total_kib {
            return Err(self.too_large(bytes));
        }
        match self.sem.try_acquire_many(k as u32) {
            Ok(p) => Ok(self.granted(p, bytes)),
            Err(_) => Err(self.exhausted(bytes)),
        }
    }

    /// 예산이 빌 때까지 최대 `wait` 대기 후 예약.
    pub async fn reserve(&'static self, bytes: usize, wait: Duration) -> Result<BudgetGuard, DbError> {
        if bytes < LARGE_MESSAGE_THRESHOLD {
            return Ok(BudgetGuard::none());
        }
        let k = kib(bytes);
        if k > self.total_kib {
            return Err(self.too_large(bytes));
        }
        if let Ok(p) = self.sem.try_acquire_many(k as u32) {
            return Ok(self.granted(p, bytes));
        }
        self.waits.fetch_add(1, Ordering::Relaxed);
        match tokio::time::timeout(wait, self.sem.acquire_many(k as u32)).await {
            Ok(Ok(p)) => Ok(self.granted(p, bytes)),
            _ => Err(self.exhausted(bytes)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reserve_release_and_reject() {
        init(4 * MIB);
        let b = global();
        assert_eq!(b.try_reserve(1000).unwrap().bytes(), 0, "small messages are free");
        let g1 = b.try_reserve(3 * MIB).unwrap();
        assert_eq!(b.stats().inflight_bytes, 3 * MIB as u64);
        assert_eq!(b.try_reserve(2 * MIB).unwrap_err().sqlstate, "53200");
        assert_eq!(b.try_reserve(5 * MIB).unwrap_err().sqlstate, "54000");
        // 대기 후 반납되면 성공
        let h = tokio::spawn(async move { global().reserve(2 * MIB, Duration::from_secs(5)).await.map(|g| g.bytes()) });
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(g1);
        assert_eq!(h.await.unwrap().unwrap(), 2 * MIB as u64);
        assert_eq!(b.stats().inflight_bytes, 0);
        assert!(b.stats().waits >= 1);
        assert!(b.stats().peak_bytes >= 3 * MIB as u64);
        assert!(b.reserve(2 * MIB, Duration::from_millis(10)).await.is_ok());
    }
}
