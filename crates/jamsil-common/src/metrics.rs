//! 프로세스 내 지표 레지스트리 (설계서 12.4 / RQ-12).
//!
//! * 히스토그램: 로그 스케일 버킷(2^(1/4) 간격, 상대 오차 ≤ 19%). 분위수(P50/P95/P99)는
//!   **최근 1~2분**(60초 창 두 개) 관측값으로 계산하고, `_count`/`_sum`은 시작 이후 누적이다.
//! * 게이지/카운터는 각 프로세스가 상태 수집 시 직접 샘플을 만든다.
//!
//! 샘플은 Status IPC 응답에 실려 `jamsil_catalog.metrics`(node, process, name, labels, value, unit)로 노출된다.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MetricSample {
    pub name: String,
    /// `k=v,k=v` (없으면 빈 문자열)
    pub labels: String,
    pub value: f64,
    /// seconds | bytes | count | entries | bool | index
    pub unit: String,
}

impl MetricSample {
    pub fn new(name: &str, labels: &str, value: f64, unit: &str) -> MetricSample {
        MetricSample {
            name: name.into(),
            labels: labels.into(),
            value,
            unit: unit.into(),
        }
    }
}

const BUCKETS: usize = 192;
const WINDOW: Duration = Duration::from_secs(60);

struct Windows {
    started: Instant,
    cur: [u64; BUCKETS],
    prev: [u64; BUCKETS],
}

pub struct Histogram {
    name: &'static str,
    labels: &'static str,
    /// 값 단위 (seconds면 내부는 마이크로초, bytes면 바이트)
    unit: &'static str,
    w: Mutex<Windows>,
    count: AtomicU64,
    sum: AtomicU64,
}

fn bucket_of(v: u64) -> usize {
    if v == 0 {
        return 0;
    }
    // floor(4 * log2(v)) + 1
    let l = (v as f64).log2() * 4.0;
    ((l.floor() as usize) + 1).min(BUCKETS - 1)
}

fn bucket_upper(b: usize) -> f64 {
    if b == 0 {
        0.0
    } else {
        2f64.powf(b as f64 / 4.0)
    }
}

impl Histogram {
    fn new(name: &'static str, labels: &'static str, unit: &'static str) -> Histogram {
        Histogram {
            name,
            labels,
            unit,
            w: Mutex::new(Windows {
                started: Instant::now(),
                cur: [0; BUCKETS],
                prev: [0; BUCKETS],
            }),
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }

    fn rotate(w: &mut Windows) {
        let el = w.started.elapsed();
        if el >= WINDOW * 2 {
            w.prev = [0; BUCKETS];
            w.cur = [0; BUCKETS];
            w.started = Instant::now();
        } else if el >= WINDOW {
            w.prev = w.cur;
            w.cur = [0; BUCKETS];
            w.started += WINDOW;
        }
    }

    /// 원시 값 기록 (seconds 단위 히스토그램은 마이크로초).
    pub fn record_raw(&self, v: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(v, Ordering::Relaxed);
        let mut w = self.w.lock().unwrap_or_else(|e| e.into_inner());
        Self::rotate(&mut w);
        w.cur[bucket_of(v)] += 1;
    }

    pub fn record_duration(&self, d: Duration) {
        self.record_raw(d.as_micros().min(u64::MAX as u128) as u64);
    }

    /// 최근 창의 분위수 (원시 단위). 관측이 없으면 None.
    pub fn quantile_raw(&self, q: f64) -> Option<f64> {
        let mut w = self.w.lock().unwrap_or_else(|e| e.into_inner());
        Self::rotate(&mut w);
        let total: u64 = w.cur.iter().zip(w.prev.iter()).map(|(a, b)| a + b).sum();
        if total == 0 {
            return None;
        }
        let rank = ((total as f64) * q).ceil().max(1.0) as u64;
        let mut acc = 0;
        for b in 0..BUCKETS {
            acc += w.cur[b] + w.prev[b];
            if acc >= rank {
                return Some(bucket_upper(b));
            }
        }
        Some(bucket_upper(BUCKETS - 1))
    }

    fn scale(&self) -> f64 {
        if self.unit == "seconds" {
            1e-6
        } else {
            1.0
        }
    }

    pub fn samples(&self) -> Vec<MetricSample> {
        let sc = self.scale();
        let lab = |q: &str| {
            if self.labels.is_empty() {
                format!("quantile={q}")
            } else {
                format!("{},quantile={q}", self.labels)
            }
        };
        let mut out = vec![];
        for (q, s) in [(0.5, "0.5"), (0.95, "0.95"), (0.99, "0.99")] {
            // 최근 창에 관측이 없으면 값은 NaN (jamsil_catalog.metrics에서 NULL)
            let v = self.quantile_raw(q).map(|v| v * sc).unwrap_or(f64::NAN);
            out.push(MetricSample::new(self.name, &lab(s), v, self.unit));
        }
        out.push(MetricSample::new(
            &format!("{}_count", self.name),
            self.labels,
            self.count.load(Ordering::Relaxed) as f64,
            "count",
        ));
        out.push(MetricSample::new(
            &format!("{}_sum", self.name),
            self.labels,
            self.sum.load(Ordering::Relaxed) as f64 * sc,
            self.unit,
        ));
        out
    }
}

static REGISTRY: OnceLock<Mutex<Vec<&'static Histogram>>> = OnceLock::new();

/// 이름+레이블별 히스토그램 (처음 호출 시 등록).
pub fn histogram(name: &'static str, labels: &'static str, unit: &'static str) -> &'static Histogram {
    let reg = REGISTRY.get_or_init(|| Mutex::new(vec![]));
    let mut g = reg.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(h) = g.iter().find(|h| h.name == name && h.labels == labels) {
        return h;
    }
    let h: &'static Histogram = Box::leak(Box::new(Histogram::new(name, labels, unit)));
    g.push(h);
    h
}

/// 등록된 모든 히스토그램 + 이 프로세스의 메모리 예산 샘플.
pub fn process_samples() -> Vec<MetricSample> {
    let mut out = vec![];
    if let Some(reg) = REGISTRY.get() {
        let g = reg.lock().unwrap_or_else(|e| e.into_inner());
        for h in g.iter() {
            out.extend(h.samples());
        }
    }
    out.push(MetricSample::new(
        "jamsil_build_info",
        &format!("version={},release={},build={}", env!("CARGO_PKG_VERSION"), crate::RELEASE, crate::BUILD_ID),
        1.0,
        "bool",
    ));
    let b = crate::budget::stats();
    out.push(MetricSample::new("jamsil_large_value_budget_bytes", "", b.total_bytes as f64, "bytes"));
    out.push(MetricSample::new("jamsil_large_text_inflight_bytes", "", b.inflight_bytes as f64, "bytes"));
    out.push(MetricSample::new("jamsil_large_text_inflight_peak_bytes", "", b.peak_bytes as f64, "bytes"));
    out.push(MetricSample::new("jamsil_ipc_backpressure_events_total", "reason=budget_wait", b.waits as f64, "count"));
    out.push(MetricSample::new("jamsil_ipc_backpressure_events_total", "reason=budget_reject", b.rejects as f64, "count"));
    out
}

/// 경로가 속한 파일시스템의 여유 바이트 (비특권 사용자 기준).
pub fn disk_free_bytes(path: &std::path::Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: c는 NUL 종료 경로, st는 초기화된 출력 버퍼.
    let r = unsafe { libc::statvfs(c.as_ptr(), &mut st) };
    if r != 0 {
        return None;
    }
    Some(st.f_bavail as u64 * st.f_frsize as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_track_distribution() {
        let h = histogram("t_duration_seconds", "kind=x", "seconds");
        for i in 1..=100u64 {
            h.record_duration(Duration::from_millis(i));
        }
        let p50 = h.quantile_raw(0.5).unwrap() / 1e6;
        let p99 = h.quantile_raw(0.99).unwrap() / 1e6;
        assert!((0.045..0.065).contains(&p50), "{p50}");
        assert!((0.095..0.125).contains(&p99), "{p99}");
        let s = h.samples();
        assert!(s.iter().any(|m| m.labels == "kind=x,quantile=0.95"));
        assert_eq!(s.iter().find(|m| m.name == "t_duration_seconds_count").unwrap().value, 100.0);
        assert!(disk_free_bytes(std::path::Path::new("/")).unwrap() > 0);
    }
}
