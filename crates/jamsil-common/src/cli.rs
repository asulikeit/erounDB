//! 실행파일 공통 인자: `--config <path>`.

use std::path::PathBuf;

use crate::config::Config;

/// 종료 코드: 설정/포맷/Cluster ID 오류처럼 재시작해도 해결되지 않는 영구 실패.
pub const EXIT_PERMANENT: i32 = 78;
/// 내구성 보장 불가 (fsync 실패 등) — Supervisor가 재시작 후 복구.
pub const EXIT_DURABILITY: i32 = 75;

pub fn config_path_from_args() -> Option<PathBuf> {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--config" || args[i] == "-c" {
            return args.get(i + 1).map(PathBuf::from);
        }
        if let Some(p) = args[i].strip_prefix("--config=") {
            return Some(PathBuf::from(p));
        }
        i += 1;
    }
    std::env::var("JAMSIL_CONFIG").ok().map(PathBuf::from)
}

/// 설정 로드. 실패하면 오류 출력 후 영구 실패 코드로 종료.
pub fn load_config_or_exit(process: &str) -> Config {
    let Some(p) = config_path_from_args() else {
        eprintln!("{process}: missing --config <path>");
        std::process::exit(EXIT_PERMANENT);
    };
    match Config::load(&p) {
        Ok(c) => {
            crate::budget::init(c.storage.limits.large_value_budget_mib * crate::limits::MIB);
            c
        }
        Err(e) => {
            eprintln!("{process}: {e}");
            std::process::exit(EXIT_PERMANENT);
        }
    }
}
