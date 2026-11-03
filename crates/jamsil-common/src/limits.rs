//! 설계서 8.3 크기 상한.

pub const MIB: usize = 1024 * 1024;

/// TEXT 값 하나의 UTF-8 바이트 상한 (32 MiB = 33,554,432 bytes).
pub const DEFAULT_TEXT_MAX_BYTES: usize = 32 * MIB;
/// SQLite SQLITE_LIMIT_LENGTH (row/값 내부 표현 상한).
pub const DEFAULT_SQLITE_MAX_LENGTH: usize = 64 * MIB;
/// 단일 트랜잭션 Mutation 페이로드 상한.
pub const DEFAULT_MAX_MUTATION_PAYLOAD: usize = 64 * MIB;
/// 내부 전송 프레임 크기 (영속 Chunk 아님).
pub const DEFAULT_TRANSPORT_FRAME: usize = MIB;
/// IPC/Raft 메시지 하나의 최대 크기. Mutation 64MiB + 메타데이터 여유.
pub const MAX_MESSAGE_BYTES: usize = 64 * MIB + 4 * MIB;
/// PG Wire 메시지 최대 크기 (Bind에 32MiB TEXT 두 개 + 여유).
pub const MAX_PG_MESSAGE_BYTES: usize = 64 * MIB + 4 * MIB;
