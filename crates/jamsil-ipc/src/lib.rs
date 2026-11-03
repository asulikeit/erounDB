//! 노드 내부 UDS 및 노드 간 TCP 공통 전송 계층.
//!
//! * `frame`: 길이 제한·체크섬 프레임. 큰 메시지(최대 64MiB Mutation)는 1MiB 프레임으로
//!   나누어 전송하며, 수신 측은 모든 프레임 수신 + CRC 검증 후에만 메시지를 넘긴다.
//! * `rpc`: 상관관계 ID 기반 다중화 요청/응답.
//! * `msg`: Store/Sync/Search/Log 프로세스 간 메시지 계약.

pub mod frame;
pub mod msg;
pub mod rpc;

pub use rpc::{connect_unix, serve_unix, Handler, RpcClient, RpcError};
