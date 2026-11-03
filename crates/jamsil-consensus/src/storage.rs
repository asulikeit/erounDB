//! Raft 영속 상태: Vote(HardState), 로그 세그먼트, Snapshot 메타데이터.
//!
//! 로그 레코드 포맷 (little-endian):
//!
//! ```text
//! magic u32 = "JRL1" | len u32 | term u64 | index u64 | crc32 u32 (term|index|payload) | payload[len]
//! ```
//!
//! 마지막 세그먼트 끝의 불완전/손상 레코드는 미완료 쓰기로 간주하고 잘라낸다 (fsync 전 Crash).
//! 중간 세그먼트의 손상은 복구 불가로 보고 기동을 거부한다.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

const MAGIC: u32 = u32::from_le_bytes(*b"JRL1");
const HEADER: usize = 4 + 4 + 8 + 8 + 4;
const SEGMENT_TARGET: u64 = 64 * 1024 * 1024;
const CACHE_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    pub term: u64,
    pub voted_for: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub index: u64,
    pub term: u64,
    pub file: String,
    pub size: u64,
}

fn fsync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// tmp에 쓰고 fsync 후 rename → 디렉터리 fsync.
pub fn atomic_write(path: &Path, data: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    fsync_dir(path.parent().unwrap())
}

fn encode_checked<T: Serialize>(v: &T) -> Vec<u8> {
    let body = bincode::serialize(v).unwrap();
    let mut out = Vec::with_capacity(body.len() + 4);
    out.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

fn decode_checked<T: for<'de> Deserialize<'de>>(b: &[u8]) -> io::Result<T> {
    if b.len() < 4 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "short file"));
    }
    let crc = u32::from_le_bytes(b[..4].try_into().unwrap());
    if crc32fast::hash(&b[4..]) != crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "checksum mismatch",
        ));
    }
    bincode::deserialize(&b[4..])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

#[derive(Debug, Clone, Copy)]
struct EntryMeta {
    term: u64,
    seg: usize,
    offset: u64,
    len: u32,
}

struct Segment {
    first_index: u64,
    path: PathBuf,
    file: File,
    size: u64,
}

pub struct RaftStorage {
    dir: PathBuf,
    pub hard: HardState,
    pub snapshot: SnapshotMeta,
    segments: Vec<Segment>,
    /// metas[i] = index (base + i)
    metas: Vec<EntryMeta>,
    base: u64,
    cache: BTreeMap<u64, Arc<Vec<u8>>>,
    cache_bytes: usize,
    /// 아직 fsync 되지 않은 쓰기가 있는 세그먼트
    dirty: bool,
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub index: u64,
    pub term: u64,
    pub data: Arc<Vec<u8>>,
}

impl RaftStorage {
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn open(dir: &Path) -> io::Result<RaftStorage> {
        fs::create_dir_all(dir.join("logs"))?;
        fs::create_dir_all(dir.join("snapshots"))?;
        let hard = match fs::read(dir.join("vote_state")) {
            Ok(b) => decode_checked(&b)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => HardState::default(),
            Err(e) => return Err(e),
        };
        let snapshot = match fs::read(dir.join("snapshot.meta")) {
            Ok(b) => decode_checked(&b)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => SnapshotMeta::default(),
            Err(e) => return Err(e),
        };
        let mut s = RaftStorage {
            dir: dir.to_path_buf(),
            hard,
            snapshot,
            segments: vec![],
            metas: vec![],
            base: 0,
            cache: BTreeMap::new(),
            cache_bytes: 0,
            dirty: false,
        };
        s.recover()?;
        Ok(s)
    }

    fn recover(&mut self) -> io::Result<()> {
        let mut names: Vec<(u64, PathBuf)> = fs::read_dir(self.dir.join("logs"))?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let p = e.path();
                let n = p
                    .file_name()?
                    .to_str()?
                    .strip_suffix(".seg")?
                    .parse::<u64>()
                    .ok()?;
                Some((n, p))
            })
            .collect();
        names.sort();
        let nseg = names.len();
        let mut expected: Option<u64> = None;
        for (si, (first, path)) in names.into_iter().enumerate() {
            let last_seg = si + 1 == nseg;
            let mut f = OpenOptions::new().read(true).append(true).open(&path)?;
            let len = f.metadata()?.len();
            let mut buf = Vec::new();
            f.read_to_end(&mut buf)?;
            let mut off = 0usize;
            let seg_idx = self.segments.len();
            let mut valid_end = 0u64;
            let mut seg_first = first;
            let mut first_rec = true;
            while off < buf.len() {
                let rec = parse_record(&buf[off..]);
                match rec {
                    Some((term, index, plen)) => {
                        if first_rec {
                            seg_first = index;
                            first_rec = false;
                        }
                        if let Some(e) = expected {
                            if index != e {
                                if index < e && index > self.snapshot.index {
                                    // 이전 truncate 잔재일 수 없음: 연속성 위반
                                    return Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        format!("raft log discontinuity at {index} (expected {e})"),
                                    ));
                                }
                                if index > e {
                                    return Err(io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        format!("raft log gap: expected {e}, found {index}"),
                                    ));
                                }
                            }
                        }
                        if self.metas.is_empty() {
                            self.base = index;
                        }
                        self.metas.push(EntryMeta {
                            term,
                            seg: seg_idx,
                            offset: (off + HEADER) as u64,
                            len: plen as u32,
                        });
                        expected = Some(index + 1);
                        off += HEADER + plen;
                        valid_end = off as u64;
                    }
                    None => {
                        if last_seg {
                            tracing::warn!(
                                "truncating torn raft log tail in {} at offset {} (file size {len})",
                                path.display(),
                                off
                            );
                            break;
                        }
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("corrupted raft log segment {}", path.display()),
                        ));
                    }
                }
            }
            if valid_end < len {
                f.set_len(valid_end)?;
                f.sync_all()?;
            }
            f.seek(SeekFrom::End(0))?;
            if valid_end == 0 && !last_seg {
                drop(f);
                fs::remove_file(&path)?;
                continue;
            }
            self.segments.push(Segment {
                first_index: seg_first,
                path,
                file: f,
                size: valid_end,
            });
        }
        // 스냅샷 이전 항목은 무시
        if !self.metas.is_empty() && self.base <= self.snapshot.index {
            let drop_n = ((self.snapshot.index + 1 - self.base) as usize).min(self.metas.len());
            self.metas.drain(..drop_n);
            self.base = self.snapshot.index + 1;
        }
        if self.metas.is_empty() {
            self.base = self.snapshot.index + 1;
        }
        Ok(())
    }

    pub fn save_hard_state(&mut self, hs: HardState) -> io::Result<()> {
        atomic_write(&self.dir.join("vote_state"), &encode_checked(&hs))?;
        self.hard = hs;
        Ok(())
    }

    pub fn first_index(&self) -> u64 {
        self.base
    }

    pub fn last_index(&self) -> u64 {
        self.base + self.metas.len() as u64 - 1
    }

    pub fn last_term(&self) -> u64 {
        self.metas
            .last()
            .map(|m| m.term)
            .unwrap_or(self.snapshot.term)
    }

    /// index의 term. 스냅샷 경계도 처리. 범위 밖이면 None.
    pub fn term(&self, index: u64) -> Option<u64> {
        if index == self.snapshot.index {
            return Some(self.snapshot.term);
        }
        if index == 0 {
            return Some(0);
        }
        if index < self.base || index > self.last_index() {
            return None;
        }
        Some(self.metas[(index - self.base) as usize].term)
    }

    pub fn entry(&mut self, index: u64) -> io::Result<Entry> {
        let m = *self
            .metas
            .get((index.wrapping_sub(self.base)) as usize)
            .filter(|_| index >= self.base)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("log entry {index} not available"),
                )
            })?;
        if let Some(d) = self.cache.get(&index) {
            return Ok(Entry {
                index,
                term: m.term,
                data: d.clone(),
            });
        }
        let mut buf = vec![0u8; m.len as usize];
        self.segments[m.seg]
            .file
            .read_exact_at(&mut buf, m.offset)?;
        let data = Arc::new(buf);
        Ok(Entry {
            index,
            term: m.term,
            data,
        })
    }

    /// [from, to] 범위를 max_bytes 이내로 (최소 1개).
    pub fn entries(&mut self, from: u64, to: u64, max_bytes: usize) -> io::Result<Vec<Entry>> {
        let mut out = vec![];
        let mut bytes = 0usize;
        let mut i = from;
        while i <= to {
            let e = self.entry(i)?;
            bytes += e.data.len();
            if !out.is_empty() && bytes > max_bytes {
                break;
            }
            out.push(e);
            i += 1;
        }
        Ok(out)
    }

    fn cache_put(&mut self, index: u64, data: Arc<Vec<u8>>) {
        self.cache_bytes += data.len();
        self.cache.insert(index, data);
        while self.cache_bytes > CACHE_BYTES {
            let Some((&k, _)) = self.cache.iter().next() else {
                break;
            };
            if let Some(v) = self.cache.remove(&k) {
                self.cache_bytes -= v.len();
            }
        }
    }

    fn active_segment(&mut self, next_index: u64) -> io::Result<usize> {
        let need_new = match self.segments.last() {
            None => true,
            Some(s) => s.size >= SEGMENT_TARGET,
        };
        if need_new {
            if self.dirty {
                self.sync()?;
            }
            let path = self.dir.join("logs").join(format!("{next_index:020}.seg"));
            let f = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&path)?;
            fsync_dir(&self.dir.join("logs"))?;
            self.segments.push(Segment {
                first_index: next_index,
                path,
                file: f,
                size: 0,
            });
        }
        Ok(self.segments.len() - 1)
    }

    /// 엔트리 추가 (fsync는 sync()에서). index는 last_index+1 이어야 한다.
    pub fn append(&mut self, index: u64, term: u64, data: Arc<Vec<u8>>) -> io::Result<()> {
        if index != self.last_index() + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("append index {index} != last+1 {}", self.last_index() + 1),
            ));
        }
        let seg = self.active_segment(index)?;
        let mut hdr = [0u8; HEADER];
        hdr[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        hdr[4..8].copy_from_slice(&(data.len() as u32).to_le_bytes());
        hdr[8..16].copy_from_slice(&term.to_le_bytes());
        hdr[16..24].copy_from_slice(&index.to_le_bytes());
        let mut h = crc32fast::Hasher::new();
        h.update(&hdr[8..24]);
        h.update(&data);
        hdr[24..28].copy_from_slice(&h.finalize().to_le_bytes());
        let s = &mut self.segments[seg];
        let offset = s.size + HEADER as u64;
        s.file.write_all(&hdr)?;
        s.file.write_all(&data)?;
        s.size += (HEADER + data.len()) as u64;
        self.metas.push(EntryMeta {
            term,
            seg,
            offset,
            len: data.len() as u32,
        });
        self.cache_put(index, data);
        self.dirty = true;
        Ok(())
    }

    /// 비동기 fsync 준비: 기록 중인 세그먼트 파일 핸들 복제본 (없으면 None = 이미 durable).
    /// 호출자는 복제본을 다른 스레드에서 `sync_data()` 한 뒤에만 그 시점까지의 엔트리를 durable로 본다.
    pub fn begin_async_sync(&mut self) -> io::Result<Option<File>> {
        if !self.dirty {
            return Ok(None);
        }
        let f = match self.segments.last() {
            Some(s) => s.file.try_clone()?,
            None => return Ok(None),
        };
        self.dirty = false;
        Ok(Some(f))
    }

    /// dirty 표시와 무관하게 마지막 세그먼트를 fsync (비동기 fsync 진행 중 역할이 바뀐 경우).
    pub fn sync_force(&mut self) -> io::Result<()> {
        if let Some(s) = self.segments.last() {
            s.file.sync_data()?;
        }
        self.dirty = false;
        Ok(())
    }

    /// 내구성 확보 (fdatasync). Durable ACK는 이 호출 이후에만 가능하다.
    pub fn sync(&mut self) -> io::Result<()> {
        if self.dirty {
            if let Some(s) = self.segments.last() {
                s.file.sync_data()?;
            }
            self.dirty = false;
        }
        Ok(())
    }

    /// index 이후(포함) 엔트리 삭제 (미확정 충돌 로그 정리).
    pub fn truncate_from(&mut self, index: u64) -> io::Result<()> {
        if index > self.last_index() {
            return Ok(());
        }
        if index < self.base {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "truncate before first index",
            ));
        }
        let pos = (index - self.base) as usize;
        let m = self.metas[pos];
        let cut = m.offset - HEADER as u64;
        // 이후 세그먼트 삭제
        while self.segments.len() > m.seg + 1 {
            let s = self.segments.pop().unwrap();
            drop(s.file);
            fs::remove_file(&s.path)?;
        }
        let s = &mut self.segments[m.seg];
        s.file.set_len(cut)?;
        s.file.sync_all()?;
        s.size = cut;
        fsync_dir(&self.dir.join("logs"))?;
        self.metas.truncate(pos);
        let keys: Vec<u64> = self.cache.range(index..).map(|(k, _)| *k).collect();
        for k in keys {
            if let Some(v) = self.cache.remove(&k) {
                self.cache_bytes -= v.len();
            }
        }
        Ok(())
    }

    /// Snapshot 이후 앞부분 로그 제거. `upto` 이하 엔트리는 더 이상 제공하지 않는다.
    pub fn compact(&mut self, upto: u64) -> io::Result<()> {
        if upto < self.base {
            return Ok(());
        }
        let upto = upto.min(self.last_index());
        let n = (upto + 1 - self.base) as usize;
        self.metas.drain(..n);
        self.base = upto + 1;
        // 전체가 upto 이하인 세그먼트 삭제
        let mut removed = 0;
        while self.segments.len() > 1 {
            let next_first = self.segments[1].first_index;
            if next_first <= upto + 1 {
                let s = self.segments.remove(0);
                drop(s.file);
                fs::remove_file(&s.path)?;
                removed += 1;
            } else {
                break;
            }
        }
        if removed > 0 {
            for m in self.metas.iter_mut() {
                m.seg -= removed;
            }
            fsync_dir(&self.dir.join("logs"))?;
        }
        let keys: Vec<u64> = self.cache.range(..=upto).map(|(k, _)| *k).collect();
        for k in keys {
            if let Some(v) = self.cache.remove(&k) {
                self.cache_bytes -= v.len();
            }
        }
        Ok(())
    }

    /// 로그 전체 폐기 (Snapshot 설치 시 로그가 Snapshot과 맞지 않을 때).
    pub fn reset_to_snapshot(&mut self) -> io::Result<()> {
        for s in self.segments.drain(..) {
            drop(s.file);
            fs::remove_file(&s.path)?;
        }
        fsync_dir(&self.dir.join("logs"))?;
        self.metas.clear();
        self.cache.clear();
        self.cache_bytes = 0;
        self.base = self.snapshot.index + 1;
        self.dirty = false;
        Ok(())
    }

    pub fn snapshots_dir(&self) -> PathBuf {
        self.dir.join("snapshots")
    }

    pub fn snapshot_path(&self) -> Option<PathBuf> {
        if self.snapshot.file.is_empty() {
            None
        } else {
            Some(self.snapshots_dir().join(&self.snapshot.file))
        }
    }

    /// 새 Snapshot 메타 기록 + 이전 Snapshot 파일 삭제.
    pub fn set_snapshot(&mut self, meta: SnapshotMeta) -> io::Result<()> {
        let old = self.snapshot.file.clone();
        atomic_write(&self.dir.join("snapshot.meta"), &encode_checked(&meta))?;
        self.snapshot = meta;
        if !old.is_empty() && old != self.snapshot.file {
            let _ = fs::remove_file(self.snapshots_dir().join(old));
        }
        // 정리되지 않은 임시 파일 삭제
        if let Ok(rd) = fs::read_dir(self.snapshots_dir()) {
            for e in rd.flatten() {
                let n = e.file_name().to_string_lossy().to_string();
                if n != self.snapshot.file
                    && n.ends_with(".sqlite")
                    && !n.starts_with("incoming")
                    && !n.starts_with("creating")
                {
                    let _ = fs::remove_file(e.path());
                }
            }
        }
        if self.base <= self.snapshot.index {
            // 스냅샷이 로그보다 앞서면 겹치는 부분 정리
            if self.term(self.snapshot.index) == Some(self.snapshot.term)
                && self.snapshot.index <= self.last_index()
            {
                self.compact(self.snapshot.index)?;
            }
        }
        Ok(())
    }

    pub fn log_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.size).sum()
    }
}

fn parse_record(b: &[u8]) -> Option<(u64, u64, usize)> {
    if b.len() < HEADER {
        return None;
    }
    let magic = u32::from_le_bytes(b[0..4].try_into().unwrap());
    if magic != MAGIC {
        return None;
    }
    let len = u32::from_le_bytes(b[4..8].try_into().unwrap()) as usize;
    if b.len() < HEADER + len {
        return None;
    }
    let term = u64::from_le_bytes(b[8..16].try_into().unwrap());
    let index = u64::from_le_bytes(b[16..24].try_into().unwrap());
    let crc = u32::from_le_bytes(b[24..28].try_into().unwrap());
    let mut h = crc32fast::Hasher::new();
    h.update(&b[8..24]);
    h.update(&b[HEADER..HEADER + len]);
    if h.finalize() != crc {
        return None;
    }
    Some((term, index, len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_reopen_truncate_compact() {
        let d = tempfile::tempdir().unwrap();
        {
            let mut s = RaftStorage::open(d.path()).unwrap();
            assert_eq!(s.last_index(), 0);
            for i in 1..=10u64 {
                s.append(i, 1 + i / 5, Arc::new(vec![i as u8; 100]))
                    .unwrap();
            }
            s.sync().unwrap();
            s.save_hard_state(HardState {
                term: 3,
                voted_for: 2,
            })
            .unwrap();
        }
        let mut s = RaftStorage::open(d.path()).unwrap();
        assert_eq!(s.last_index(), 10);
        assert_eq!(
            s.hard,
            HardState {
                term: 3,
                voted_for: 2
            }
        );
        assert_eq!(s.term(5), Some(2));
        assert_eq!(*s.entry(7).unwrap().data, vec![7u8; 100]);
        s.truncate_from(8).unwrap();
        assert_eq!(s.last_index(), 7);
        s.append(8, 9, Arc::new(vec![1])).unwrap();
        s.sync().unwrap();
        drop(s);
        let mut s = RaftStorage::open(d.path()).unwrap();
        assert_eq!(s.last_index(), 8);
        assert_eq!(s.term(8), Some(9));
        s.compact(5).unwrap();
        assert_eq!(s.first_index(), 6);
        assert!(s.entry(5).is_err());
        assert_eq!(*s.entry(6).unwrap().data, vec![6u8; 100]);
    }

    #[test]
    fn torn_tail_is_truncated() {
        let d = tempfile::tempdir().unwrap();
        {
            let mut s = RaftStorage::open(d.path()).unwrap();
            for i in 1..=3u64 {
                s.append(i, 1, Arc::new(vec![9u8; 1000])).unwrap();
            }
            s.sync().unwrap();
        }
        // 마지막 레코드 일부만 기록된 상황 재현
        let seg = d.path().join("logs").join(format!("{:020}.seg", 1));
        let len = fs::metadata(&seg).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&seg)
            .unwrap()
            .set_len(len - 10)
            .unwrap();
        let s = RaftStorage::open(d.path()).unwrap();
        assert_eq!(s.last_index(), 2);
    }
}
