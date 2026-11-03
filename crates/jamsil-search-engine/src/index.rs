//! 인덱스 디렉터리, 색인기(Indexer), 질의 핸들.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tantivy::collector::{Count, TopDocs};
use tantivy::query::QueryParser;
use tantivy::schema::{
    Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value as _, STORED, STRING,
};
use tantivy::{doc, Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term};

use jamsil_common::value::{pk_from_json, pk_to_json};
use jamsil_common::{DbError, Value};
use jamsil_ipc::msg::{SearchHit, SearchIndexDef, SearchIndexStatus};

use crate::analyzer::{normalize, tokenizer_name, Analyzers};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Checkpoint {
    /// 마지막으로 색인·Commit한 Outbox seq.
    pub seq: u64,
    /// 이 index까지의 RDB 변경이 검색에 반영됨.
    pub visible_index: u64,
    /// Store의 재색인 세대 (Snapshot 설치 시 증가).
    pub epoch: u64,
    /// 인덱스 이름 → 생성 세대.
    pub indexes: HashMap<String, u64>,
}

#[derive(Debug, Clone)]
pub struct IndexerConfig {
    pub root: PathBuf,
    /// 모든 인덱스 Writer가 나눠 쓰는 총 메모리 (search.indexing.max_memory_mib)
    pub memory_budget_bytes: usize,
    /// 인덱스 Writer당 색인 스레드 수 상한 (search.indexing.workers)
    pub threads: usize,
    pub max_tokens_per_doc: usize,
    pub max_token_bytes: usize,
}

/// 질의용 읽기 핸들 (색인 스레드와 독립).
pub struct QueryHandle {
    pub def: SearchIndexDef,
    index: Index,
    reader: IndexReader,
    fields: Vec<Field>,
    f_key: Field,
    pub state: RwLock<String>,
}

impl QueryHandle {
    pub fn docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }

    /// `field`의 문서 중 질의와 일치하는 것 전부를 점수와 함께 돌려준다.
    /// 일치 문서가 `limit`을 넘으면 불완전한 결과 대신 JDB-4104 오류.
    pub fn search(
        &self,
        query: &str,
        field: Option<&str>,
        limit: usize,
    ) -> Result<Vec<SearchHit>, DbError> {
        let fields: Vec<Field> = match field {
            None => self.fields.clone(),
            Some(f) => {
                let pos = self
                    .def
                    .columns
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(f))
                    .ok_or_else(|| {
                        DbError::search_index_missing(format!(
                            "search index {} does not cover column {f}",
                            self.def.name
                        ))
                    })?;
                vec![self.fields[pos]]
            }
        };
        let parser = QueryParser::for_index(&self.index, fields.clone());
        let q = parser
            .parse_query(&normalize(query))
            .map_err(|e| DbError::new("", "42601", format!("invalid search query: {e}")))?;
        // 명시적 field 질의(`title:foo`)로 TEXT_MATCH 컬럼 밖을 검색하는 것은 허용하지 않는다.
        let mut foreign = None;
        q.query_terms(&mut |t, _| {
            if !fields.contains(&t.field()) {
                foreign = Some(t.field());
            }
        });
        if let Some(f) = foreign {
            let name = self.index.schema().get_field_name(f).to_string();
            return Err(DbError::new(
                "",
                "42601",
                format!(
                    "invalid search query: field '{name}' is outside the TEXT_MATCH column{}",
                    field.map(|c| format!(" '{c}'")).unwrap_or_default()
                ),
            )
            .with_hint("use TEXT_MATCH(<column>, ...) once per column and combine with AND/OR"));
        }
        let searcher = self.reader.searcher();
        let limit = limit.max(1);
        let (total, top) = searcher
            .search(&q, &(Count, TopDocs::with_limit(limit)))
            .map_err(|e| DbError::internal(format!("search failed: {e}")))?;
        if total > limit {
            return Err(DbError::search_too_many_candidates(total, limit));
        }
        let mut out = Vec::with_capacity(top.len());
        for (score, addr) in top {
            let d: TantivyDocument = searcher
                .doc(addr)
                .map_err(|e| DbError::internal(e.to_string()))?;
            if let Some(k) = d.get_first(self.f_key).and_then(|v| v.as_str()) {
                out.push(SearchHit {
                    pk: pk_from_json(k)?,
                    score,
                });
            }
        }
        Ok(out)
    }
}

/// 질의 측과 공유하는 상태.
#[derive(Default)]
pub struct SearchShared {
    pub handles: RwLock<HashMap<String, Arc<QueryHandle>>>,
}

impl SearchShared {
    pub fn get(&self, name: &str) -> Option<Arc<QueryHandle>> {
        self.handles.read().get(name).cloned()
    }

    pub fn status(&self) -> Vec<SearchIndexStatus> {
        let mut v: Vec<SearchIndexStatus> = self
            .handles
            .read()
            .values()
            .map(|h| SearchIndexStatus {
                name: h.def.name.clone(),
                table: h.def.table.clone(),
                columns: h.def.columns.clone(),
                analyzer: h.def.analyzer.clone(),
                docs: h.docs(),
                state: h.state.read().clone(),
            })
            .collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }
}

struct WriterState {
    def: SearchIndexDef,
    writer: IndexWriter,
    fields: Vec<Field>,
    f_key: Field,
    dirty: bool,
    handle: Arc<QueryHandle>,
    /// 이 Writer에 할당된 메모리 (총 예산 / 인덱스 수)
    budget: usize,
}

/// Tantivy가 요구하는 스레드당 최소 메모리.
const MIN_THREAD_BUDGET: usize = 15_000_000;

pub struct Indexer {
    cfg: IndexerConfig,
    analyzers: Analyzers,
    writers: HashMap<String, WriterState>,
    pub ckpt: Checkpoint,
    shared: Arc<SearchShared>,
}

fn index_dir(root: &Path, name: &str, generation: u64) -> PathBuf {
    let safe: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    root.join("indexes").join(format!("{safe}__{generation}"))
}

fn atomic_write(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    std::fs::File::open(path.parent().unwrap())?.sync_all()
}

fn ierr(e: impl std::fmt::Display) -> DbError {
    DbError::internal(format!("search index: {e}"))
}

impl Indexer {
    pub fn open(cfg: IndexerConfig, shared: Arc<SearchShared>) -> Result<Indexer, DbError> {
        std::fs::create_dir_all(cfg.root.join("indexes"))?;
        std::fs::create_dir_all(cfg.root.join("checkpoints"))?;
        let analyzers = Analyzers::new(cfg.max_tokens_per_doc, cfg.max_token_bytes)
            .map_err(DbError::internal)?;
        let ckpt = match std::fs::read(cfg.root.join("checkpoints").join("state.json")) {
            Ok(b) => serde_json::from_slice(&b).unwrap_or_default(),
            Err(_) => Checkpoint::default(),
        };
        Ok(Indexer {
            cfg,
            analyzers,
            writers: HashMap::new(),
            ckpt,
            shared,
        })
    }

    fn schema(def: &SearchIndexDef) -> (Schema, Vec<Field>, Field) {
        let mut b = Schema::builder();
        let f_key = b.add_text_field("_key", STRING | STORED);
        let opts = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(tokenizer_name(&def.analyzer))
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        );
        let fields = def
            .columns
            .iter()
            .map(|c| b.add_text_field(c, opts.clone()))
            .collect();
        (b.build(), fields, f_key)
    }

    /// 인덱스를 연다. `fresh`이면 기존 디렉터리를 지우고 새로 만든다. 반환: 새로 만들었는지 (전체 색인 필요).
    pub fn ensure_index(&mut self, def: &SearchIndexDef, fresh: bool) -> Result<bool, DbError> {
        if let Some(w) = self.writers.get(&def.name) {
            if w.def.created_index == def.created_index && w.def.columns == def.columns && !fresh {
                return Ok(false);
            }
        }
        let known = self.ckpt.indexes.get(&def.name) == Some(&def.created_index);
        if self.writers.contains_key(&def.name) {
            self.drop_index(&def.name);
        }
        let dir = index_dir(&self.cfg.root, &def.name, def.created_index);
        let (schema, fields, f_key) = Self::schema(def);
        let mut created = false;
        let index = if !fresh && known && dir.join("meta.json").exists() {
            match Index::open_in_dir(&dir) {
                Ok(i) if i.schema() == schema => i,
                other => {
                    let why = match &other {
                        Ok(_) => "schema changed".to_string(),
                        Err(e) => e.to_string(),
                    };
                    drop(other);
                    tracing::warn!("search index {} is unusable ({why}); rebuilding", def.name);
                    let _ = std::fs::remove_dir_all(&dir);
                    created = true;
                    std::fs::create_dir_all(&dir)?;
                    Index::create_in_dir(&dir, schema.clone()).map_err(ierr)?
                }
            }
        } else {
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir)?;
            created = true;
            Index::create_in_dir(&dir, schema.clone()).map_err(ierr)?
        };
        self.analyzers.register(&index);
        let (threads, budget) = self.writer_budget(self.writers.len() + 1)?;
        let writer: IndexWriter = index.writer_with_num_threads(threads, budget).map_err(ierr)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(ierr)?;
        let handle = Arc::new(QueryHandle {
            def: def.clone(),
            index,
            reader,
            fields: fields.clone(),
            f_key,
            state: RwLock::new(if created {
                "building".into()
            } else {
                "ready".into()
            }),
        });
        self.shared
            .handles
            .write()
            .insert(def.name.clone(), handle.clone());
        self.writers.insert(
            def.name.clone(),
            WriterState {
                def: def.clone(),
                writer,
                fields,
                f_key,
                dirty: created,
                handle,
                budget,
            },
        );
        self.ckpt
            .indexes
            .insert(def.name.clone(), def.created_index);
        Ok(created)
    }

    /// 인덱스 `n`개가 총 예산을 나눌 때 Writer 하나의 (스레드 수, 메모리).
    /// 총 예산을 넘겨 할당하지 않는다: 인덱스당 최소 메모리를 줄 수 없으면 오류.
    fn writer_budget(&self, n: usize) -> Result<(usize, usize), DbError> {
        let per = self.cfg.memory_budget_bytes / n.max(1);
        if per < MIN_THREAD_BUDGET {
            return Err(DbError::new(
                "",
                "53200",
                format!(
                    "search.indexing.max_memory_mib ({} MiB) is too small for {n} search indexes (needs {} MiB)",
                    self.cfg.memory_budget_bytes >> 20,
                    (MIN_THREAD_BUDGET * n).div_ceil(1 << 20)
                ),
            )
            .with_hint("raise search.indexing.max_memory_mib or drop unused search indexes"));
        }
        let threads = self.cfg.threads.clamp(1, (per / MIN_THREAD_BUDGET).max(1));
        Ok((threads, per))
    }

    /// 인덱스 추가/삭제 후 모든 Writer의 메모리를 다시 나눈다 (Commit 직후 호출: 대기 문서 없음).
    fn rebalance(&mut self) -> Result<(), DbError> {
        let n = self.writers.len();
        if n == 0 {
            return Ok(());
        }
        let (threads, per) = self.writer_budget(n)?;
        let names: Vec<String> = self
            .writers
            .iter()
            .filter(|(_, w)| w.budget != per && !w.dirty)
            .map(|(k, _)| k.clone())
            .collect();
        for name in names {
            let WriterState {
                def,
                writer,
                fields,
                f_key,
                dirty,
                handle,
                ..
            } = self.writers.remove(&name).unwrap();
            // 기존 Writer를 먼저 닫아야 디렉터리 잠금이 풀린다.
            drop(writer);
            let writer = handle
                .index
                .writer_with_num_threads(threads, per)
                .map_err(ierr)?;
            self.writers.insert(
                name,
                WriterState {
                    def,
                    writer,
                    fields,
                    f_key,
                    dirty,
                    handle,
                    budget: per,
                },
            );
        }
        Ok(())
    }

    /// 현재 Writer들에 할당된 메모리 합계 (지표용).
    pub fn allocated_budget(&self) -> usize {
        self.writers.values().map(|w| w.budget).sum()
    }

    pub fn drop_index(&mut self, name: &str) {
        if let Some(w) = self.writers.remove(name) {
            let dir = index_dir(&self.cfg.root, name, w.def.created_index);
            drop(w);
            self.shared.handles.write().remove(name);
            let _ = std::fs::remove_dir_all(dir);
        }
        self.ckpt.indexes.remove(name);
    }

    /// 체크포인트에 없는 디렉터리 정리.
    pub fn gc_dirs(&self) {
        let Ok(rd) = std::fs::read_dir(self.cfg.root.join("indexes")) else {
            return;
        };
        let keep: Vec<PathBuf> = self
            .writers
            .values()
            .map(|w| index_dir(&self.cfg.root, &w.def.name, w.def.created_index))
            .collect();
        for e in rd.flatten() {
            if !keep.contains(&e.path()) {
                let _ = std::fs::remove_dir_all(e.path());
            }
        }
    }

    pub fn has_index(&self, name: &str) -> bool {
        self.writers.contains_key(name)
    }

    pub fn defs(&self) -> Vec<SearchIndexDef> {
        self.writers.values().map(|w| w.def.clone()).collect()
    }

    pub fn reset_all(&mut self) {
        let names: Vec<String> = self.writers.keys().cloned().collect();
        for n in names {
            self.drop_index(&n);
        }
        self.ckpt = Checkpoint::default();
        let _ = std::fs::remove_dir_all(self.cfg.root.join("indexes"));
        let _ = std::fs::create_dir_all(self.cfg.root.join("indexes"));
    }

    pub fn upsert(
        &mut self,
        name: &str,
        pk: &[Value],
        texts: &[Option<String>],
    ) -> Result<(), DbError> {
        let Some(w) = self.writers.get_mut(name) else {
            return Ok(());
        };
        let key = pk_to_json(pk);
        w.writer.delete_term(Term::from_field_text(w.f_key, &key));
        let mut d = doc!(w.f_key => key);
        for (i, f) in w.fields.iter().enumerate() {
            if let Some(Some(t)) = texts.get(i) {
                d.add_text(*f, normalize(t));
            }
        }
        w.writer.add_document(d).map_err(ierr)?;
        w.dirty = true;
        Ok(())
    }

    pub fn delete(&mut self, name: &str, pk: &[Value]) {
        if let Some(w) = self.writers.get_mut(name) {
            w.writer
                .delete_term(Term::from_field_text(w.f_key, &pk_to_json(pk)));
            w.dirty = true;
        }
    }

    pub fn any_dirty(&self) -> bool {
        self.writers.values().any(|w| w.dirty)
    }

    /// 변경된 인덱스 Commit → Reader Reload → Checkpoint 원자 기록.
    pub fn commit(&mut self, ckpt: Checkpoint) -> Result<(), DbError> {
        let payload = serde_json::to_string(&ckpt).map_err(ierr)?;
        for w in self.writers.values_mut() {
            if w.dirty {
                let mut p = w.writer.prepare_commit().map_err(ierr)?;
                p.set_payload(&payload);
                p.commit().map_err(ierr)?;
                w.dirty = false;
            }
        }
        let mut ck = ckpt;
        ck.indexes = self
            .writers
            .values()
            .map(|w| (w.def.name.clone(), w.def.created_index))
            .collect();
        atomic_write(
            &self.cfg.root.join("checkpoints").join("state.json"),
            &serde_json::to_vec(&ck).map_err(ierr)?,
        )?;
        for w in self.writers.values() {
            w.handle.reader.reload().map_err(ierr)?;
            *w.handle.state.write() = "ready".into();
        }
        self.ckpt = ck;
        // 인덱스 추가/삭제로 몫이 바뀐 Writer만 다시 만든다 (합계가 총 예산을 넘지 않게).
        self.rebalance()?;
        Ok(())
    }

    pub fn mark_building(&self, name: &str) {
        if let Some(w) = self.writers.get(name) {
            *w.handle.state.write() = "building".into();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(name: &str, analyzer: &str) -> SearchIndexDef {
        SearchIndexDef {
            name: name.into(),
            table: "documents".into(),
            columns: vec!["content".into()],
            pk_columns: vec!["id".into()],
            analyzer: analyzer.into(),
            scoring: "bm25".into(),
            created_index: 7,
        }
    }

    #[test]
    fn korean_term_boolean_phrase_bm25() {
        let d = tempfile::tempdir().unwrap();
        let shared = Arc::new(SearchShared::default());
        let cfg = IndexerConfig {
            root: d.path().to_path_buf(),
            memory_budget_bytes: 50_000_000,
            threads: 1,
            max_tokens_per_doc: 1_000_000,
            max_token_bytes: 256,
        };
        let mut ix = Indexer::open(cfg.clone(), shared.clone()).unwrap();
        assert!(ix.ensure_index(&def("idx", "korean"), false).unwrap());
        let docs = [
            (1, "금융 시장에서 주식 투자가 늘었다"),
            (2, "채권 금리가 하락하고 금융 투자 심리가 회복되었다"),
            (3, "인공지능 기술이 산업을 바꾼다"),
            (4, "금융 투자 금융 투자 금융 투자"),
        ];
        for (id, t) in docs {
            ix.upsert("idx", &[Value::Int(id)], &[Some(t.to_string())])
                .unwrap();
        }
        ix.commit(Checkpoint {
            seq: 4,
            visible_index: 10,
            epoch: 0,
            indexes: HashMap::new(),
        })
        .unwrap();
        let h = shared.get("idx").unwrap();
        let ids = |q: &str| -> Vec<i64> {
            h.search(q, None, 10)
                .unwrap()
                .into_iter()
                .map(|x| match x.pk[0] {
                    Value::Int(i) => i,
                    _ => -1,
                })
                .collect()
        };
        let mut r = ids("금융 AND (주식 OR 채권)");
        r.sort();
        assert_eq!(r, vec![1, 2]);
        let mut r = ids("\"금융 투자\"");
        r.sort();
        assert_eq!(r, vec![2, 4]);
        assert_eq!(ids("인공지능"), vec![3]);
        // BM25: 반복 빈도 높은 4번이 "투자" 최상위
        assert_eq!(ids("투자")[0], 4);
        // 삭제/갱신
        ix.delete("idx", &[Value::Int(3)]);
        ix.upsert("idx", &[Value::Int(1)], &[Some("완전히 다른 내용".into())])
            .unwrap();
        ix.commit(Checkpoint {
            seq: 6,
            visible_index: 12,
            ..Default::default()
        })
        .unwrap();
        assert!(ids("인공지능").is_empty());
        assert!(!ids("주식").contains(&1));
        // 재시작 후 Checkpoint/인덱스 재사용
        drop(ix);
        let shared2 = Arc::new(SearchShared::default());
        let mut ix2 = Indexer::open(cfg, shared2.clone()).unwrap();
        assert_eq!(ix2.ckpt.seq, 6);
        assert!(!ix2.ensure_index(&def("idx", "korean"), false).unwrap());
        assert_eq!(
            shared2
                .get("idx")
                .unwrap()
                .search("다른", None, 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn field_scoped_and_complete_or_fail() {
        let d = tempfile::tempdir().unwrap();
        let shared = Arc::new(SearchShared::default());
        let cfg = IndexerConfig {
            root: d.path().to_path_buf(),
            memory_budget_bytes: 50_000_000,
            threads: 1,
            max_tokens_per_doc: 1_000_000,
            max_token_bytes: 256,
        };
        let mut ix = Indexer::open(cfg, shared.clone()).unwrap();
        let mut def = def("f", "standard");
        def.columns = vec!["title".into(), "body".into()];
        ix.ensure_index(&def, false).unwrap();
        ix.upsert("f", &[Value::Int(1)], &[Some("alpha".into()), Some("beta".into())]).unwrap();
        ix.upsert("f", &[Value::Int(2)], &[Some("gamma".into()), Some("alpha".into())]).unwrap();
        ix.upsert("f", &[Value::Int(3)], &[Some("alpha".into()), Some("alpha".into())]).unwrap();
        ix.commit(Checkpoint { seq: 3, visible_index: 3, ..Default::default() }).unwrap();
        let h = shared.get("f").unwrap();
        let ids = |q: &str, f: Option<&str>| -> Vec<i64> {
            let mut v: Vec<i64> = h
                .search(q, f, 100)
                .unwrap()
                .into_iter()
                .map(|x| match x.pk[0] {
                    Value::Int(i) => i,
                    _ => -1,
                })
                .collect();
            v.sort();
            v
        };
        assert_eq!(ids("alpha", Some("body")), vec![2, 3]);
        assert_eq!(ids("alpha", Some("title")), vec![1, 3]);
        assert_eq!(ids("alpha AND beta", Some("body")), Vec::<i64>::new());
        assert_eq!(ids("alpha", None), vec![1, 2, 3]);
        // 다른 필드를 명시한 질의는 거부
        assert_eq!(h.search("title:alpha", Some("body"), 100).unwrap_err().sqlstate, "42601");
        assert!(h.search("missing", Some("nope"), 100).is_err());
        // 상한 초과: 불완전 결과 대신 오류
        let e = h.search("alpha", None, 2).unwrap_err();
        assert_eq!(e.code, "JDB-4104");
        assert_eq!(h.search("alpha", None, 3).unwrap().len(), 3);
    }

    #[test]
    fn writer_budgets_never_exceed_total() {
        let d = tempfile::tempdir().unwrap();
        let shared = Arc::new(SearchShared::default());
        let cfg = IndexerConfig {
            root: d.path().to_path_buf(),
            memory_budget_bytes: 50_000_000,
            threads: 4,
            max_tokens_per_doc: 1_000_000,
            max_token_bytes: 256,
        };
        let mut ix = Indexer::open(cfg, shared).unwrap();
        ix.ensure_index(&def("a", "standard"), false).unwrap();
        ix.ensure_index(&def("b", "standard"), false).unwrap();
        ix.ensure_index(&def("c", "standard"), false).unwrap();
        ix.commit(Checkpoint::default()).unwrap();
        assert!(ix.allocated_budget() <= 50_000_000, "{}", ix.allocated_budget());
        // 15MB 미만으로 나눠야 하는 인덱스는 거부
        let e = ix.ensure_index(&def("d", "standard"), false).unwrap_err();
        assert_eq!(e.sqlstate, "53200");
        ix.drop_index("c");
        ix.commit(Checkpoint::default()).unwrap();
        assert_eq!(ix.allocated_budget(), 50_000_000);
    }
}
