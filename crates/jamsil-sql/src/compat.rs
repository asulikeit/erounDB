//! 클라이언트 드라이버 메타데이터 질의 호환 재작성 (pgjdbc 42.7.x 기준).
//!
//! PostgreSQL 전용 구문(집합 반환 함수 `_pg_expandarray`, 복합 필드 접근 `(x).n`, 배열 첨자,
//! `generate_series(...) alias(col)`)을 SQLite에서 실행 가능한 동등 표현으로 바꾼다.
//! 지원 범위는 드라이버 버전을 고정한 통합 시험으로 선언한다 (설계서 4.3).

use std::borrow::Cow;
use std::sync::OnceLock;

use regex::Regex;

struct Rule {
    re: Regex,
    rep: &'static str,
}

fn rules() -> &'static [Rule] {
    static R: OnceLock<Vec<Rule>> = OnceLock::new();
    R.get_or_init(|| {
        let r = |p: &str, rep: &'static str| Rule { re: Regex::new(p).unwrap(), rep };
        vec![
            // getPrimaryKeys: KEY_SEQ / KEYS / (result.KEYS).x
            r(
                r"(?i)\(\s*(?:information_schema\.)?_pg_expandarray\(\s*i\.indkey\s*\)\s*\)\.n\s+AS\s+KEY_SEQ",
                "_jamsil_array_pos(i.indkey, a.attnum) AS KEY_SEQ",
            ),
            r(
                r"(?i)(?:information_schema\.)?_pg_expandarray\(\s*i\.indkey\s*\)\s+AS\s+KEYS\s*,",
                "_jamsil_array_pos(i.indkey, a.attnum) AS KEYS,",
            ),
            r(r"(?i)result\.A_ATTNUM\s*=\s*\(\s*result\.KEYS\s*\)\.x", "result.KEYS IS NOT NULL"),
            // getIndexInfo: 인덱스 키 위치별 Row
            r(
                r"(?i)\(\s*(?:information_schema\.)?_pg_expandarray\(\s*i\.indkey\s*\)\s*\)\.n\s+AS\s+ORDINAL_POSITION",
                "_k.pos AS ORDINAL_POSITION",
            ),
            r(
                r"(?i)JOIN\s+pg_catalog\.pg_am\s+am\s+ON\s+\(ci\.relam\s*=\s*am\.oid\)\s+WHERE\s+true",
                "JOIN pg_catalog.pg_am am ON (ci.relam = am.oid) JOIN _jm_indkeys _k ON (_k.indexrelid = i.indexrelid) WHERE true",
            ),
            r(
                r"(?i)(?:pg_catalog\.)?pg_get_indexdef\(\s*([\w.$]+)\s*,\s*([\w.$]+)\s*,\s*(?:false|true)\s*\)",
                "(SELECT _k2.colname FROM _jm_indkeys _k2 WHERE _k2.indexrelid = $1 AND _k2.pos = $2)",
            ),
            r(
                r"(?i)(?:pg_catalog\.)?pg_get_indexdef\(\s*([\w.$]+)\s*\)",
                "(SELECT _ix.indexdef FROM pg_indexes _ix WHERE _jamsil_oid('i', _ix.indexname) = $1)",
            ),
            r(r"(?i)[\w.]+\[[^\]]+\]\s*&\s*1::smallint", "0"),
            // getBestRowIdentifier
            r(
                r"(?i)(?:information_schema\.)?_pg_expandarray\(\s*i\.indkey\s*\)\s+AS\s+keys\s+FROM\s+pg_catalog\.pg_index\s+i",
                "_k.attnum AS keys FROM pg_catalog.pg_index i JOIN _jm_indkeys _k ON (_k.indexrelid = i.indexrelid)",
            ),
            r(r"(?i)\(\s*i\.keys\s*\)\.x", "i.keys"),
            // getImportedKeys/getExportedKeys/getCrossReference
            // generate_series(a, b) [AS] alias(col) → json_each 기반 정수 열
            r(
                r"(?i)(?:pg_catalog\.)?generate_series\(((?:[^()]|\((?:[^()]|\([^()]*\))*\))*)\)\s+(?:AS\s+)?(\w+)\s*\(\s*(\w+)\s*\)",
                "(SELECT value AS $3 FROM json_each(_jamsil_series_json($1))) AS $2",
            ),
            r(r"(?i)\b(\w+)\.(confkey|conkey)\[\s*(\w+\.\w+)\s*\]", "_jamsil_arr_get($1.$2, $3)"),
        ]
    })
}

/// 키워드 목록 등 상수 결과로 대체하는 질의.
fn replace_whole(sql: &str) -> Option<&'static str> {
    let l = sql.to_ascii_lowercase();
    if l.contains("pg_get_keywords()") {
        return Some("SELECT 'limit,offset,returning,ilike,text_match,text_score' AS string_agg");
    }
    None
}

pub fn rewrite(sql: &str) -> Cow<'_, str> {
    if let Some(r) = replace_whole(sql) {
        return Cow::Borrowed(r);
    }
    let l = sql.as_bytes();
    // 빠른 경로: 대상 구문이 없으면 그대로
    let hay = std::str::from_utf8(l).unwrap_or("");
    if !(hay.contains("_pg_expandarray")
        || hay.contains("pg_get_indexdef")
        || hay.contains("generate_series")
        || hay.contains("::smallint")
        || hay.contains("keys).x"))
    {
        return Cow::Borrowed(sql);
    }
    let mut s = sql.to_string();
    for r in rules() {
        if r.re.is_match(&s) {
            s = r.re.replace_all(&s, r.rep).into_owned();
        }
    }
    Cow::Owned(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_keys_query() {
        let q = "SELECT result.TABLE_CAT FROM (SELECT NULL AS TABLE_CAT, (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS PK_NAME, information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM FROM pg_catalog.pg_class ct WHERE true AND i.indisprimary ) result where  result.A_ATTNUM = (result.KEYS).x  ORDER BY result.key_seq";
        let r = rewrite(q);
        assert!(!r.contains("_pg_expandarray"), "{r}");
        assert!(r.contains("result.KEYS IS NOT NULL"), "{r}");
        crate::classify::classify(&r).unwrap();
    }

    #[test]
    fn fk_query() {
        let q = "SELECT pos.n AS KEY_SEQ FROM pg_catalog.pg_constraint con, pg_catalog.generate_series(1, 32) pos(n) WHERE pka.attnum = con.confkey[pos.n] AND fka.attnum = con.conkey[pos.n]";
        let r = rewrite(q);
        assert!(
            r.contains("(SELECT value AS n FROM json_each(_jamsil_series_json(1, 32))) AS pos"),
            "{r}"
        );
        let q2 =
            "SELECT s.r FROM generate_series(1, array_upper(current_schemas(false), 1)) AS s (r)";
        let r2 = rewrite(q2);
        assert!(
            r2.contains(
                "json_each(_jamsil_series_json(1, array_upper(current_schemas(false), 1)))) AS s"
            ),
            "{r2}"
        );
        crate::classify::classify(&r2).unwrap();
        assert!(r.contains("_jamsil_arr_get(con.confkey, pos.n)"), "{r}");
        crate::classify::classify(&r).unwrap();
    }
}
