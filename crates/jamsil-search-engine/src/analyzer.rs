//! 분석기: NFKC 정규화 + 토크나이저 + 소문자화 + 토큰 길이/개수 상한.

use tantivy::tokenizer::{
    Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, TextAnalyzer, Token,
    TokenFilter, TokenStream, Tokenizer,
};
use unicode_normalization::UnicodeNormalization;

pub const KOREAN: &str = "jamsil_korean";
pub const ENGLISH: &str = "jamsil_english";
pub const STANDARD: &str = "jamsil_standard";

pub fn tokenizer_name(analyzer: &str) -> &'static str {
    match analyzer {
        "korean" => KOREAN,
        "english" => ENGLISH,
        _ => STANDARD,
    }
}

/// 검색·색인 전 텍스트 정규화 (원문 저장은 무손실, 색인만 정규화 — 설계서 9.4).
pub fn normalize(s: &str) -> String {
    s.nfkc().collect()
}

/// 문서당 최대 토큰 수 제한.
#[derive(Clone)]
pub struct LimitTokens(pub usize);

impl TokenFilter for LimitTokens {
    type Tokenizer<T: Tokenizer> = LimitTokensWrapper<T>;
    fn transform<T: Tokenizer>(self, tokenizer: T) -> LimitTokensWrapper<T> {
        LimitTokensWrapper {
            limit: self.0,
            inner: tokenizer,
        }
    }
}

#[derive(Clone)]
pub struct LimitTokensWrapper<T: Tokenizer> {
    limit: usize,
    inner: T,
}

impl<T: Tokenizer> Tokenizer for LimitTokensWrapper<T> {
    type TokenStream<'a> = LimitTokensStream<T::TokenStream<'a>>;
    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        LimitTokensStream {
            left: self.limit,
            tail: self.inner.token_stream(text),
        }
    }
}

pub struct LimitTokensStream<T> {
    left: usize,
    tail: T,
}

impl<T: TokenStream> TokenStream for LimitTokensStream<T> {
    fn advance(&mut self) -> bool {
        if self.left == 0 {
            return false;
        }
        self.left -= 1;
        self.tail.advance()
    }
    fn token(&self) -> &Token {
        self.tail.token()
    }
    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

pub struct Analyzers {
    pub korean: TextAnalyzer,
    pub english: TextAnalyzer,
    pub standard: TextAnalyzer,
}

impl Analyzers {
    pub fn new(max_tokens: usize, max_token_bytes: usize) -> Result<Analyzers, String> {
        let korean = {
            use lindera::dictionary::{load_dictionary_from_kind, DictionaryKind};
            use lindera::mode::Mode;
            use lindera::segmenter::Segmenter;
            let dict = load_dictionary_from_kind(DictionaryKind::KoDic)
                .map_err(|e| format!("ko-dic: {e}"))?;
            let seg = Segmenter::new(Mode::Normal, dict, None);
            TextAnalyzer::builder(lindera_tantivy::tokenizer::LinderaTokenizer::from_segmenter(seg))
                .filter(RemoveLongFilter::limit(max_token_bytes))
                .filter(LowerCaser)
                .filter(LimitTokens(max_tokens))
                .build()
        };
        let english = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(max_token_bytes))
            .filter(LowerCaser)
            .filter(Stemmer::new(Language::English))
            .filter(LimitTokens(max_tokens))
            .build();
        let standard = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(max_token_bytes))
            .filter(LowerCaser)
            .filter(LimitTokens(max_tokens))
            .build();
        Ok(Analyzers {
            korean,
            english,
            standard,
        })
    }

    pub fn register(&self, index: &tantivy::Index) {
        index.tokenizers().register(KOREAN, self.korean.clone());
        index.tokenizers().register(ENGLISH, self.english.clone());
        index.tokenizers().register(STANDARD, self.standard.clone());
    }
}
