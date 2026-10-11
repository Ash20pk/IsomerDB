//! Embedding providers: any OpenAI-compatible endpoint, a model run on the embedder's own CPU
//! (`local`), and a deterministic test embedder (`local-hash`).

use std::fmt;
use std::sync::OnceLock;
use std::time::Duration;

use serde::Deserialize;

pub struct Model {
    pub name: String,
    pub provider: String,
    pub endpoint: Option<String>,
    pub model: String,
    pub dimensions: usize,
    pub api_key_env: Option<String>,
    pub max_batch_tokens: i64,
    pub max_batch_items: i64,
    pub requests_per_min: f64,
    pub tokens_per_min: f64,
}

pub struct EmbedResult {
    pub vectors: Vec<Vec<f32>>,
    /// Tokens the provider billed, when it reports them.
    pub tokens: Option<i64>,
}

#[derive(Debug)]
pub enum EmbedError {
    /// 429: retry after the provider's Retry-After, if it gave one.
    RateLimited { retry_after: Option<f64>, message: String },
    /// Network errors, timeouts, 5xx: retry with backoff.
    Transient(String),
    /// Bad request, auth, wrong dimensions: retrying will not help.
    Permanent(String),
}

impl fmt::Display for EmbedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EmbedError::RateLimited { message, .. } => write!(f, "rate limited: {message}"),
            EmbedError::Transient(m) => write!(f, "transient: {m}"),
            EmbedError::Permanent(m) => write!(f, "permanent: {m}"),
        }
    }
}

pub async fn embed(http: &reqwest::Client, model: &Model, inputs: &[&str]) -> Result<EmbedResult, EmbedError> {
    let result = match model.provider.as_str() {
        "openai" => openai(http, model, inputs).await?,
        "local" => local(model, inputs).await?,
        "local-hash" => EmbedResult {
            vectors: inputs.iter().map(|i| local_hash(i, model.dimensions)).collect(),
            tokens: None,
        },
        other => return Err(EmbedError::Permanent(format!("unknown provider {other}"))),
    };
    if result.vectors.len() != inputs.len() {
        return Err(EmbedError::Transient(format!(
            "provider returned {} embeddings for {} inputs",
            result.vectors.len(),
            inputs.len()
        )));
    }
    if let Some(v) = result.vectors.iter().find(|v| v.len() != model.dimensions) {
        return Err(EmbedError::Permanent(format!(
            "model {} returned {} dimensions, but it is registered with {}",
            model.name,
            v.len(),
            model.dimensions
        )));
    }
    Ok(result)
}

fn local_models() -> &'static crate::local::LocalModels {
    static MODELS: OnceLock<crate::local::LocalModels> = OnceLock::new();
    MODELS.get_or_init(Default::default)
}

/// Run the model on a blocking thread: inference is CPU-bound and would stall the async workers.
async fn local(model: &Model, inputs: &[&str]) -> Result<EmbedResult, EmbedError> {
    let dir = crate::local::model_dir(&model.model);
    let owned: Vec<String> = inputs.iter().map(|s| (*s).to_owned()).collect();
    let (name, dims) = (model.name.clone(), model.dimensions);
    let (vectors, tokens) = tokio::task::spawn_blocking(move || {
        let m = local_models().get(&dir).map_err(|e| EmbedError::Permanent(format!("{e:#}")))?;
        if m.dimensions != dims {
            return Err(EmbedError::Permanent(format!(
                "model {name} is registered with {dims} dimensions, but {} produces {}",
                dir.display(),
                m.dimensions
            )));
        }
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
        m.embed(&refs).map_err(|e| EmbedError::Transient(format!("{e:#}")))
    })
    .await
    .map_err(|e| EmbedError::Transient(format!("local model task: {e}")))??;
    Ok(EmbedResult { vectors, tokens: Some(tokens) })
}

#[derive(Deserialize)]
struct OpenAiResponse {
    data: Vec<OpenAiItem>,
    usage: Option<OpenAiUsage>,
}

#[derive(Deserialize)]
struct OpenAiItem {
    index: usize,
    embedding: Vec<f32>,
}

#[derive(Deserialize)]
struct OpenAiUsage {
    total_tokens: Option<i64>,
    prompt_tokens: Option<i64>,
}

async fn openai(http: &reqwest::Client, model: &Model, inputs: &[&str]) -> Result<EmbedResult, EmbedError> {
    let endpoint = model.endpoint.as_deref().unwrap_or("https://api.openai.com/v1").trim_end_matches('/');
    let mut request = http
        .post(format!("{endpoint}/embeddings"))
        .json(&serde_json::json!({ "model": model.model, "input": inputs }));
    if let Some(var) = &model.api_key_env {
        let key = std::env::var(var).map_err(|_| {
            EmbedError::Permanent(format!("environment variable {var} (the API key for model {}) is not set on the embedder", model.name))
        })?;
        request = request.bearer_auth(key);
    }
    let response = request.send().await.map_err(|e| EmbedError::Transient(e.to_string()))?;
    let status = response.status();
    if status.as_u16() == 429 {
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<f64>().ok());
        let body = response.text().await.unwrap_or_default();
        return Err(EmbedError::RateLimited { retry_after, message: truncate(&body) });
    }
    if status.is_server_error() {
        return Err(EmbedError::Transient(format!("{status}: {}", truncate(&response.text().await.unwrap_or_default()))));
    }
    if !status.is_success() {
        return Err(EmbedError::Permanent(format!("{status}: {}", truncate(&response.text().await.unwrap_or_default()))));
    }
    let mut parsed: OpenAiResponse = response
        .json()
        .await
        .map_err(|e| EmbedError::Transient(format!("unreadable response: {e}")))?;
    parsed.data.sort_by_key(|d| d.index);
    Ok(EmbedResult {
        vectors: parsed.data.into_iter().map(|d| d.embedding).collect(),
        tokens: parsed.usage.and_then(|u| u.total_tokens.or(u.prompt_tokens)),
    })
}

fn truncate(s: &str) -> String {
    s.chars().take(300).collect()
}

/// Deterministic, network-free embedding: words feature-hashed into `dims` buckets with a sign,
/// L2-normalised. Similar wording gives similar vectors; there is no semantic understanding.
/// For tests, development and air-gapped smoke checks only.
pub fn local_hash(input: &str, dims: usize) -> Vec<f32> {
    let mut v = vec![0f32; dims];
    for word in input
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(|w| w.to_lowercase())
    {
        let h = fnv1a(word.as_bytes());
        let idx = (h % dims as u64) as usize;
        v[idx] += if h >> 63 == 0 { 1.0 } else { -1.0 };
    }
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm == 0.0 {
        v[0] = 1.0;
    } else {
        v.iter_mut().for_each(|x| *x /= norm);
    }
    v
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// pgvector's text input format.
pub fn to_pgvector(v: &[f32]) -> String {
    let mut s = String::with_capacity(v.len() * 10 + 2);
    s.push('[');
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("{x}"));
    }
    s.push(']');
    s
}

#[allow(dead_code)]
pub fn backoff(attempt: u32) -> Duration {
    Duration::from_secs_f64((2f64.powi(attempt as i32)).min(300.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_hash_is_deterministic_and_normalised() {
        let a = local_hash("Quiet mechanical keyboard", 64);
        assert_eq!(a, local_hash("quiet  MECHANICAL keyboard!", 64));
        let norm: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-5);
    }

    #[test]
    fn similar_text_scores_closer() {
        let cos = |a: &[f32], b: &[f32]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
        let q = local_hash("mechanical keyboard", 64);
        let near = local_hash("quiet mechanical keyboard", 64);
        let far = local_hash("wireless gaming mouse", 64);
        assert!(cos(&q, &near) > cos(&q, &far));
    }

    #[test]
    fn pgvector_format() {
        assert_eq!(to_pgvector(&[1.0, -0.5]), "[1,-0.5]");
    }
}
