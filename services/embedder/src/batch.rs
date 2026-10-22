//! Provider batch APIs for large backfills (PRD FR-V1a): OpenAI's `/v1/files` and `/v1/batches`,
//! or any endpoint that speaks them. A batch is one JSONL file of embedding requests, processed
//! by the provider within 24 hours at a discount and outside the per-minute rate limits. Each
//! line's `custom_id` is the item's number within the batch, which the database maps back to its
//! job (`isomer.embed_batch_lease`).

use serde::Deserialize;
use serde_json::{json, Value};

use crate::providers::{truncate, EmbedError, Model};

pub struct Submitted {
    pub batch_id: String,
    pub input_file_id: String,
}

pub struct Status {
    pub status: String,
    pub output_file_id: Option<String>,
    pub error_file_id: Option<String>,
    pub error: Option<String>,
}

/// One line of a finished batch's output (or error) file.
pub struct ItemResult {
    pub item: i32,
    pub embedding: Option<Vec<f32>>,
    pub error: Option<String>,
    pub tokens: i64,
}

fn endpoint(model: &Model) -> &str {
    model.endpoint.as_deref().unwrap_or("https://api.openai.com/v1").trim_end_matches('/')
}

fn authorised(request: reqwest::RequestBuilder, model: &Model) -> Result<reqwest::RequestBuilder, EmbedError> {
    match &model.api_key_env {
        Some(var) => {
            let key = std::env::var(var).map_err(|_| {
                EmbedError::Permanent(format!("environment variable {var} (the API key for model {}) is not set on the embedder", model.name))
            })?;
            Ok(request.bearer_auth(key))
        }
        None => Ok(request),
    }
}

async fn checked(response: Result<reqwest::Response, reqwest::Error>) -> Result<reqwest::Response, EmbedError> {
    let response = response.map_err(|e| EmbedError::Transient(e.to_string()))?;
    let status = response.status();
    if status.as_u16() == 429 || status.is_server_error() {
        return Err(EmbedError::Transient(format!("{status}: {}", truncate(&response.text().await.unwrap_or_default()))));
    }
    if !status.is_success() {
        return Err(EmbedError::Permanent(format!("{status}: {}", truncate(&response.text().await.unwrap_or_default()))));
    }
    Ok(response)
}

/// The JSONL a batch is made of: one embeddings request per item.
pub fn jsonl(model: &Model, items: &[(i32, String)]) -> String {
    let mut out = String::new();
    for (item, input) in items {
        out.push_str(
            &json!({
                "custom_id": item.to_string(),
                "method": "POST",
                "url": "/v1/embeddings",
                "body": { "model": model.model, "input": input },
            })
            .to_string(),
        );
        out.push('\n');
    }
    out
}

#[derive(Deserialize)]
struct Created {
    id: String,
}

/// Upload the items and start a batch over them.
pub async fn submit(http: &reqwest::Client, model: &Model, items: &[(i32, String)]) -> Result<Submitted, EmbedError> {
    let file = reqwest::multipart::Part::bytes(jsonl(model, items).into_bytes())
        .file_name("isomer-backfill.jsonl")
        .mime_str("application/jsonl")
        .map_err(|e| EmbedError::Permanent(e.to_string()))?;
    let form = reqwest::multipart::Form::new().text("purpose", "batch").part("file", file);
    let uploaded: Created = checked(authorised(http.post(format!("{}/files", endpoint(model))).multipart(form), model)?.send().await)
        .await?
        .json()
        .await
        .map_err(|e| EmbedError::Transient(format!("unreadable file upload response: {e}")))?;
    let batch: Created = checked(
        authorised(http.post(format!("{}/batches", endpoint(model))), model)?
            .json(&json!({ "input_file_id": uploaded.id, "endpoint": "/v1/embeddings", "completion_window": "24h" }))
            .send()
            .await,
    )
    .await?
    .json()
    .await
    .map_err(|e| EmbedError::Transient(format!("unreadable batch response: {e}")))?;
    Ok(Submitted { batch_id: batch.id, input_file_id: uploaded.id })
}

pub async fn status(http: &reqwest::Client, model: &Model, batch_id: &str) -> Result<Status, EmbedError> {
    let v: Value = checked(authorised(http.get(format!("{}/batches/{batch_id}", endpoint(model))), model)?.send().await)
        .await?
        .json()
        .await
        .map_err(|e| EmbedError::Transient(format!("unreadable batch status: {e}")))?;
    let text = |k: &str| v.get(k).and_then(Value::as_str).map(str::to_owned);
    Ok(Status {
        status: text("status").unwrap_or_else(|| "unknown".into()),
        output_file_id: text("output_file_id"),
        error_file_id: text("error_file_id"),
        error: v.get("errors").filter(|e| !e.is_null()).map(|e| truncate(&e.to_string())),
    })
}

pub async fn file(http: &reqwest::Client, model: &Model, file_id: &str) -> Result<String, EmbedError> {
    checked(authorised(http.get(format!("{}/files/{file_id}/content", endpoint(model))), model)?.send().await)
        .await?
        .text()
        .await
        .map_err(|e| EmbedError::Transient(format!("unreadable file: {e}")))
}

/// Parse an output or error file. Lines that aren't ours are skipped.
pub fn parse(text: &str) -> Vec<ItemResult> {
    let mut out = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let Ok(v) = serde_json::from_str::<Value>(line) else { continue };
        let Some(item) = v.get("custom_id").and_then(Value::as_str).and_then(|s| s.parse::<i32>().ok()) else { continue };
        let response = v.get("response");
        let status = response.and_then(|r| r.get("status_code")).and_then(Value::as_u64).unwrap_or(0);
        let body = response.and_then(|r| r.get("body"));
        let embedding = (status == 200)
            .then(|| body.and_then(|b| b.pointer("/data/0/embedding")).and_then(Value::as_array))
            .flatten()
            .map(|a| a.iter().filter_map(Value::as_f64).map(|x| x as f32).collect::<Vec<f32>>());
        let tokens = body
            .and_then(|b| b.pointer("/usage/total_tokens").or_else(|| b.pointer("/usage/prompt_tokens")))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let error = if embedding.is_some() {
            None
        } else {
            Some(truncate(
                &v.get("error")
                    .filter(|e| !e.is_null())
                    .or_else(|| body.and_then(|b| b.get("error")))
                    .map(Value::to_string)
                    .unwrap_or_else(|| format!("status {status}")),
            ))
        };
        out.push(ItemResult { item, embedding, error, tokens });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model() -> Model {
        Model {
            name: "m".into(),
            provider: "openai".into(),
            endpoint: Some("http://x/v1".into()),
            model: "text-embedding-3-small".into(),
            dimensions: 2,
            api_key_env: None,
            max_batch_tokens: 8000,
            max_batch_items: 96,
            requests_per_min: 1.0,
            tokens_per_min: 1.0,
            batch_api: true,
            batch_min_items: 1000,
        }
    }

    #[test]
    fn jsonl_has_one_request_per_item() {
        let s = jsonl(&model(), &[(1, "a".into()), (2, "b \"quoted\"".into())]);
        let lines: Vec<Value> = s.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1]["custom_id"], "2");
        assert_eq!(lines[1]["body"]["input"], "b \"quoted\"");
        assert_eq!(lines[0]["url"], "/v1/embeddings");
    }

    #[test]
    fn parse_reads_results_and_errors() {
        let out = parse(concat!(
            r#"{"custom_id":"1","response":{"status_code":200,"body":{"data":[{"embedding":[0.5,-0.5]}],"usage":{"total_tokens":3}}},"error":null}"#,
            "\n",
            r#"{"custom_id":"2","response":{"status_code":400,"body":{"error":{"message":"bad input"}}},"error":null}"#,
            "\n",
            r#"{"custom_id":"x","response":null}"#
        ));
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].embedding.as_deref(), Some(&[0.5f32, -0.5][..]));
        assert_eq!(out[0].tokens, 3);
        assert!(out[1].embedding.is_none());
        assert!(out[1].error.as_deref().unwrap().contains("bad input"));
    }
}
