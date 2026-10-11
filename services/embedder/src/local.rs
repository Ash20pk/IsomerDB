//! In-cluster CPU embeddings (PRD FR-V1b): a small BERT sentence-embedding model run on the
//! embedder's own CPU, for the free tier, development and air-gapped clusters. No API key, no
//! network, no per-token cost.
//!
//! The model directory holds `config.json`, `tokenizer.json` and `model.safetensors` in the
//! Hugging Face layout. The image ships BAAI/bge-small-en-v1.5 (MIT, 384 dimensions) under
//! `/usr/share/isomer/models/<name>`; `ISOMER_MODEL_DIR` points elsewhere. BGE embeddings are the
//! [CLS] token's final hidden state, L2-normalised.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context, Result};
use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config};
use tokenizers::{PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};

/// Longest input in tokens: longer text is truncated, as BERT's position table ends here.
const MAX_TOKENS: usize = 512;

pub struct LocalModel {
    model: BertModel,
    tokenizer: Tokenizer,
    pub dimensions: usize,
}

impl LocalModel {
    pub fn load(dir: &Path) -> Result<Self> {
        let config: Config = serde_json::from_str(
            &std::fs::read_to_string(dir.join("config.json")).with_context(|| format!("{}/config.json", dir.display()))?,
        )?;
        let mut tokenizer = Tokenizer::from_file(dir.join("tokenizer.json")).map_err(|e| anyhow!("{}/tokenizer.json: {e}", dir.display()))?;
        tokenizer.with_padding(Some(PaddingParams { strategy: PaddingStrategy::BatchLongest, ..Default::default() }));
        tokenizer
            .with_truncation(Some(TruncationParams { max_length: MAX_TOKENS, ..Default::default() }))
            .map_err(|e| anyhow!("tokenizer truncation: {e}"))?;
        let weights = dir.join("model.safetensors");
        // SAFETY: the weights file is memory-mapped read-only and not modified while mapped.
        let vb = unsafe { VarBuilder::from_mmaped_safetensors(&[&weights], DType::F32, &Device::Cpu)? };
        let dimensions = config.hidden_size;
        let model = BertModel::load(vb, &config).with_context(|| format!("loading {}", weights.display()))?;
        Ok(LocalModel { model, tokenizer, dimensions })
    }

    /// Embed a batch. Returns the vectors and the number of tokens processed.
    pub fn embed(&self, inputs: &[&str]) -> Result<(Vec<Vec<f32>>, i64)> {
        if inputs.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let encodings = self
            .tokenizer
            .encode_batch(inputs.to_vec(), true)
            .map_err(|e| anyhow!("tokenizing: {e}"))?;
        let tokens: i64 = encodings.iter().map(|e| e.get_attention_mask().iter().filter(|m| **m == 1).count() as i64).sum();
        let ids = encodings.iter().map(|e| Tensor::new(e.get_ids(), &Device::Cpu)).collect::<candle_core::Result<Vec<_>>>()?;
        let mask = encodings
            .iter()
            .map(|e| Tensor::new(e.get_attention_mask(), &Device::Cpu))
            .collect::<candle_core::Result<Vec<_>>>()?;
        let ids = Tensor::stack(&ids, 0)?;
        let mask = Tensor::stack(&mask, 0)?;
        let type_ids = ids.zeros_like()?;
        let hidden = self.model.forward(&ids, &type_ids, Some(&mask))?; // (batch, seq, hidden)
        let cls = hidden.narrow(1, 0, 1)?.squeeze(1)?; // (batch, hidden)
        let norm = cls.sqr()?.sum_keepdim(1)?.sqrt()?;
        let cls = cls.broadcast_div(&norm)?;
        Ok((cls.to_vec2::<f32>()?, tokens))
    }
}

/// Loaded models, by directory. Loading takes a second or two and ~130 MB, so it happens once.
#[derive(Default)]
pub struct LocalModels {
    loaded: Mutex<HashMap<PathBuf, Arc<LocalModel>>>,
}

impl LocalModels {
    pub fn get(&self, dir: &Path) -> Result<Arc<LocalModel>> {
        let mut loaded = self.loaded.lock().map_err(|_| anyhow!("local model cache poisoned"))?;
        if let Some(m) = loaded.get(dir) {
            return Ok(m.clone());
        }
        let m = Arc::new(LocalModel::load(dir)?);
        loaded.insert(dir.to_path_buf(), m.clone());
        Ok(m)
    }
}

/// Where a local model's files are: `ISOMER_MODEL_DIR/<model>` when set, else the image's copy.
pub fn model_dir(model: &str) -> PathBuf {
    let root = std::env::var("ISOMER_MODEL_DIR").unwrap_or_else(|_| "/usr/share/isomer/models".into());
    Path::new(&root).join(model)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Needs the model files: ISOMER_MODEL_DIR=<dir holding bge-small-en-v1.5/> cargo test -- --ignored
    #[test]
    #[ignore]
    fn bge_small_ranks_meaning_not_spelling() {
        let model = LocalModel::load(&model_dir("bge-small-en-v1.5")).expect("model files");
        assert_eq!(model.dimensions, 384);
        let (v, tokens) = model
            .embed(&[
                "a quiet keyboard for typing at night",
                "silent mechanical switches for late-night writing",
                "a cast iron pan for searing steak",
            ])
            .unwrap();
        assert!(tokens > 10);
        let cos = |a: &[f32], b: &[f32]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
        for x in &v {
            assert!((cos(x, x) - 1.0).abs() < 1e-4, "normalised");
        }
        // No shared content words between the first two, yet they are the closest pair.
        assert!(cos(&v[0], &v[1]) > cos(&v[0], &v[2]) + 0.1, "{} vs {}", cos(&v[0], &v[1]), cos(&v[0], &v[2]));
        // Matches the reference implementation (sentence-transformers, CLS pooling), when given:
        // ISOMER_REFERENCE_EMBEDDINGS=<json array of the three vectors above>.
        if let Ok(path) = std::env::var("ISOMER_REFERENCE_EMBEDDINGS") {
            let reference: Vec<Vec<f32>> = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
            for (ours, theirs) in v.iter().zip(&reference) {
                assert!(cos(ours, theirs) > 0.9999, "cosine to reference {}", cos(ours, theirs));
            }
        }
        // Batching does not change a vector.
        let (alone, _) = model.embed(&["a quiet keyboard for typing at night"]).unwrap();
        assert!(cos(&alone[0], &v[0]) > 0.9999);
    }
}
