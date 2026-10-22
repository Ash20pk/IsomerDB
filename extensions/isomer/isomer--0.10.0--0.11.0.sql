-- IsomerDB 0.11.0: an embedding model that runs on the embedder's own CPU (PRD FR-V1b).
--
--   * provider 'local': a small sentence-embedding model computed by the embedder service, with
--     no API key, no network and no per-token cost. For the free tier, development and
--     air-gapped clusters.
--   * bge-small-en-v1.5 (BAAI, MIT, 384 dimensions) is registered and ships in the embedder image.

\echo Use "ALTER EXTENSION isomer UPDATE TO '0.11.0'" to load this file. \quit

ALTER TABLE isomer.embedding_models DROP CONSTRAINT embedding_models_provider_check;
ALTER TABLE isomer.embedding_models ADD CONSTRAINT embedding_models_provider_check
    CHECK (provider IN ('openai', 'local', 'local-hash'));

-- Batches are CPU-bound rather than rate-limited: small batches keep one projection from holding
-- a worker for long, and the request/token limits only need to stay out of the way.
INSERT INTO isomer.embedding_models (name, provider, model, dimensions, price_per_million,
                                     max_batch_tokens, max_batch_items, requests_per_min, tokens_per_min)
VALUES ('bge-small-en-v1.5', 'local', 'bge-small-en-v1.5', 384, 0, 8192, 32, 1000000, 100000000)
ON CONFLICT (name) DO NOTHING;
