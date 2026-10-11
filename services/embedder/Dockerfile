# syntax=docker/dockerfile:1
# Build from the repo root:  docker build -f services/embedder/Dockerfile -t isomer-embedder:dev .
FROM rust:1.95-bookworm AS build
WORKDIR /src
COPY services/embedder/Cargo.toml services/embedder/Cargo.lock* services/embedder/rust-toolchain.toml ./
COPY services/embedder/src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo test --release && cargo build --release && cp target/release/isomer-embedder /isomer-embedder

# The in-cluster CPU model (provider 'local'): BAAI/bge-small-en-v1.5, MIT, pinned and checksummed,
# so the image embeds with no network access.
FROM debian:bookworm-slim AS model
ARG BGE_REVISION=5c38ec7c405ec4b44b94cc5a9bb96e735b38267a
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
WORKDIR /model
RUN for f in config.json tokenizer.json model.safetensors; do \
      curl -fsSL -o "$f" "https://huggingface.co/BAAI/bge-small-en-v1.5/resolve/${BGE_REVISION}/$f"; \
    done && printf '%s\n' \
      '094f8e891b932f2000c92cfc663bac4c62069f5d8af5b5278c4306aef3084750  config.json' \
      'd241a60d5e8f04cc1b2b3e9ef7a4921b27bf526d9f6050ab90f9267a1f9e5c66  tokenizer.json' \
      '3c9f31665447c8911517620762200d2245a2518d6e7208acc78cd9db317e21ad  model.safetensors' \
    | sha256sum -c -
COPY services/embedder/models/bge-small-en-v1.5.LICENSE /model/LICENSE

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /isomer-embedder /usr/local/bin/isomer-embedder
COPY --from=model /model /usr/share/isomer/models/bge-small-en-v1.5
ENTRYPOINT ["isomer-embedder"]
