# syntax=docker/dockerfile:1
# Build from the repo root:  docker build -f services/embedder/Dockerfile -t isomer-embedder:dev .
FROM rust:1.89-bookworm AS build
WORKDIR /src
COPY services/embedder/Cargo.toml services/embedder/Cargo.lock* ./
COPY services/embedder/src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo test --release && cargo build --release && cp target/release/isomer-embedder /isomer-embedder

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /isomer-embedder /usr/local/bin/isomer-embedder
ENTRYPOINT ["isomer-embedder"]
