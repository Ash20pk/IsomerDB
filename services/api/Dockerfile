# syntax=docker/dockerfile:1
# Build from the repo root:  docker build -f services/api/Dockerfile -t isomer-api:dev .
FROM rust:1.89-bookworm AS build
WORKDIR /src
COPY services/api/Cargo.toml services/api/Cargo.lock* ./
COPY services/api/src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release && cp target/release/isomer-api /isomer-api

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /isomer-api /usr/local/bin/isomer-api
ENTRYPOINT ["isomer-api"]
EXPOSE 8080
