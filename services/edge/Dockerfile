# IsomerDB edge replica. Build from the repository root:
#   docker build -f services/edge/Dockerfile -t isomer-edge:dev .
FROM rust:1.89-bookworm AS build
WORKDIR /src
COPY services/edge/Cargo.toml services/edge/Cargo.lock* ./
COPY services/edge/src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release && cp target/release/isomer-edge /isomer-edge

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /isomer-edge /usr/local/bin/isomer-edge
COPY LICENSE NOTICE /usr/share/doc/isomerdb/
COPY licenses/third-party/isomer-edge.txt /usr/share/doc/isomerdb/THIRD_PARTY_LICENSES
ENTRYPOINT ["/usr/local/bin/isomer-edge"]
