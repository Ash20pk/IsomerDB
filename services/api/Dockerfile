# syntax=docker/dockerfile:1
# Build from the repo root:  docker build -f services/api/Dockerfile -t isomer-api:dev .
FROM node:22-bookworm-slim AS console
WORKDIR /src/clients
COPY clients/package.json clients/package-lock.json clients/tsconfig.base.json ./
COPY clients/sdk ./sdk
COPY clients/cli/package.json ./cli/package.json
COPY clients/console ./console
RUN npm ci --ignore-scripts && npm run build -w @isomerdb/console

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
COPY --from=console /src/clients/console/dist /usr/share/isomer/console
COPY LICENSE NOTICE /usr/share/doc/isomerdb/
COPY clients/LICENSE /usr/share/isomer/console/LICENSE
ENTRYPOINT ["isomer-api"]
EXPOSE 8080
