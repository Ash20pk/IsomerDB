# syntax=docker/dockerfile:1
# IsomerDB base image: PostgreSQL 18 + Citus + DocumentDB (+ gateway) + pg_search + pgvector +
# pg_trickle. Citus, DocumentDB and pg_trickle are built from vendor/; the rest are pinned
# packages. Build from the repo root:
#
#   docker build -f docker/Dockerfile -t isomerdb:dev .

ARG PG_MAJOR=18
ARG PG_IMAGE=postgres:18.6-bookworm

# ── Shared build toolchain ────────────────────────────────────────────────────
FROM ${PG_IMAGE} AS build-base
ARG PG_MAJOR
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential git cmake pkg-config curl wget ca-certificates gnupg lsb-release \
        postgresql-server-dev-${PG_MAJOR} \
        libicu-dev libkrb5-dev libssl-dev libcurl4-openssl-dev liblz4-dev libzstd-dev \
        libselinux1-dev libpam0g-dev libxslt1-dev libreadline-dev zlib1g-dev \
        libclang-dev clang python3 \
    && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

# ── Citus (vendor/citus) ──────────────────────────────────────────────────────
FROM build-base AS citus
COPY vendor/citus /src/citus
WORKDIR /src/citus
RUN ./configure && make -j"$(nproc)" && make install && make install DESTDIR=/out

# ── DocumentDB extensions (vendor/documentdb) ─────────────────────────────────
# Built on top of the Citus stage so pg_documentdb_distributed finds the Citus headers.
FROM citus AS documentdb
COPY vendor/documentdb/scripts /tmp/install_setup
RUN export INSTALL_DEPENDENCIES_ROOT=/tmp/install_setup CLEAN_SETUP=1 && \
    MAKE_PROGRAM=cmake /tmp/install_setup/install_setup_libbson.sh && \
    /tmp/install_setup/install_setup_pcre2.sh && \
    /tmp/install_setup/install_setup_intel_decimal_math_lib.sh
COPY vendor/documentdb /src/documentdb
WORKDIR /src/documentdb
RUN make -j"$(nproc)" && make install DESTDIR=/out

# ── DocumentDB gateway (MongoDB wire protocol, Rust) ──────────────────────────
FROM build-base AS gateway
COPY vendor/documentdb /src/documentdb
WORKDIR /src/documentdb/pg_documentdb_gw
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo build --release && \
    install -D target/release/documentdb_gateway /out/usr/local/bin/documentdb_gateway

# ── pg_trickle (vendor/pg_trickle) ────────────────────────────────────────────
FROM build-base AS pg_trickle
ARG PG_MAJOR
RUN cargo install --locked cargo-pgrx --version 0.18.0 && \
    cargo pgrx init --pg${PG_MAJOR} /usr/bin/pg_config
COPY vendor/pg_trickle /src/pg_trickle
WORKDIR /src/pg_trickle
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo pgrx package --pg-config /usr/bin/pg_config && \
    mkdir -p /out && cp -r target/release/pg_trickle-pg${PG_MAJOR}/* /out/ && \
    cp sql/pg_trickle--*--*.sql /out/usr/share/postgresql/${PG_MAJOR}/extension/

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM ${PG_IMAGE}
ARG PG_MAJOR
ARG TARGETARCH
ARG PG_SEARCH_VERSION=0.26.1

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates libcurl4 liblz4-1 libzstd1 \
        postgresql-${PG_MAJOR}-cron \
        postgresql-${PG_MAJOR}-pgvector \
        postgresql-${PG_MAJOR}-postgis-3 \
    && curl -fsSL -o /tmp/pg_search.deb \
        "https://github.com/paradedb/paradedb/releases/download/v${PG_SEARCH_VERSION}/postgresql-${PG_MAJOR}-pg-search_${PG_SEARCH_VERSION}-1PARADEDB-bookworm_${TARGETARCH}.deb" \
    && apt-get install -y /tmp/pg_search.deb \
    && rm -f /tmp/pg_search.deb && rm -rf /var/lib/apt/lists/*

COPY --from=citus /out/ /
COPY --from=documentdb /out/ /
COPY --from=pg_trickle /out/ /
COPY --from=gateway /out/ /

COPY docker/postgresql.isomer.conf /usr/share/postgresql/isomer.conf
RUN printf "\ninclude = '/usr/share/postgresql/isomer.conf'\n" >> /usr/share/postgresql/postgresql.conf.sample
COPY docker/initdb/ /docker-entrypoint-initdb.d/
COPY docker/gateway.json /etc/isomerdb/gateway.json
COPY vendor/citus/LICENSE /usr/share/doc/isomerdb/licenses/citus.LICENSE
COPY vendor/documentdb/LICENSE /usr/share/doc/isomerdb/licenses/documentdb.LICENSE
COPY vendor/pg_trickle/LICENSE /usr/share/doc/isomerdb/licenses/pg_trickle.LICENSE

LABEL org.opencontainers.image.title="isomerdb" \
      org.opencontainers.image.description="IsomerDB base: PostgreSQL 18 + Citus + DocumentDB + pg_search + pgvector + pg_trickle"

EXPOSE 5432 10260
