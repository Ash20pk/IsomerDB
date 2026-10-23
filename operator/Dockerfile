# syntax=docker/dockerfile:1
# Build:  docker build -t isomerdb-operator:dev operator
FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY api ./api
COPY internal ./internal
COPY cmd ./cmd
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /operator ./cmd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /operator /operator
COPY LICENSE /usr/share/doc/isomerdb/LICENSE
COPY THIRD_PARTY_LICENSES /usr/share/doc/isomerdb/THIRD_PARTY_LICENSES
USER 65532:65532
ENTRYPOINT ["/operator"]
