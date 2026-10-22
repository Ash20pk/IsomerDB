# The IsomerDB cloud control plane. Build from the repository root (it compiles against the
# operator's API types):
#   docker build -f controlplane/Dockerfile -t isomerdb-controlplane:dev .
FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY operator/go.mod operator/go.sum operator/
COPY controlplane/go.mod controlplane/go.sum controlplane/
RUN cd controlplane && go mod download
COPY operator/api operator/api
COPY controlplane/*.go controlplane/
COPY controlplane/web controlplane/web
RUN cd controlplane && CGO_ENABLED=0 go build -trimpath -o /controlplane .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /controlplane /controlplane
COPY LICENSE /usr/share/doc/isomerdb/LICENSE
ENTRYPOINT ["/controlplane"]
