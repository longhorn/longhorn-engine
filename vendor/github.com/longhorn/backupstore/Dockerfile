# syntax=docker/dockerfile:1.22.0@sha256:4a43a54dd1fedceb30ba47e76cfcf2b47304f4161c0caeac2db1c61804ea3c91

FROM golangci/golangci-lint:v2.12.2@sha256:5cceeef04e53efe1470638d4b4b4f5ceefd574955ab3941b2d9a68a8c9ad5240 AS golangci-lint
FROM registry.suse.com/bci/golang:1.26@sha256:09c1ac7463e47fefceaa013c07828af079b5c3e40fc36be6a69ddfbb63e31c91 AS base

ARG TARGETARCH
ARG http_proxy
ARG https_proxy

ENV ARCH=${TARGETARCH}
ENV GOFLAGS=-mod=vendor

RUN zypper -n install gcc ca-certificates git wget curl vim less file nfs-client docker awk e2fsprogs && \
    rm -rf /var/cache/zypp/*

# Copy golangci-lint binary from official image
COPY --from=golangci-lint /usr/bin/golangci-lint /usr/local/bin/golangci-lint

WORKDIR /go/src/github.com/longhorn/backupstore
COPY . .

FROM base AS validate
RUN ./scripts/validate && touch /validate.done

FROM scratch AS validate-artifacts
COPY --from=validate /validate.done /validate.done
