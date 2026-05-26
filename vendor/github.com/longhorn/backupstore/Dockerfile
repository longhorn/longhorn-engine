# syntax=docker/dockerfile:1.22.0@sha256:4a43a54dd1fedceb30ba47e76cfcf2b47304f4161c0caeac2db1c61804ea3c91
FROM registry.suse.com/bci/golang:1.25@sha256:ddf55c9d81a8255b6de7ca74e0c2aed4e425fa845c936cf62a03a99bfb5deaed AS base

ARG TARGETARCH
ARG http_proxy
ARG https_proxy

ENV GOLANGCI_LINT_VERSION=v2.11.4

ENV ARCH=${TARGETARCH}
ENV GOFLAGS=-mod=vendor

RUN zypper -n install gcc ca-certificates git wget curl vim less file nfs-client docker awk e2fsprogs && \
    rm -rf /var/cache/zypp/*

# Install golangci-lint
RUN curl -fsSL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh -o /tmp/install.sh \
    && chmod +x /tmp/install.sh \
    && /tmp/install.sh -b /usr/local/bin ${GOLANGCI_LINT_VERSION}

WORKDIR /go/src/github.com/longhorn/backupstore
COPY . .

FROM base AS validate
RUN ./scripts/validate && touch /validate.done

FROM scratch AS validate-artifacts
COPY --from=validate /validate.done /validate.done
