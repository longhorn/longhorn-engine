# syntax=docker/dockerfile:1.22.0
FROM registry.suse.com/bci/golang:1.26 AS base

ARG TARGETARCH
ARG SRC_BRANCH=master
ARG SRC_TAG
ARG http_proxy
ARG https_proxy

ENV ARCH=${TARGETARCH}
ENV GOFLAGS=-mod=vendor
ENV PROTOBUF_VER_PY=4.24.3

ENV LONGHORN_INSTANCE_MANAGER_BRANCH="master"

WORKDIR /go/src/github.com/longhorn/longhorn-engine

RUN for i in {1..10}; do \
        zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
        zypper --gpg-auto-import-keys ref && break || sleep 1; \
    done

RUN if [ ${ARCH} == "amd64" ]; then \
        zypper -n install autoconf libtool libunwind-devel; \
    fi

# TODO: use default python3 if SLE upgrade system python version to python3.11
RUN zypper -n install cmake curl git less file gcc python311 python311-pip python311-devel \
    libkmod-devel libnl3-devel linux-glibc-devel pkg-config psmisc qemu-tools fuse \
    bash-completion librdmacm1 librdmacm-utils libibverbs xsltproc docbook-xsl-stylesheets \
    perl-Config-General libaio-devel glibc-devel-static glibc-devel sg3_utils iptables libltdl7 \
    libdevmapper1_03 iproute2 jq unzip zlib-devel zlib-devel-static \
    rpm-build rdma-core-devel gcc-c++ docker open-iscsi e2fsprogs && \
    rm -rf /var/cache/zypp/*

# Install golangci-lint
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin latest

# Install Minio
ENV MINIO_URL_amd64=https://dl.min.io/server/minio/release/linux-amd64/archive/minio.RELEASE.2021-12-20T22-07-16Z \
    MINIO_URL_arm64=https://dl.min.io/server/minio/release/linux-arm64/archive/minio.RELEASE.2021-12-20T22-07-16Z \
    MINIO_URL=MINIO_URL_${ARCH}
RUN curl -sSfL ${!MINIO_URL} -o /usr/bin/minio && chmod +x /usr/bin/minio

# Install libqcow
RUN curl -sSfL https://s3-us-west-1.amazonaws.com/rancher-longhorn/libqcow-alpha-20181117.tar.gz | tar xvzf - -C /usr/src && \
    cd /usr/src/libqcow-20181117 && \
    ./configure && \
    make -j$(nproc) && \
    make install && \
    ldconfig

# GRPC health probe
ENV GRPC_HEALTH_PROBE_amd64=https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.2/grpc_health_probe-linux-amd64 \
    GRPC_HEALTH_PROBE_arm64=https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.2/grpc_health_probe-linux-arm64 \
    GRPC_HEALTH_PROBE=GRPC_HEALTH_PROBE_${ARCH}

RUN curl -sSfL ${!GRPC_HEALTH_PROBE} -o /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe

# TODO: use default python3 if SLE upgrade system python version to python3.11
RUN ln -sf /usr/bin/python3.11 /usr/bin/python3 & \
    ln -sf /usr/bin/pip3.11 /usr/bin/pip3 && \
    pip3 install grpcio==1.58.0 grpcio_tools==1.58.0 protobuf==${PROTOBUF_VER_PY}

RUN git clone https://github.com/longhorn/dep-versions.git -b ${SRC_BRANCH} /usr/src/dep-versions && \
    cd /usr/src/dep-versions && \
    if [ -n "${SRC_TAG}" ] && git show-ref --tags ${SRC_TAG} > /dev/null 2>&1; then \
        echo "Checking out tag ${SRC_TAG}"; \
        cd /usr/src/dep-versions && git checkout tags/${SRC_TAG}; \
    fi

# Build liblonghorn
RUN export REPO_OVERRIDE="" && \
    export COMMIT_ID_OVERRIDE="" && \
    bash /usr/src/dep-versions/scripts/build-liblonghorn.sh "${REPO_OVERRIDE}" "${COMMIT_ID_OVERRIDE}"

# Build TGT
RUN export REPO_OVERRIDE="" && \
    export COMMIT_ID_OVERRIDE="" && \
    bash /usr/src/dep-versions/scripts/build-tgt.sh "${REPO_OVERRIDE}" "${COMMIT_ID_OVERRIDE}"

# Build cache for tox
RUN mkdir integration/

COPY integration/setup.py integration/tox.ini integration/requirements.txt integration/flake8-requirements.txt integration/

RUN cd integration && \
    pip3 install tox==4.11.3; \
    tox --notest

# Build longhorn-instance-manager for integration testing
RUN cd /go/src/github.com/longhorn && \
    git clone https://github.com/longhorn/longhorn-instance-manager.git -b ${LONGHORN_INSTANCE_MANAGER_BRANCH} && \
    cd longhorn-instance-manager && \
    go build -o ./longhorn-instance-manager -tags netgo -ldflags "-linkmode external -extldflags -static" && \
    install longhorn-instance-manager /usr/local/bin

# Docker Buildx
RUN curl -sSfLO https://github.com/docker/buildx/releases/download/v0.13.1/buildx-v0.13.1.linux-${ARCH} && \
    chmod +x buildx-v0.13.1.linux-${ARCH} && \
    mv buildx-v0.13.1.linux-${ARCH} /usr/local/bin/buildx

COPY . .

ENTRYPOINT ["./scripts/entry"]
CMD ["ci"]

FROM base AS build
RUN ./scripts/build

FROM base AS validate
RUN ./scripts/validate && touch /validate.done

FROM base AS test
RUN ./scripts/test

FROM scratch AS build-artifacts
COPY --from=build /go/src/github.com/longhorn/longhorn-engine/bin/ /bin/

FROM scratch AS ci-artifacts
COPY --from=build /go/src/github.com/longhorn/longhorn-engine/bin/ /bin/
COPY --from=test /go/src/github.com/longhorn/longhorn-engine/coverage.out /coverage.out
COPY --from=validate /validate.done /validate.done
