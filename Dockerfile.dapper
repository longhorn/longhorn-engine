FROM registry.suse.com/bci/bci-base:15.4

ARG DAPPER_HOST_ARCH=amd64
ARG http_proxy
ARG https_proxy
ENV HOST_ARCH=${DAPPER_HOST_ARCH} ARCH=${DAPPER_HOST_ARCH}
ENV PROTOBUF_VER=3.18.0

# Setup environment
ENV PATH /go/bin:$PATH
ENV DAPPER_DOCKER_SOCKET true
ENV DAPPER_ENV TAG REPO
ENV DAPPER_OUTPUT bin coverage.out
ENV DAPPER_RUN_ARGS --privileged --tmpfs /go/src/github.com/longhorn/longhorn-engine/integration/.venv:exec --tmpfs /go/src/github.com/longhorn/longhorn-engine/integration/.tox:exec -v /dev:/host/dev -v /proc:/host/proc
ENV DAPPER_SOURCE /go/src/github.com/longhorn/longhorn-engine
WORKDIR ${DAPPER_SOURCE}

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper --gpg-auto-import-keys ref

# Install packages
RUN if [ ${ARCH} == "amd64" ]; then \
        zypper -n install autoconf libtool libunwind-devel; \
    fi

RUN zypper -n install cmake wget curl git less file gcc \
    libkmod-devel libnl3-devel linux-glibc-devel pkg-config psmisc tox qemu-tools fuse python3-devel \
    bash-completion librdmacm1 librdmacm-utils libibverbs xsltproc docbook-xsl-stylesheets \
    perl-Config-General libaio-devel glibc-devel-static glibc-devel sg3_utils iptables libltdl7 \
    python3-pip uuid-runtime libdevmapper1_03 iproute2 jq unzip zlib-devel zlib-devel-static \
    rpm-build rdma-core-devel gcc-c++ docker && \
    rm -rf rm -rf /var/cache/zypp/*

# needed for ${!var} substitution
RUN rm -f /bin/sh && ln -s /bin/bash /bin/sh

RUN if [ ${ARCH} == "s390x" ]; then \
        ln -s /usr/bin/gcc /usr/bin/s390x-linux-gnu-gcc;\
    fi

# Install Go & tools
ENV GOLANG_ARCH_amd64=amd64 GOLANG_ARCH_arm64=arm64 GOLANG_ARCH_s390x=s390x GOLANG_ARCH=GOLANG_ARCH_${ARCH} \
    GOPATH=/go PATH=/go/bin:/usr/local/go/bin:${PATH} SHELL=/bin/bash
RUN wget -O - https://storage.googleapis.com/golang/go1.20.3.linux-${!GOLANG_ARCH}.tar.gz | tar -xzf - -C /usr/local && \
    go install golang.org/x/lint/golint@latest

# Minio
ENV MINIO_URL_amd64=https://dl.min.io/server/minio/release/linux-amd64/archive/minio.RELEASE.2021-12-20T22-07-16Z \
    MINIO_URL_arm64=https://dl.min.io/server/minio/release/linux-arm64/archive/minio.RELEASE.2021-12-20T22-07-16Z \
    MINIO_URL_s390x=https://dl.min.io/server/minio/release/linux-s390x/archive/minio.RELEASE.2021-12-20T22-07-16Z \
	MINIO_URL=MINIO_URL_${ARCH}

RUN wget ${!MINIO_URL} -O /usr/bin/minio && chmod +x /usr/bin/minio

# Install libqcow
RUN wget -O - https://s3-us-west-1.amazonaws.com/rancher-longhorn/libqcow-alpha-20181117.tar.gz | tar xvzf - -C /usr/src
RUN cd /usr/src/libqcow-20181117 && \
    ./configure
RUN cd /usr/src/libqcow-20181117 && \
    make -j$(nproc) && \
    make install
RUN ldconfig

# GRPC dependencies
# GRPC health probe
ENV GRPC_HEALTH_PROBE_amd64=https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.2/grpc_health_probe-linux-amd64 \
    GRPC_HEALTH_PROBE_arm64=https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.2/grpc_health_probe-linux-arm64 \
    GRPC_HEALTH_PROBE_s390x=https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.2/grpc_health_probe-linux-s390x \
	GRPC_HEALTH_PROBE=GRPC_HEALTH_PROBE_${ARCH}

RUN wget ${!GRPC_HEALTH_PROBE} -O /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe

# protoc
ENV PROTOC_amd64=https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VER}/protoc-${PROTOBUF_VER}-linux-x86_64.zip \
	PROTOC_arm64=https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VER}/protoc-${PROTOBUF_VER}-linux-aarch_64.zip \
	PROTOC_s390x=https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VER}/protoc-${PROTOBUF_VER}-linux-s390_64.zip \
	PROTOC=PROTOC_${ARCH}

RUN cd /usr/src && \
	wget ${!PROTOC} -O protoc_${ARCH}.zip && \
    unzip protoc_${ARCH}.zip -d /usr/local/

# protoc-gen-go
RUN cd /go/src/github.com/ && \
    mkdir golang/ && \
    cd golang && \
    git clone https://github.com/golang/protobuf.git && \
    cd protobuf && \
    git checkout v1.3.2 && \
    cd protoc-gen-go && \
    go build && \
    cp protoc-gen-go /usr/local/bin

# python grpc-tools
RUN if [ "${ARCH}" == "s390x" ]; then \
        zypper -n in libopenssl-devel && \
        GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=True pip3 install grpcio==1.25.0 grpcio_tools==1.25.0 protobuf==${PROTOBUF_VER}; \
    else \
        pip3 install grpcio==1.25.0 grpcio_tools==1.25.0 protobuf==${PROTOBUF_VER}; \
    fi

# buf
ENV GO111MODULE=on
RUN go install github.com/bufbuild/buf/cmd/buf@v1.4.0

# Build liblonghorn
RUN cd /usr/src && \
    git clone https://github.com/rancher/liblonghorn.git && \
    cd liblonghorn && \
    git checkout 53d1c063b95efc8d949b095bd4bf04637230265f && \
    make; \
    make install

# Build TGT
RUN cd /usr/src && \
    git clone https://github.com/rancher/tgt.git && \
    cd tgt && \
    git checkout 3a8bc4823b5390e046f7aa8231ed262c0365c42c && \
    make; \
    make install

# Build cache for tox
RUN mkdir integration/
COPY integration/setup.py integration/tox.ini integration/requirements.txt integration/flake8-requirements.txt integration/
RUN cd integration && \
    if [ "${ARCH}" == "s390x" ]; then \
        GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=True tox --notest;\
    else \
        tox --notest; \
    fi

# Build longhorn-instance-manager for integration testing
RUN cd /go/src/github.com/longhorn && \
    git clone https://github.com/longhorn/longhorn-instance-manager.git && \
    cd longhorn-instance-manager && \
    git checkout v1_20210731 && \
    go build -o ./longhorn-instance-manager && \
    cp -r integration/rpc/ ${DAPPER_SOURCE}/integration/rpc/ && \
    cp longhorn-instance-manager /usr/local/bin

VOLUME /tmp
ENV TMPDIR /tmp
ENTRYPOINT ["./scripts/entry"]
CMD ["ci"]
