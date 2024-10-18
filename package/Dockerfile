FROM registry.suse.com/bci/bci-base:15.6 AS builder

ARG ARCH=amd64

RUN zypper -n ref && \
    zypper update -y

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/network:/utilities/SLE_15_SP5/network:utilities.repo && \
    zypper --gpg-auto-import-keys ref

RUN zypper -n install autoconf automake libtool gcc-c++

RUN zypper -n install cmake curl git gcc wget xsltproc docbook-xsl-stylesheets && \
    rm -rf /var/cache/zypp/*

# Build liblonghorn
ENV LIBLONGHORN_COMMIT_ID 53d1c063b95efc8d949b095bd4bf04637230265f
RUN cd /usr/src && \
    git clone https://github.com/rancher/liblonghorn.git && \
    cd liblonghorn && \
    git checkout ${LIBLONGHORN_COMMIT_ID} && \
    make; \
    make install

# Build TGT
ENV TGT_COMMIT_ID 3a8bc4823b5390e046f7aa8231ed262c0365c42c
RUN cd /usr/src && \
    git clone https://github.com/rancher/tgt.git && \
    cd tgt && \
    git checkout ${TGT_COMMIT_ID} && \
    make; \
    make install

# Build ubdsrv
ENV UBD_COMMIT_ID 19d3b2133baf1af8ae3a5fe300c962567fb7b0ce
RUN git clone --depth 1 --branch liburing-2.5 https://github.com/axboe/liburing.git /usr/src/liburing && \
    cd /usr/src/liburing && \
    ./configure --cc=gcc --cxx=g++ && \
    make -j$(nproc) && \
    make installgit clone https://github.com/Kampadais/ubdsrv.git && \
    cd ubdsrv && \
    git checkout ${UBD_COMMIT_ID} && \
    export LIBURING_CFLAGS="-I/usr/include/liburing" && \
    export LIBURING_LIBS="-Lusr/lib/pkgconfig -luring"cd /usr/src && \
    ls;autoreconf -i && \
    ./configure && \
    make; \
    make install

# Install grpc_health_probe
RUN wget https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.28/grpc_health_probe-linux-${ARCH} -O /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe

FROM registry.suse.com/bci/bci-base:15.6 AS release

ARG ARCH=amd64

RUN zypper -n ref && \
    zypper update -y

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/network:/utilities/SLE_15_SP5/network:utilities.repo && \
    zypper --gpg-auto-import-keys ref

RUN zypper -n install nfs-client nfs4-acl-tools cifs-utils libaio1 sg3_utils \
    iputils iproute2 qemu-tools e2fsprogs jq && \
    rm -rf /var/cache/zypp/*

# Copy pre-built binaries from builder
COPY --from=builder \
    /usr/local/bin/grpc_health_probe \
    /usr/sbin/tgt-admin \
    /usr/sbin/tgt-setup-lun \
    /usr/sbin/tgtadm \
    /usr/sbin/tgtd \
    /usr/sbin/tgtimg \
    /usr/local/bin/

COPY package/bin/longhorn /usr/local/bin/
COPY package/bin/longhorn-instance-manager /usr/local/bin/

COPY package/launch-simple-longhorn package/engine-manager package/launch-simple-file /usr/local/bin/

# Add Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["longhorn"]
