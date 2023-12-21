FROM registry.suse.com/bci/bci-base:15.5 AS builder

ARG ARCH=amd64

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/network:/utilities/SLE_15_SP5/network:utilities.repo && \
    zypper --gpg-auto-import-keys ref

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

RUN wget -O - https://s3-us-west-1.amazonaws.com/rancher-longhorn/libqcow-alpha-20181117.tar.gz | tar xvzf - -C /usr/src
RUN cd /usr/src/libqcow-20181117 && \
    ./configure
RUN cd /usr/src/libqcow-20181117 && \
    make -j$(nproc) && \
    make install

RUN zypper -n install libxml2-devel nbd
RUN cd /usr/src && \
    wget -O - https://download.libguestfs.org/libnbd/1.13-development/libnbd-1.13.1.tar.gz | tar -xzf - && \
    cd libnbd-1.13.1 && \
    ./configure --disable-ocaml --disable-python --disable-golang; \
    make; \
    make install

RUN ldconfig

# Install grpc_health_probe
RUN wget https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.21/grpc_health_probe-linux-${ARCH} -O /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe

FROM registry.suse.com/bci/bci-base:15.5 AS release

ARG ARCH=amd64

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/network:/utilities/SLE_15_SP5/network:utilities.repo && \
    zypper --gpg-auto-import-keys ref

RUN zypper -n install nfs-client nfs4-acl-tools cifs-utils libaio1 sg3_utils nbd \
    iputils iproute2 qemu-tools e2fsprogs && \
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

COPY bin/longhorn bin/longhorn-instance-manager /usr/local/bin/

COPY package/launch-simple-longhorn package/engine-manager package/launch-simple-file /usr/local/bin/

COPY --from=builder /usr/sbin/nbd-client /usr/sbin/nbd-client
COPY --from=builder /usr/local/lib /usr/local/lib
ENV LD_LIBRARY_PATH=/usr/local/lib

VOLUME /usr/local/bin

# Add Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["longhorn"]
