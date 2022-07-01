FROM registry.suse.com/bci/bci-base:15.3

ARG ARCH=amd64

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:tools:scm/15.3/devel:tools:scm.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/network:utilities/SLE_15_SP3/network:utilities.repo && \
    zypper --gpg-auto-import-keys ref

RUN zypper -n install cmake kmod curl nfs-client nfs4-acl-tools fuse git gcc \
    libibverbs librdmacm1 rdma-core-devel perl-Config-General libaio1 sg3_utils \
    iputils telnet iperf qemu-tools wget iproute2 xsltproc docbook-xsl-stylesheets e2fsprogs && \
    rm -rf /var/cache/zypp/*

# Install grpc_health_probe
RUN wget https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.2/grpc_health_probe-linux-${ARCH} -O /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe

# Build liblonghorn
RUN cd /usr/src && \
    git clone https://github.com/rancher/liblonghorn.git && \
    cd liblonghorn && \
    git checkout 9eaf16c13dccc7f3bdb111d4e21662ae753bdccf && \
    make && \
    make install && \
    rm -rf /usr/src/liblonghorn

# Build TGT
RUN cd /usr/src && \
    git clone https://github.com/rancher/tgt.git && \
    cd tgt && \
    git checkout e042fdd3616ca90619637b5826695a3de9b5dd8e && \
    make && \
    make install && \
    rm -rf /usr/src/tgt

COPY bin/longhorn bin/longhorn-instance-manager /usr/local/bin/

COPY package/launch-simple-longhorn package/engine-manager package/launch-simple-file /usr/local/bin/

VOLUME /usr/local/bin

# Add Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["longhorn"]
