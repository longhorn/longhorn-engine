FROM registry.suse.com/bci/bci-base:15.4

ARG ARCH=amd64

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:tools:scm/15.4/devel:tools:scm.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/network:utilities/SLE_15_SP4/network:utilities.repo && \
    zypper --gpg-auto-import-keys ref

RUN if [ ${ARCH} == "amd64" ]; then \
        zypper -n install autoconf libtool libunwind-devel gcc-c++; \
    fi

RUN zypper -n install cmake kmod curl nfs-client nfs4-acl-tools fuse git gcc \
    libibverbs librdmacm1 rdma-core-devel perl-Config-General libaio1 sg3_utils \
    iputils telnet iperf qemu-tools wget iproute2 xsltproc docbook-xsl-stylesheets e2fsprogs && \
    rm -rf /var/cache/zypp/*

# Install grpc_health_probe
RUN wget https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.2/grpc_health_probe-linux-${ARCH} -O /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe

# Build gperftools
# 1. Just use tcmalloc lib from gperftools installation
# 2. tcmalloc also supports arm64 platform, but we only focus on amd64 first currently.
#    arm64 will be supported after more testing.
RUN if [ ${ARCH} == "amd64" ]; then \
        cd /usr/src && \
        git clone https://github.com/gperftools/gperftools.git&& \
        cd gperftools && \
        git checkout db6d94d9893fdd8c74028b5113cd9185be1e3111 && \
        ./autogen.sh && \
        ./configure && \
        make && \
        make install; \
        zypper -n remove autoconf libtool libunwind-devel gcc-c++; \
        rm -rf /usr/src/gperftools; \
        ldconfig; \
    fi

# Build liblonghorn
RUN cd /usr/src && \
    git clone https://github.com/rancher/liblonghorn.git && \
    cd liblonghorn && \
    git checkout 53d1c063b95efc8d949b095bd4bf04637230265f && \
    if [ ${ARCH} == "amd64" ]; then \
        make EXTRA_LIBS=/usr/local/lib/libtcmalloc.a; \
    else \
        make; \
    fi; \
    make install

# Build TGT
RUN cd /usr/src && \
    git clone https://github.com/rancher/tgt.git && \
    cd tgt && \
    git checkout 026a99fbc4e06ce898b5e9cbf9e037a7d938a7a4 && \
    if [ ${ARCH} == "amd64" ]; then \
        make EXTRA_LIBS=-ltcmalloc; \
    else \
        make; \
    fi; \
    make install

COPY bin/longhorn bin/longhorn-instance-manager /usr/local/bin/

COPY package/launch-simple-longhorn package/engine-manager package/launch-simple-file /usr/local/bin/

VOLUME /usr/local/bin

# Add Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["longhorn"]
