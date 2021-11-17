FROM ubuntu:20.04

ARG ARCH=amd64

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get install -y kmod curl nfs-common fuse \
    libibverbs1 librdmacm1 libconfig-general-perl libaio1 sg3-utils \
    iputils-ping telnet iperf qemu-utils wget iproute2 && \
    rm -rf /var/lib/apt/lists/*

# Install grpc_health_probe
RUN wget https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.2/grpc_health_probe-linux-${ARCH} -O /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe

COPY bin/longhorn bin/longhorn-instance-manager /usr/local/bin/

COPY package/launch-simple-longhorn package/engine-manager package/launch-simple-file /usr/local/bin/

COPY bin/tgt_*.deb /opt/

RUN dpkg -i /opt/tgt_*.deb

VOLUME /usr/local/bin

# Add Tini
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["longhorn"]
