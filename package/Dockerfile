FROM ubuntu:18.04

ARG ARCH=amd64

RUN apt-get update && apt-get install -y kmod curl nfs-common fuse \
        libibverbs1 librdmacm1 libconfig-general-perl libaio1 sg3-utils \
        iputils-ping telnet iperf qemu-utils wget iproute2

# needed for ${!var} substitution
RUN rm -f /bin/sh && ln -s /bin/bash /bin/sh

# Install grpc_health_probe
ENV GRPC_HEALTH_amd64=https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.1/grpc_health_probe-linux-amd64 \
    GRPC_HEALTH_arm=https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.1/grpc_health_probe-linux-arm \
    GRPC_HEALTH_arm64=https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.1/grpc_health_probe-linux-arm64 \
    GRPC_HEALTH=GRPC_HEALTH_${ARCH}

RUN wget -O /usr/local/bin/grpc_health_probe ${!GRPC_HEALTH} && \
    chmod +x /usr/local/bin/grpc_health_probe

COPY bin/longhorn bin/longhorn-instance-manager /usr/local/bin/

COPY package/launch-simple-longhorn package/engine-manager package/launch-simple-file /usr/local/bin/

COPY bin/tgt_*.deb /opt/

RUN dpkg -i /opt/tgt_*.deb

VOLUME /usr/local/bin

# Add Tini
ENV TINI_VERSION v0.18.0
ENV TINI_amd64=https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-amd64 \
    TINI_arm=https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-armhf \
    TINI_arm64=https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-arm64 \
    TINI=TINI_${ARCH}

ADD ${!TINI} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["longhorn"]
