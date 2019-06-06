FROM ubuntu:16.04
RUN apt-get update && apt-get install -y kmod curl nfs-common fuse \
        libibverbs1 librdmacm1 libconfig-general-perl libaio1 sg3-utils \
        iputils-ping telnet iperf qemu-utils wget

# Install grpc_health_probe
RUN wget -O /usr/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.0/grpc_health_probe-linux-amd64 && \
    chmod +x /usr/bin/grpc_health_probe

COPY longhorn launch launch-simple-longhorn longhorn-engine-launcher engine-launcher launch-simple-file /usr/local/bin/
COPY tgt_*.deb /opt/
RUN dpkg -i /opt/tgt_*.deb
VOLUME /usr/local/bin
CMD ["longhorn"]
