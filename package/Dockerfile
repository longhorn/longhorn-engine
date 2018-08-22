FROM ubuntu:16.04
RUN apt-get update && apt-get install -y kmod curl nfs-common fuse \
        libibverbs1 librdmacm1 libconfig-general-perl libaio1 sg3-utils \
        iputils-ping telnet iperf

COPY longhorn launch launch-simple-longhorn longhorn-engine-launcher engine-launcher /usr/local/bin/
COPY tgt_*.deb /opt/
RUN dpkg -i /opt/tgt_*.deb
VOLUME /usr/local/bin
CMD ["longhorn"]
