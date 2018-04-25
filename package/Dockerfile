FROM ubuntu:16.04
RUN apt-get update && apt-get install -y kmod curl nfs-common fuse \
        libibverbs1 librdmacm1 libconfig-general-perl libaio1

COPY longhorn launch copy-binary launch-with-vm-backing-file launch-simple-longhorn longhorn-engine-launcher /usr/local/bin/
COPY tgt_*.deb /opt/
RUN dpkg -i /opt/tgt_*.deb
VOLUME /usr/local/bin
CMD ["longhorn"]
