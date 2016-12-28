FROM ubuntu:16.04
RUN sed -i "s/http:\/\/archive.ubuntu.com\/ubuntu\//mirror:\/\/mirrors.ubuntu.com\/mirrors.txt/g" /etc/apt/sources.list && \
	apt-get update && apt-get install -y kmod curl nfs-common fuse \
        libibverbs1 librdmacm1 libconfig-general-perl libaio1

COPY longhorn launch copy-binary launch-with-vm-backing-file launch-simple-longhorn /usr/bin/
COPY tgt_*.deb /opt/
RUN dpkg -i /opt/tgt_*.deb
CMD ["longhorn"]
