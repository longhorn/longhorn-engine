FROM ubuntu:16.04
RUN apt-get update && apt-get install -y kmod curl nfs-common

RUN curl -sSL -o share-mnt https://github.com/rancher/runc/releases/download/share-mnt-v0.0.3/share-mnt && \
    chmod u+x share-mnt && mv share-mnt /usr/bin

COPY longhorn longhorn-agent launch launch-driver docker-longhorn-driver copy-binary launch-with-vm-backing-file launch-simple-longhorn /usr/bin/
CMD ["longhorn"]
