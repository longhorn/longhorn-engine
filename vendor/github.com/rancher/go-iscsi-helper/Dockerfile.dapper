FROM ubuntu:16.04

# Setup environment
ENV PATH /go/bin:$PATH
ENV DAPPER_DOCKER_SOCKET true
ENV DAPPER_ENV TAG REPO
ENV DAPPER_OUTPUT bin
ENV DAPPER_RUN_ARGS --privileged -v /dev:/host/dev -v /proc:/host/proc
ENV DAPPER_SOURCE /go/src/github.com/rancher/go-iscsi-helper
ENV TRASH_CACHE ${DAPPER_SOURCE}/.trash-cache
WORKDIR ${DAPPER_SOURCE}

# Install packages
RUN sed -i "s/http:\/\/archive.ubuntu.com\/ubuntu\//mirror:\/\/mirrors.ubuntu.com\/mirrors.txt/g" /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y cmake curl git \
        devscripts debhelper bash-completion librdmacm-dev libibverbs-dev xsltproc docbook-xsl \
        libconfig-general-perl libaio-dev libc6-dev

# Install Go
RUN curl -o go.tar.gz https://storage.googleapis.com/golang/go1.7.linux-amd64.tar.gz
RUN echo '702ad90f705365227e902b42d91dd1a40e48ca7f67a2f4b2fd052aaa4295cd95 go.tar.gz' | sha256sum -c && \
    tar xzf go.tar.gz -C /usr/local && \
    rm go.tar.gz
RUN mkdir -p /go
ENV PATH $PATH:/usr/local/go/bin
ENV GOPATH=/go

# Go tools
RUN go get github.com/rancher/trash
RUN go get github.com/golang/lint/golint

# Docker
RUN curl -sL https://get.docker.com/builds/Linux/x86_64/docker-1.9.1 > /usr/bin/docker && \
    chmod +x /usr/bin/docker

# Build TGT
RUN cd /usr/src && \
    git clone https://github.com/rancher/tgt.git && \
    cd tgt && \
    git checkout 4574d18db4eeebb0f761fe272ccbe1e3ff681b7f && \
    ./scripts/build-pkg.sh deb && \
    dpkg -i ./pkg/tgt_*.deb

VOLUME /tmp
ENV TMPDIR /tmp
ENTRYPOINT ["./scripts/entry"]
CMD ["ci"]
