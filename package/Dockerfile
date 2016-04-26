FROM ubuntu:16.04
RUN apt-get update && apt-get install kmod

COPY longhorn longhorn-agent launch ssync docker-longhorn-driver /usr/bin/
CMD ["longhorn"]
