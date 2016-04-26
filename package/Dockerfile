FROM ubuntu:16.04

RUN apt-get update && apt-get install kmod

COPY longhorn /usr/bin/
COPY launch /usr/bin/
COPY ssync /usr/bin/
CMD ["longhorn"]
