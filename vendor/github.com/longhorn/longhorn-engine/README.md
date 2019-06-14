Longhorn Engine 
========
[![Build Status](https://drone-pr.rancher.io/api/badges/longhorn/longhorn-engine/status.svg)](https://drone-pr.rancher.io/longhorn/longhorn-engine) [![Go Report Card](https://goreportcard.com/badge/github.com/rancher/longhorn-engine)](https://goreportcard.com/report/github.com/rancher/longhorn-engine)

Longhorn Engine implements a lightweight block device storage controller capable of storing the data in a number of replicas. It functions like a network RAID controller.

1. The replicas are backed by Linux sparse files, and support efficient snapshots using differencing disks.
1. The replicas function like a networked disk, supporting read/write operations over a network protocol.
1. The frontend (either TCMU or Open-iSCSI/tgt are supported at this moment) is a kernel driver that translates read/write operations on the Longhorn block device (mapped at `/dev/longhorn/vol-name`) to user-level network requests on the controller.
1. Each Longhorn block device is backed by its own dedicated controller.
1. The controller sychronously replicates write operations to all replicas.
1. The controller detects faulty replicas and rebuilds replicas.
1. The controller coordinates snapshot and backup operations.
1. Controllers and replicas are packaged as Docker containers.

The following figure illustrates the relationship between the Longhorn block device, TCMU/tgt frontend, controller, and replicas.

![Overview Graphics](/overview.png)

## Building from source code

`make`


## Running a controller with a single replica

The easiest way to try the Longhorn Engine is to start a controller with a single replica.

You can choose either TGT or TCMU frontend. TGT frontend is recommended. TGT
can work with majority of the Linux distributions, while TCMU can work with
RancherOS v0.4.4 and above only.

Host needs to have `docker` installed. Run following command to make sure:
```
docker info
```

#### With TGT frontend

User need to make sure the host has `iscsiadm` installed. Run following command to check:
```
iscsiadm --version
```

To start Longhorn Engine with an single replica, run following command:
```
docker run --privileged -v /dev:/host/dev -v /proc:/host/proc -v /volume \
    rancher/longhorn-engine launch-simple-longhorn vol-name 10g tgt
```

That will create the device `/dev/longhorn/vol-name`

#### With TCMU frontend

You need to be running RancherOS v0.4.4 (all kernel patches are upstreamed but only available after Linux v4.5).
Also ensure that TCMU is enabled:

    modprobe target_core_user
    mount -t configfs none /sys/kernel/config

To start Longhorn Engine with an single replica, run following command:
```
docker run --privileged -v /dev:/host/dev -v /proc:/host/proc \
    -v /sys/kernel/config:/sys/kernel/config -v /volume \
    rancher/longhorn-engine launch-simple-longhorn vol-name 10g tcmu
```

That will create the device `/dev/longhorn/vol-name`

## Running a controller with multiple replicas

In order to start Longhorn Engine with multiple replicas, you need to setup a network between replica container and controller container. Here we use Docker network feature to demostrate that:

##### 1. Create a network named `longhorn-net`
```
docker network create --subnet=172.18.0.0/16 longhorn-net
```
##### 2. Add two replicas to the network, and set their IPs to `172.18.0.2` and `172.18.0.3`:
```
docker run --net longhorn-net --ip 172.18.0.2 -v /volume \
    rancher/longhorn-engine launch replica --listen 172.18.0.2:9502 --size 10g /volume
docker run --net longhorn-net --ip 172.18.0.3 -v /volume \
    rancher/longhorn-engine launch replica --listen 172.18.0.3:9502 --size 10g /volume
```

##### 3. Start the controller. Take TGT for example:
```
docker run --net longhorn-net --privileged -v /dev:/host/dev -v /proc:/host/proc \
    rancher/longhorn-engine launch controller --frontend tgt \
    --replica tcp://172.18.0.2:9502 --replica tcp://172.18.0.3:9502 vol-name
```
Now you will have device `/dev/longhorn/vol-name`.

## Run `longhorn` command

The `longhorn` command allows you to manage a Longhorn controller. By executing the `longhorn` command in the controller container, you can list replicas, add and remove replicas, take snapshots, and create backups.

```
$ docker exec <controller-docker-id> longhorn ls
ADDRESS               MODE CHAIN
tcp://172.18.0.2:9502 RW   [volume-head-000.img]
tcp://172.18.0.3:9502 RW   [volume-head-000.img]
```

## License
Copyright (c) 2014-2019 [Rancher Labs, Inc.](http://rancher.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
