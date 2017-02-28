Longhorn [![Build Status](https://drone.rancher.io/api/badges/rancher/longhorn/status.svg)](https://drone.rancher.io/rancher/longhorn)
========

A microservice that does micro things.

## Using Longhorn in Rancher

Longhorn can provide EBS-style persistent storage to Rancher. It means one volumes can be attached to the different containers, but they cannot be accessed by two containers at the same time.


### Installation

1. Make sure you are running Rancher v1.4 or higher.
  - To work with Rancher, Longhorn requires `iscsiadm` executable on the host. If you don't have `iscsiadm` on the host, you need to install `open-iscsi` package.
  - We recommend running Ubuntu 16.04 or later on your hosts.
2. In ADMIN -> Settings, click "Add Catalog":
  - Name: `longhorn`
  - URL:  `https://github.com/rancher/rancher-catalog.git`
  - Branch: `longhorn`
  - (NOTE: Do not edit `library` catalog)
3. In STACKS → Infrastructure, click "Add from Catalog" 
4. Choose "Longhorn" in the catalog, click "View Details" 
5. Click "Launch"

Once Longhorn storage driver is deployed, you can create Longhorn volumes and use them with your containers and services.


### Add a Longhorn Volume

You don't need to create volume beforehand if you're fine with default volume size (10GB).

Otherwise, go to INFRASTRUCTURE -> Storage: you'll see `rancher-longhorn` driver. 

Click "Add Volume" and fill in the "Name" field.

Specify volume size with Driver Options (e.g. Key: `size`  Value: `300G`)

**NOTE:** Volume names can use latin letters, numbers, `-` and `_`. Do not start or end the name with `-` or `_`.


### Use a Longhorn Volume

When creating a container or a service, you need to specify the volume name and the volume driver `rancher-longhorn` for the volume.

If volume wasn't created before, it will be created automatically with size of 10GB.

For example, if we have created a Longhorn volume named `vol1`, we can use it to create a container:

Volumes: `vol1:/any/mount/path`
Volume Driver: `rancher-longhorn`


### Rancher UI Note: Longhorn Volume components

Longhorn driver creates and manages `controller` and `replica-<XXXX>` containers enabling the volumes. These will be shown in Rancher UI as part of the volume stacks in a future release. They are currently visible as standalone containers on your hosts. 

When a volume is mounted, a `controller` container is run on the same host as the client container and `replica-XXXX` containers are started.

When the volume is not used, `replica-XXXX` containers are stopped and the `controller` container is removed.


## Building from source code

`make`


## Running Standalone

You can choose either TGT or TCMU frontend. TGT frontend is recommended. TGT
can work with majority of the Linux distributions, while TCMU can work with
RancherOS v0.4.4 and above only.

Host need to have `docker` installed. Run following command to check:
```
docker info
```

### With TGT frontend

User need to make sure the host has `iscsiadm` installed. Run following command to check:
```
iscsiadm --version
```

To start Longhorn with an single replica, run following command:
```
docker run --privileged -v /dev:/host/dev -v /proc:/host/proc -v /volume rancher/longhorn launch-simple-longhorn vol-name 10g tgt
```

That will create the device `/dev/longhorn/vol-name`

### With TCMU frontend

You need to be running RancherOS v0.4.4 (all kernel patches are upstreamed but only available after Linux v4.5).
Also ensure that TCMU is enabled:

    modprobe target_core_user
    mount -t configfs none /sys/kernel/config

To start Longhorn with an single replica, run following command:
```
docker run --privileged -v /dev:/host/dev -v /proc:/host/proc -v /sys/kernel/config:/sys/kernel/config -v /volume rancher/longhorn launch-simple-longhorn vol-name 10g tcmu
```

That will create the device `/dev/longhorn/vol-name`

### Longhorn with multiple replicas

In order to start Longhorn with multiple replicas, you need to setup a network between Longhorn replica container and controller container. Here we use Docker(v1.10 or later) network feature to demostrate that:

##### 1. Create a network named `longhorn-net`
```
docker network create --subnet=172.18.0.0/16 longhorn-net
```
##### 2. Add two replicas to the network, suppose their IPs are `172.18.0.2` and `172.18.0.3`:
```
docker run --net longhorn-net --ip 172.18.0.2 --expose 9502-9504 -v /volume rancher/longhorn launch replica --listen 172.18.0.2:9502 --size 10g /volume
docker run --net longhorn-net --ip 172.18.0.3 --expose 9502-9504 -v /volume rancher/longhorn launch replica --listen 172.18.0.3:9502 --size 10g /volume
```
Notice you need to expose port 9502 to 9504 for Longhorn controller to communicate with replica.
##### 3. Start Longhorn controller. Take TGT for example:
```
docker run --net longhorn-net --privileged -v /dev:/host/dev -v /proc:/host/proc rancher/longhorn launch controller --frontend tgt --replica tcp://172.18.0.2:9502 --replica tcp://172.18.0.3:9502 vol-name
```
Now you will have device `/dev/longhorn/vol-name`.

## License
Copyright (c) 2014-2017 [Rancher Labs, Inc.](http://rancher.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
