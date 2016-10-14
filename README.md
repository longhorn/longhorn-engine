longhorn [![Build Status](https://drone.rancher.io/api/badges/rancher/longhorn/status.svg)](https://drone.rancher.io/rancher/longhorn)
========

A microservice that does micro things.

## Building

`make`

## Running

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

To be documented.

## License
Copyright (c) 2014-2016 [Rancher Labs, Inc.](http://rancher.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
