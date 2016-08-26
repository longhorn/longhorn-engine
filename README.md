longhorn [![Build Status](https://drone.rancher.io/api/badges/rancher/longhorn/status.svg)](https://drone.rancher.io/rancher/longhorn)
========

A microservice that does micro things.

## Building

`make`

## Running

You need to be running RancherOS v0.4.4 (all kernel patches are not yet fully upstreamed).
Also ensure that TCMU is enabled:

    modprobe target_core_user
    mount -t configfs none /sys/kernel/config

Then run

    ./bin/longhorn replica --size 10g /opt/volume
    ./bin/longhorn replica --size 10g --listen localhost:9505 /opt/volume2
    ./bin/longhorn controller --frontend tcmu --replica tcp://localhost:9502 --replica tcp://localhost:9505 vol-name

That will create the device `/dev/longhorn/vol-name`

## Ports

Each replica uses three consequetive ports.  By default they are 9502, 9503, and 9504.  They are used for
controll channel, data channel, and sync communication respectively.

## CLI

List replicas

    longhorn ls

Remove replica

    longhorn rm tcp://localhost:9502

Add replica

    longhorn add tcp://localhost:9502

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
