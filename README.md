longhorn
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
    ./bin/longhorn controller --frontend tcmu --replica tcp://localhost:9502 vol-name

That will create the device `/dev/longhorn/vol-name`

## Add Replica/Rebuild

To add a live replica needs the sync agent to be running.  The replica runs by default on port
9502 and 9053.  9502 is the HTTP control port and 9503 is the data path.  The sync agent must run
on the next port, which is 9503 by default.  You must run the sync agent for each running replica and
in the same folder as the replica.  For example:

    ./bin/longhorn replica --size 10g /opt/volume &
    cd /opt/volume
    $OLDPWD/bin/longhorn sync-agent &

You also need to install `ssync` to your $PATH.  The code for that is available at https://github.com/kp6/alphorn/tree/master/ssync

To perform the actual add replica/rebuild run the below command

    ./bin/longhorn add-replica tcp://123.123.123.123:9502

If that program exist with exit code 0, then it worked.

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
