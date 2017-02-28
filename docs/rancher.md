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
