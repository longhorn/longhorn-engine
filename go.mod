module github.com/longhorn/longhorn-instance-manager

go 1.13

replace github.com/rancher/kine => github.com/ibuildthecloud/kine v0.0.0-20190807043656-10106605fb70

require (
	github.com/RoaringBitmap/roaring v0.4.18
	github.com/c9s/goprocinfo v0.0.0-20170724085704-0010a05ce49f // indirect
	github.com/docker/go-units v0.3.3
	github.com/golang/protobuf v1.3.1
	github.com/gorilla/handlers v1.4.2 // indirect
	github.com/longhorn/go-iscsi-helper v0.0.0-20190816204506-694fd1e35575
	github.com/longhorn/longhorn-engine v0.5.1-0.20190805181732-15da61515414
	github.com/pkg/errors v0.8.1
	github.com/rancher/kine v0.0.0-00010101000000-000000000000
	github.com/satori/go.uuid v1.0.0
	github.com/sirupsen/logrus v1.4.1
	github.com/tinylib/msgp v1.1.1-0.20190612170807-0573788bc2a8 // indirect
	github.com/urfave/cli v1.22.1
	github.com/yasker/nsfilelock v0.0.0-20161215025017-90eff0ebb9e2 // indirect
	golang.org/x/net v0.0.0-20190311183353-d8887717615a
	google.golang.org/grpc v1.21.0
	gopkg.in/check.v1 v1.0.0-20160105164936-4f90aeace3a2
)
