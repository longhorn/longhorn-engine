#!/bin/bash

set -e

# check and download dependency for gRPC code generate
if [ ! -e ./vendor_proto/protobuf/src/google/protobuf ]; then
    rm -rf ./vendor_proto/protobuf/src/google/protobuf
    DIR="./vendor_proto/protobuf/src/google/protobuf"
    mkdir -p $DIR
    wget https://raw.githubusercontent.com/protocolbuffers/protobuf/v3.9.0/src/google/protobuf/empty.proto -P $DIR
fi

# controller
protoc -I controller/rpc/pb -I vendor_proto/protobuf/src/ controller/rpc/pb/controller.proto --go_out=plugins=grpc:controller/rpc/pb
python -m grpc_tools.protoc -I controller/rpc/pb -I vendor_proto/protobuf/src/ --python_out=integration/rpc/controller --grpc_python_out=integration/rpc/controller controller/rpc/pb/controller.proto

# replica
protoc -I replica/rpc -I vendor_proto/protobuf/src/ replica/rpc/replica.proto --go_out=plugins=grpc:replica/rpc
python -m grpc_tools.protoc -I replica/rpc -I vendor_proto/protobuf/src/ --python_out=integration/rpc/replica --grpc_python_out=integration/rpc/replica replica/rpc/replica.proto

# sync agent
protoc -I sync/rpc -I vendor_proto/protobuf/src/ sync/rpc/rpc.proto --go_out=plugins=grpc:sync/rpc

# instance manager
python -m grpc_tools.protoc -I vendor/github.com/longhorn/longhorn-instance-manager/rpc -I vendor_proto/protobuf/src/ --python_out=integration/rpc/instance_manager --grpc_python_out=integration/rpc/instance_manager vendor/github.com/longhorn/longhorn-instance-manager/rpc/rpc.proto
