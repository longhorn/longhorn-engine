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
protoc -I pkg/engine/controller/rpc/pb -I vendor_proto/protobuf/src/ pkg/engine/controller/rpc/pb/controller.proto --go_out=plugins=grpc:pkg/engine/controller/rpc/pb
python3 -m grpc_tools.protoc -I pkg/engine/controller/rpc/pb -I vendor_proto/protobuf/src/ --python_out=integration/rpc/controller --grpc_python_out=integration/rpc/controller pkg/engine/controller/rpc/pb/controller.proto

# replica
protoc -I pkg/engine/replica/rpc -I vendor_proto/protobuf/src/ pkg/engine/replica/rpc/replica.proto --go_out=plugins=grpc:pkg/engine/replica/rpc
python3 -m grpc_tools.protoc -I pkg/engine/replica/rpc -I vendor_proto/protobuf/src/ --python_out=integration/rpc/replica --grpc_python_out=integration/rpc/replica pkg/engine/replica/rpc/replica.proto

# sync agent
protoc -I pkg/engine/sync/rpc -I vendor_proto/protobuf/src/ pkg/engine/sync/rpc/rpc.proto --go_out=plugins=grpc:pkg/engine/sync/rpc

# instance manager
python3 -m grpc_tools.protoc -I pkg/instance-manager/rpc -I vendor_proto/protobuf/src/ --python_out=integration/rpc/instance_manager --grpc_python_out=integration/rpc/instance_manager pkg/instance-manager/rpc/rpc.proto
protoc -I pkg/instance-manager/rpc/ -I vendor_proto/protobuf/src/ pkg/instance-manager/rpc/rpc.proto --go_out=plugins=grpc:pkg/instance-manager/rpc
