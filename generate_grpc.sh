#!/bin/bash

set -e

# check and download dependency for gRPC code generate
if [ ! -e ./proto/vendor/protobuf/src/google/protobuf ]; then
    rm -rf ./proto/vendor/protobuf/src/google/protobuf
    DIR="./proto/vendor/protobuf/src/google/protobuf"
    mkdir -p $DIR
    wget https://raw.githubusercontent.com/protocolbuffers/protobuf/v3.9.0/src/google/protobuf/empty.proto -P $DIR
fi

# proto lint check
buf lint

# common
protoc -I proto/ptypes/ -I proto/vendor/protobuf/src/ proto/ptypes/common.proto --go_out=plugins=grpc:proto/ptypes/
python3 -m grpc_tools.protoc -I proto/ptypes/ -I proto/vendor/protobuf/src/ --python_out=integration/rpc/controller --grpc_python_out=integration/rpc/controller proto/ptypes/common.proto
python3 -m grpc_tools.protoc -I proto/ptypes/ -I proto/vendor/protobuf/src/ --python_out=integration/rpc/replica --grpc_python_out=integration/rpc/replica proto/ptypes/common.proto
python3 -m grpc_tools.protoc -I proto/ptypes/ -I proto/vendor/protobuf/src/ --python_out=integration/rpc/sync --grpc_python_out=integration/rpc/sync proto/ptypes/common.proto

# controller
protoc -I proto/ptypes/ -I proto/vendor/protobuf/src/ proto/ptypes/controller.proto --go_out=plugins=grpc:proto/ptypes/
python3 -m grpc_tools.protoc -I proto/ptypes/ -I proto/vendor/protobuf/src/ --python_out=integration/rpc/controller --grpc_python_out=integration/rpc/controller proto/ptypes/controller.proto

# replica
protoc -I proto/ptypes/ -I proto/vendor/protobuf/src/ proto/ptypes/replica.proto --go_out=plugins=grpc:proto/ptypes/
python3 -m grpc_tools.protoc -I proto/ptypes/ -I proto/vendor/protobuf/src/ --python_out=integration/rpc/replica --grpc_python_out=integration/rpc/replica proto/ptypes/replica.proto

# sync agent
protoc -I proto/ptypes/ -I proto/vendor/protobuf/src/ proto/ptypes/syncagent.proto --go_out=plugins=grpc:proto/ptypes/
python3 -m grpc_tools.protoc -I proto/ptypes/ -I proto/vendor/protobuf/src/ --python_out=integration/rpc/sync --grpc_python_out=integration/rpc/sync proto/ptypes/syncagent.proto
