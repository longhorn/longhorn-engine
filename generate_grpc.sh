#!/bin/bash

# check and download dependency for gRPC code generate
set -e

# check and download dependency for gRPC code generate
if [ ! -e ./vendor_proto/protobuf/src/google/protobuf ]; then
    rm -rf ./vendor_proto/protobuf/src/google/protobuf
    DIR="./vendor_proto/protobuf/src/google/protobuf"
    mkdir -p $DIR
    wget https://raw.githubusercontent.com/protocolbuffers/protobuf/v3.9.0/src/google/protobuf/empty.proto -P $DIR
fi

protoc -I rpc/ -I vendor_proto/protobuf/src/ rpc/rpc.proto --go_out=plugins=grpc:rpc
