#!/bin/bash

# check and download dependency for gRPC code generate
if [ ! -e ./vendor_proto/protobuf/src/google/protobuf ]; then
    rm -rf ./vendor_proto/protobuf/ &&
    git clone https://github.com/protocolbuffers/protobuf.git ./vendor_proto/protobuf/ &&
    cd ./vendor_proto/protobuf/ &&
    git checkout tags/v3.9.0 &&
    find . ! -regex './src/google/protobuf.*' -delete &&
    cd ../../
fi

protoc -I rpc/ -I vendor_proto/protobuf/src/ rpc/rpc.proto --go_out=plugins=grpc:rpc
