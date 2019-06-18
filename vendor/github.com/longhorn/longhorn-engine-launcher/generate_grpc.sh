#!/bin/bash

mkdir -p $GOPATH/src/github.com/protocolbuffers && git clone https://github.com/protocolbuffers/protobuf.git $GOPATH/src/github.com/protocolbuffers/protobuf

protoc -I rpc/ -I $GOPATH/src/github.com/protocolbuffers/protobuf/src/ rpc/rpc.proto --go_out=plugins=grpc:rpc
