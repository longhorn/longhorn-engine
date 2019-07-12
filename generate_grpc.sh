#!/bin/bash

protoc -I rpc/ -I vendor/github.com/golang/protobuf/ptypes/empty rpc/rpc.proto --go_out=plugins=grpc:rpc
