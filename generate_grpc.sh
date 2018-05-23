#!/bin/bash

protoc -I rpc/ rpc/rpc.proto --go_out=plugins=grpc:rpc
