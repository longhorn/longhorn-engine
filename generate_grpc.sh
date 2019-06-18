#!/bin/bash

# controller
protoc -I controller/rpc/pb -I vendor/github.com/golang/protobuf/ptypes/  controller/rpc/pb/controller.proto --go_out=plugins=grpc:controller/rpc/pb
python -m grpc_tools.protoc -I controller/rpc/pb -I vendor/github.com/golang/protobuf/ptypes/ --python_out=integration/rpc/controller --grpc_python_out=integration/rpc/controller controller/rpc/pb/controller.proto

# replica
protoc -I replica/rpc -I vendor/github.com/golang/protobuf/ptypes/  replica/rpc/replica.proto --go_out=plugins=grpc:replica/rpc
python -m grpc_tools.protoc -I replica/rpc -I vendor/github.com/golang/protobuf/ptypes/ --python_out=integration/rpc/replica --grpc_python_out=integration/rpc/replica replica/rpc/replica.proto

# sync agent
protoc -I sync/rpc sync/rpc/rpc.proto --go_out=plugins=grpc:sync/rpc

# engine manager
python -m grpc_tools.protoc -I vendor/github.com/longhorn/longhorn-engine-launcher/rpc -I vendor/github.com/golang/protobuf/ptypes/ --python_out=integration/rpc/engine_manager --grpc_python_out=integration/rpc/engine_manager vendor/github.com/longhorn/longhorn-engine-launcher/rpc/rpc.proto