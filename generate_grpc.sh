#!/bin/bash

set -e

# check and download dependency for gRPC code generate
if [ ! -e ./proto/vendor/protobuf/src/google/protobuf ]; then
    rm -rf ./proto/vendor/protobuf/src/google/protobuf
    DIR="./proto/vendor/protobuf/src/google/protobuf"
    mkdir -p $DIR
    wget https://raw.githubusercontent.com/protocolbuffers/protobuf/v24.3/src/google/protobuf/empty.proto -P $DIR
fi

PKG_DIR="proto/ptypes"
TMP_DIR_BASE=".protobuild"
TMP_DIR="${TMP_DIR_BASE}/github.com/longhorn/longhorn-engine/proto/ptypes/"
mkdir -p "${TMP_DIR}"
cp -a "${PKG_DIR}"/*.proto "${TMP_DIR}"
for PROTO in common controller replica syncagent; do
    mkdir -p "integration/rpc/${PROTO}"
    python3 -m grpc_tools.protoc -I "${TMP_DIR_BASE}" -I "proto/vendor/" -I "proto/vendor/protobuf/src/" --python_out=integration/rpc/${PROTO} --grpc_python_out=integration/rpc/${PROTO} "${TMP_DIR}/${PROTO}.proto"
    protoc -I ${TMP_DIR_BASE}/ -I proto/vendor/ -I proto/vendor/protobuf/src/ "${TMP_DIR}/${PROTO}.proto" --go_out=plugins=grpc:"${TMP_DIR_BASE}"
    mv "${TMP_DIR}/${PROTO}.pb.go" "${PKG_DIR}/${PROTO}.pb.go"
    mv integration/rpc/${PROTO}/github.com/longhorn/longhorn_engine/proto/ptypes/${PROTO}_pb2_grpc.py integration/rpc/${PROTO}/github/com/longhorn/longhorn_engine/proto/ptypes/${PROTO}_pb2_grpc.py
    rm -rf integration/rpc/${PROTO}/github.com
    if [ "${PROTO}" != "common" ]; then
        cp -a integration/rpc/common/github/com/longhorn/longhorn_engine/proto/ptypes/*.py integration/rpc/${PROTO}/github/com/longhorn/longhorn_engine/proto/ptypes/
    fi
    if [ "${PROTO}" = "syncagent" ]; then
        cp -a integration/rpc/syncagent/* integration/rpc/sync
    fi
done

# clean up
rm -rf integration/rpc/common
rm -rf integration/rpc/syncagent
rm -rf "${TMP_DIR_BASE}"
