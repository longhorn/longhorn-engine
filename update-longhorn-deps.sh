#!/bin/bash

go get github.com/longhorn/go-iscsi-helper@master
go get github.com/longhorn/sparse-tools@master
go get github.com/longhorn/backupstore@master
go mod tidy
go mod vendor
