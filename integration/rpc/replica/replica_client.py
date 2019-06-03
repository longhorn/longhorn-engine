import grpc

import replica_pb2
import replica_pb2_grpc
from google.protobuf import empty_pb2


class ReplicaClient(object):
    def __init__(self, url):
        self.channel = grpc.insecure_channel(url)
        self.stub = replica_pb2_grpc.ReplicaServiceStub(self.channel)

    def replica_create(self, size):
        return self.stub.ReplicaCreate(replica_pb2.ReplicaCreateRequest(size=size))

    def replica_open(self):
        return self.stub.ReplicaOpen(empty_pb2.Empty())
