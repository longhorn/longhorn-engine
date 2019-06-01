import grpc

import replica_pb2
import replica_pb2_grpc


class ReplicaClient(object):
    def __init__(self, url):
        self.channel = grpc.insecure_channel(url)
        self.stub = replica_pb2_grpc.ReplicaServiceStub(self.channel)

    def replica_create(self, size):
        return self.stub.ReplicaCreate(replica_pb2.ReplicaCreateRequest(size=size))
