import grpc

import replica_pb2
import replica_pb2_grpc
from google.protobuf import empty_pb2


class ReplicaClient(object):
    def __init__(self, address):
        self.address = address
        self.channel = grpc.insecure_channel(address)
        self.url = "tcp://" + self.address
        self.stub = replica_pb2_grpc.ReplicaServiceStub(self.channel)

    def replica_create(self, size):
        return self.stub.ReplicaCreate(
            replica_pb2.ReplicaCreateRequest(size=size)).replica

    def replica_delete(self):
        return self.stub.ReplicaDelete(empty_pb2.Empty())

    def replica_get(self):
        return self.stub.ReplicaGet(empty_pb2.Empty()).replica

    def replica_open(self):
        return self.stub.ReplicaOpen(empty_pb2.Empty()).replica

    def replica_close(self):
        return self.stub.ReplicaClose(empty_pb2.Empty()).replica

    def replica_reload(self):
        return self.stub.ReplicaReload(empty_pb2.Empty()).replica

    def replica_snapshot(self, name="", user_created=False,
                         created="", labels={}):
        return self.stub.ReplicaSnapshot(
            replica_pb2.ReplicaSnapshotRequest(
                name=name, user_created=user_created,
                created=created, labels=labels,
            )).replica

    def replica_expand(self, size):
        return self.stub.ReplicaExpand(
            replica_pb2.ReplicaExpandRequest(size=size)).replica

    def disk_remove(self, name, force=False):
        return self.stub.DiskRemove(
            replica_pb2.DiskRemoveRequest(name=name, force=force)).replica

    def disk_prepare_remove(self, name):
        return self.stub.DiskPrepareRemove(
            replica_pb2.DiskPrepareRemoveRequest(name=name))

    def disk_mark_as_removed(self, name):
        return self.stub.DiskMarkAsRemoved(
            replica_pb2.DiskMarkAsRemovedRequest(name=name)).replica

    def rebuilding_set(self, rebuilding):
        return self.stub.RebuildingSet(
            replica_pb2.RebuildingSetRequest(rebuilding=rebuilding)).replica
