import grpc

from imrpc import disk_pb2
from imrpc import disk_pb2_grpc # NOQA

from google.protobuf import empty_pb2


class DiskClient(object):
    def __init__(self, url):
        self.address = url
        self.channel = grpc.insecure_channel(url)
        self.stub = disk_pb2_grpc.DiskServiceStub(self.channel)

    def version_get(self):
        return self.stub.VersionGet(empty_pb2.Empty())

    def disk_create(self, disk_type, disk_name, disk_path, block_size):
        return self.stub.DiskCreate(disk_pb2.DiskCreateRequest(
            disk_type=disk_type,
            disk_name=disk_name,
            disk_path=disk_path,
            block_size=block_size))

    def disk_get(self, disk_type, disk_name, disk_path):
        return self.stub.DiskGet(disk_pb2.DiskGetRequest(
            disk_type=disk_type,
            disk_name=disk_name,
            disk_path=disk_path))

    def disk_delete(self, disk_type, disk_name, disk_uuid):
        return self.stub.DiskDelete(disk_pb2.DiskDeleteRequest(
            disk_type=disk_type,
            disk_name=disk_name,
            disk_uuid=disk_uuid))

    def disk_replica_instance_list(self, disk_type, disk_name):
        return self.stub.DiskReplicaInstanceList(
            disk_pb2.DiskReplicaInstanceListRequest(
                disk_type=disk_type,
                disk_name=disk_name))

    def disk_replica_instance_delete(self, disk_type, disk_name, disk_uuid,
                                     replica_instance_name):
        return self.stub.DiskReplicaInstanceDelete(
            disk_pb2.DiskReplicaInstanceDeleteRequest(
                disk_type=disk_type,
                disk_name=disk_name,
                disk_uuid=disk_uuid,
                replcia_instance_name=replica_instance_name))
