import grpc

import controller_pb2
import controller_pb2_grpc
from google.protobuf import empty_pb2


class ControllerClient(object):
    def __init__(self, url):
        self.address = url
        self.channel = grpc.insecure_channel(url)
        self.stub = controller_pb2_grpc.ControllerServiceStub(self.channel)

    def volume_get(self):
        return self.stub.VolumeGet(empty_pb2.Empty())

    def volume_start(self, replicas):
        return self.stub.VolumeStart(controller_pb2.VolumeStartRequest(
            replicaAddresses=replicas,
        ))

    def volume_shutdown(self):
        return self.stub.VolumeShutdown(empty_pb2.Empty())

    def volume_snapshot(self, name="", labels={}):
        return self.stub.VolumeSnapshot(controller_pb2.VolumeSnapshotRequest(
            name=name, labels=labels
        )).name

    def volume_revert(self, name=""):
        return self.stub.VolumeRevert(controller_pb2.VolumeRevertRequest(
            name=name))

    def volume_expand(self, size):
        return self.stub.VolumeExpand(controller_pb2.VolumeExpandRequest(
            size=size))

    def replica_list(self):
        cr_list = self.stub.ReplicaList(empty_pb2.Empty()).replicas
        r_list = []
        for cr in cr_list:
            r_list.append(ControllerReplicaInfo(cr))
        return r_list

    def replica_get(self, address):
        return ControllerReplicaInfo(self.stub.ReplicaGet(
            controller_pb2.ReplicaAddress(address=address)))

    def replica_create(self, address):
        return ControllerReplicaInfo(
            self.stub.ReplicaCreate(
                controller_pb2.ReplicaAddress(address=address)))

    def replica_delete(self, address):
        return self.stub.ReplicaDelete(controller_pb2.ReplicaAddress(
            address=address))

    def replica_update(self, address, mode):
        return ControllerReplicaInfo(
            self.stub.ReplicaUpdate(
                controller_pb2.ControllerReplica(
                    address=controller_pb2.ReplicaAddress(address=address), mode=mode)))

    def metric_get(self):
        return self.stub.MetricGet(empty_pb2.Empty())

    def client_upgrade(self, address):
        self.address = address
        self.channel = grpc.insecure_channel(address)
        self.stub = controller_pb2_grpc.ControllerServiceStub(self.channel)

    def volume_frontend_start(self, frontend=""):
        return self.stub.VolumeFrontendStart(controller_pb2.VolumeFrontendStartRequest(
            frontend=frontend))

    def volume_frontend_shutdown(self):
        return self.stub.VolumeFrontendShutdown(empty_pb2.Empty())


class ControllerReplicaInfo(object):
    def __init__(self, cr):
        self.address = cr.address.address
        self.mode = controller_pb2.ReplicaMode.Name(cr.mode)
