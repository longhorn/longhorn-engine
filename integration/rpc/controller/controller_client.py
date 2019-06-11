import grpc

import controller_pb2
import controller_pb2_grpc
from google.protobuf import empty_pb2


class ControllerClient(object):
    def __init__(self, url):
        self.channel = grpc.insecure_channel(url)
        self.stub = controller_pb2_grpc.ControllerServiceStub(self.channel)

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
