import grpc

import rpc_pb2
import rpc_pb2_grpc
from google.protobuf import empty_pb2


class EngineManagerClient(object):
    def __init__(self, url):
        self.address = url
        self.channel = grpc.insecure_channel(url)
        self.stub = rpc_pb2_grpc.EngineManagerServiceStub(self.channel)

    def engine_create(self, name, volume_name, binary, listen, listen_ip,
                   size, frontend, replicas, backends=["tcp"]):
        if not name or not volume_name or not binary:
            raise Exception("missing parameter")

        if not listen and not listen_ip:
            raise Exception("missing parameter")

        return self.stub.EngineCreate(rpc_pb2.EngineCreateRequest(
            spec=rpc_pb2.EngineSpec(
                name=name, volume_name=volume_name, binary=binary,
                listen=listen, listen_ip=listen_ip, size=size,
                frontend=frontend, backends=backends, replicas=replicas,
            )))

    def engine_get(self, name):
        if not name:
            raise Exception("missing parameter name")

        return self.stub.EngineGet(rpc_pb2.EngineRequest(name=name))

    def engine_list(self):
        return self.stub.EngineList(empty_pb2.Empty()).engines

    def engine_upgrade(self, name, binary, size, replicas):

        if not name or not binary:
            raise Exception("missing parameter")

        return self.stub.EngineUpgrade(rpc_pb2.EngineUpgradeRequest(
            spec=rpc_pb2.EngineSpec(
                name=name, volume_name="", binary=binary,
                listen="", listen_ip="", size=size,
                frontend="", backends="", replicas=replicas,
            )))

    def engine_delete(self, name):
        if not name:
            raise Exception("missing parameter name")

        return self.stub.EngineDelete(rpc_pb2.EngineRequest(name=name))

    def frontend_start(self, name, frontend):
        if not name or not frontend:
            raise Exception("missing parameter")

        return self.stub.FrontendStart(rpc_pb2.FrontendStartRequest(
            name=name, frontend=frontend))

    def frontend_shutdown(self, name):
        if not name:
            raise Exception("missing parameter name")

        return self.stub.FrontendShutdown(rpc_pb2.EngineRequest(
            name=name))

    def frontend_start_callback(self, name):
        if not name:
            raise Exception("missing parameter name")

        return self.stub.FrontendStartCallback(
            rpc_pb2.EngineRequest(name=name))

    def frontend_shutdown_callback(self, name):
        if not name:
            raise Exception("missing parameter name")

        return self.stub.FrontendShutdownCallback(
            rpc_pb2.EngineRequest(name=name))
