import grpc

from imrpc import imrpc_pb2
from imrpc import imrpc_pb2_grpc

from google.protobuf import empty_pb2


class ProcessManagerClient(object):
    def __init__(self, url):
        self.address = url
        self.channel = grpc.insecure_channel(url)
        self.stub = imrpc_pb2_grpc.ProcessManagerServiceStub(self.channel)

    def version_get(self):
        return self.stub.VersionGet(empty_pb2.Empty())

    def process_create(self, name, binary, args, port_count=0, port_args=[]):
        if not name or not binary:
            raise Exception("missing parameter")

        return self.stub.ProcessCreate(imrpc_pb2.ProcessCreateRequest(
            spec=imrpc_pb2.ProcessSpec(
                name=name, binary=binary,
                args=args, port_count=port_count, port_args=port_args,
            )
        ))

    def process_get(self, name):
        if not name:
            raise Exception("missing parameter")

        return self.stub.ProcessGet(imrpc_pb2.ProcessGetRequest(name=name))

    def process_list(self):
        return self.stub.ProcessList(imrpc_pb2.ProcessListRequest()).processes

    def process_delete(self, name):
        if not name:
            raise Exception("missing parameter")

        return self.stub.ProcessDelete(imrpc_pb2.ProcessDeleteRequest(name=name))

    def process_replace(self, name, binary, args, port_count=1,
                        port_args=["--listen,localhost:"],
                        terminate_signal="SIGHUP"):
        if not name:
            raise Exception("missing parameter")

        return self.stub.ProcessReplace(imrpc_pb2.ProcessReplaceRequest(
            spec=imrpc_pb2.ProcessSpec(
                name=name, binary=binary,
                args=args, port_count=port_count, port_args=port_args,
            ),
            terminate_signal=terminate_signal,
        ))

