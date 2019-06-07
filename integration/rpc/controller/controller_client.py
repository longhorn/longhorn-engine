import grpc

import controller_pb2
import controller_pb2_grpc


class ControllerClient(object):
    def __init__(self, url):
        self.channel = grpc.insecure_channel(url)
        self.stub = controller_pb2_grpc.ControllerServiceStub(self.channel)
