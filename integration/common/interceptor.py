import collections
import grpc

class _ClientCallDetails(
        collections.namedtuple(
            '_ClientCallDetails',
            ('method', 'timeout', 'metadata', 'credentials')),
        grpc.ClientCallDetails):
    pass


# IdentityValidationInterceptor injects gRPC metadata into all outgoing gRPC
# calls. It only injects metadata that it has been instantiated with awareness
# of. Implementation details are derived from
# https://github.com/grpc/grpc/tree/master/examples/python/interceptors/headers
class IdentityValidationInterceptor(grpc.UnaryUnaryClientInterceptor):
    def __init__(self, volume_name, instance_name):
        self.volume_name = volume_name
        self.instance_name = instance_name

    def intercept_unary_unary(self, continuation, client_call_details, request):
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)
        if self.volume_name is not None:
            metadata.append(('volume-name', self.volume_name))
        if self.instance_name is not None:
            metadata.append(('instance-name', self.instance_name))
        client_call_details = _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials)
        return continuation(client_call_details, next(iter((request,))))
