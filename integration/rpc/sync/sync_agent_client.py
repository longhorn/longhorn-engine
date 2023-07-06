import grpc

import common_pb2
import syncagent_pb2
import syncagent_pb2_grpc
from google.protobuf import empty_pb2

from common.interceptor import IdentityValidationInterceptor


class SyncAgentClient(object):
    def __init__(self, address, volume_name=None, instance_name=None):
        self.address = address
        channel = grpc.insecure_channel(address)
        # Default volume_name = instance_name = None disables identity
        # validation.
        interceptor = IdentityValidationInterceptor(volume_name, instance_name)
        self.channel = grpc.intercept_channel(channel, interceptor)
        self.stub = syncagent_pb2_grpc.SyncAgentServiceStub(self.channel)

    # Only two client methods have been implemented in service of 
    # test_validation_fails_with_client. Additional methods are TODO.

    def replica_rebuild_status(self):
        return self.stub.ReplicaRebuildStatus(empty_pb2.Empty())
    
    def sync_files(self, from_address, sync_file_info_tuples, fast_sync,
                   file_sync_http_client_timeout):

        sync_file_info_list = []
        for tuple in sync_file_info_tuples:
            info = common_pb2.SyncFileInfo(from_file_name=tuple[0],
                                           to_file_name=tuple[1],
                                           actual_size=tuple[2])
            sync_file_info_list.append(info)

        return self.stub.FilesSync(syncagent_pb2.FilesSyncRequest(
            from_address=from_address, to_host='localhost', 
            sync_file_info_list=sync_file_info_list,
            fast_sync=fast_sync, 
            file_sync_http_client_timeout=file_sync_http_client_timeout
        ))
    
    def file_send(self, from_file_name, host, port, fast_sync, 
                  file_sync_http_client_timeout):
        return self.stub.FileSend(syncagent_pb2.FileSendRequest(
            from_file_name=from_file_name, host=host, port=port, 
            fast_sync=fast_sync, 
            file_sync_http_client_timeout=file_sync_http_client_timeout
        ))
