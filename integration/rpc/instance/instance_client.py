import grpc

from imrpc import instance_pb2
from imrpc import instance_pb2_grpc

from google.protobuf import empty_pb2


class InstanceClient(object):
    def __init__(self, url):
        self.address = url
        self.channel = grpc.insecure_channel(url)
        self.stub = instance_pb2_grpc.InstanceServiceStub(self.channel)

    def version_get(self):
        return self.stub.VersionGet(empty_pb2.Empty())

    def instance_create(self, backend_store_driver, name, type,
                        volume_name, size,
                        expose_required=False,
                        disk_name="", disk_uuid="",
                        port_count=0, port_args=[],
                        binary="", args=[], replica_address_map={},
                        frontend=""):
        process_instance_spec = instance_pb2.ProcessInstanceSpec(binary=binary,
                                                                 args=args)
        spdk_instance_spec = instance_pb2.SpdkInstanceSpec(
            replica_address_map=replica_address_map,
            disk_name=disk_name,
            disk_uuid=disk_uuid,
            size=size,
            expose_required=expose_required,
            frontend=frontend)
        return self.stub.InstanceCreate(instance_pb2.InstanceCreateRequest(
            spec=instance_pb2.InstanceSpec(
                backend_store_driver=backend_store_driver,
                name=name,
                type=type,
                volume_name=volume_name,
                port_count=port_count,
                port_args=port_args,
                process_instance_spec=process_instance_spec,
                spdk_instance_spec=spdk_instance_spec)))

    def instance_get(self, backend_store_driver, name, type):
        return self.stub.InstanceGet(instance_pb2.InstanceGetRequest(
            backend_store_driver=backend_store_driver,
            name=name,
            type=type))

    def instance_list(self):
        return self.stub.InstanceList(empty_pb2.Empty()).instances

    def instance_delete(self, backend_store_driver, name, type, disk_uuid="", cleanup_required=False): # NOQA
        return self.stub.InstanceDelete(instance_pb2.InstanceDeleteRequest(
            backend_store_driver=backend_store_driver,
            name=name,
            type=type,
            disk_uuid=disk_uuid,
            cleanup_required=cleanup_required))

    def instance_replace(self, backend_store_driver, name, type, volume_name,
                         port_count, port_args, binary, args,
                         replica_address_map, disk_name, disk_uuid,
                         size, expose_required, frontend,
                         terminate_signal="SIGHUP"):
        process_instance_spec = instance_pb2.ProcessInstanceSpec(binary, args)
        spdk_instance_spec = instance_pb2.SpdkInstanceSpec(
            replica_address_map=replica_address_map,
            disk_name=disk_name,
            disk_uuid=disk_uuid,
            size=size,
            expose_required=expose_required,
            frontend=frontend)
        instance_spec = self.stub.InstanceCreate(
            instance_pb2.InstanceCreateRequest(
                spec=instance_pb2.InstanceSpec(
                    backend_store_driver=backend_store_driver,
                    name=name,
                    type=type,
                    volume_name=volume_name,
                    port_count=port_count,
                    port_args=port_args,
                    process_instance_spec=process_instance_spec,
                    spdk_instance_spec=spdk_instance_spec)))

        return self.stub.InstanceReplace(instance_pb2.InstanceReplaceRequest(
            spec=instance_spec,
            terminate_signal=terminate_signal))
