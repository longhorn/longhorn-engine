from common import (  # NOQA
    grpc_controller_no_frontend,  # NOQA
    grpc_replica1, grpc_replica2,  # NOQA
    open_replica, get_backend_replica_url,
    get_blockdev, random_string, verify_read, verify_data,
    start_engine_frontend, shutdown_engine_frontend,
)
from setting import (
    VOLUME_NO_FRONTEND_NAME, ENGINE_NO_FRONTEND_NAME,
)


def test_frontend_switch(grpc_controller_no_frontend,  # NOQA
                         grpc_replica1, grpc_replica2):  # NOQA

    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = grpc_controller_no_frontend.replica_list()
    assert len(replicas) == 0

    r1_url = get_backend_replica_url(grpc_replica1.address)
    r2_url = get_backend_replica_url(grpc_replica2.address)
    v = grpc_controller_no_frontend.volume_start(
        replicas=[r1_url, r2_url])
    assert v.name == VOLUME_NO_FRONTEND_NAME
    assert v.replicaCount == 2
    assert v.frontend == ""

    start_engine_frontend(ENGINE_NO_FRONTEND_NAME)

    dev = get_blockdev(volume=VOLUME_NO_FRONTEND_NAME)

    data = random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, data)

    shutdown_engine_frontend(ENGINE_NO_FRONTEND_NAME)

    start_engine_frontend(ENGINE_NO_FRONTEND_NAME)

    dev = get_blockdev(volume=VOLUME_NO_FRONTEND_NAME)
    verify_read(dev, data_offset, data)

    shutdown_engine_frontend(ENGINE_NO_FRONTEND_NAME)
