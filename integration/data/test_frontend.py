from common.core import (  # NOQA
    open_replica,
    get_blockdev, random_string, verify_read, verify_data,
)
from common.constants import (
    VOLUME_NO_FRONTEND_NAME,
    FRONTEND_TGT_BLOCKDEV,
    SIZE
)


def test_frontend_switch(grpc_controller_no_frontend,  # NOQA
                         grpc_replica1, grpc_replica2):  # NOQA

    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = grpc_controller_no_frontend.replica_list()
    assert len(replicas) == 0

    r1_url = grpc_replica1.url
    r2_url = grpc_replica2.url
    v = grpc_controller_no_frontend.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
    assert v.name == VOLUME_NO_FRONTEND_NAME
    assert v.replicaCount == 2
    assert v.frontend == ""

    grpc_controller_no_frontend.volume_frontend_start(FRONTEND_TGT_BLOCKDEV)

    dev = get_blockdev(volume=VOLUME_NO_FRONTEND_NAME)

    data = random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, data)

    grpc_controller_no_frontend.volume_frontend_shutdown()

    grpc_controller_no_frontend.volume_frontend_start(FRONTEND_TGT_BLOCKDEV)

    dev = get_blockdev(volume=VOLUME_NO_FRONTEND_NAME)
    verify_read(dev, data_offset, data)

    grpc_controller_no_frontend.volume_frontend_shutdown()
