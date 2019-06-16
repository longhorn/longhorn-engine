import common
import launcher
from launcher import LAUNCHER_NO_FRONTEND
from common import grpc_controller_no_frontend  # NOQA
from common import grpc_replica1, grpc_replica2  # NOQA
from common import open_replica
from common import VOLUME2_NAME
from common import get_blockdev, verify_read, verify_data


FRONTEND_TGT_BLOCKDEV = "tgt-blockdev"


def test_frontend_switch(grpc_controller_no_frontend,  # NOQA
                         grpc_replica1, grpc_replica2):  # NOQA

    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = grpc_controller_no_frontend.replica_list()
    assert len(replicas) == 0

    v = grpc_controller_no_frontend.volume_start(replicas=[
        common.REPLICA1,
        common.REPLICA2
    ])
    assert v.replicaCount == 2
    assert v.frontend == ""

    launcher.start_engine_frontend(FRONTEND_TGT_BLOCKDEV,
                                   url=LAUNCHER_NO_FRONTEND)
    v = grpc_controller_no_frontend.volume_get()
    assert v.frontend != ""

    dev = get_blockdev(volume=VOLUME2_NAME)

    data = common.random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, data)

    launcher.shutdown_engine_frontend(url=LAUNCHER_NO_FRONTEND)
    v = grpc_controller_no_frontend.volume_get()
    assert v.frontend != ""
    assert v.frontendState == "down"

    launcher.start_engine_frontend(FRONTEND_TGT_BLOCKDEV,
                                   url=LAUNCHER_NO_FRONTEND)
    v = grpc_controller_no_frontend.volume_get()
    assert v.frontend != ""
    assert v.frontendState == "up"

    dev = get_blockdev(volume=VOLUME2_NAME)
    verify_read(dev, data_offset, data)

    launcher.shutdown_engine_frontend(url=LAUNCHER_NO_FRONTEND)
