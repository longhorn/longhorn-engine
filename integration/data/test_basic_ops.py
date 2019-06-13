import random
import time
from os import path

import pytest

import cmd
import launcher
import common
import frontend
from common import dev  # NOQA
from common import PAGE_SIZE, SIZE  # NOQA
from common import controller, grpc_controller, read_dev, write_dev  # NOQA
from common import grpc_replica1, grpc_replica2  # NOQA


def test_basic_rw(dev):  # NOQA
    for i in range(0, 10):
        base = random.randint(1, SIZE - PAGE_SIZE)
        offset = (base / PAGE_SIZE) * PAGE_SIZE
        length = base - offset
        data = common.random_string(length)
        common.verify_data(dev, offset, data)


def test_rw_with_metric(controller, grpc_controller,  # NOQA
                        grpc_replica1, grpc_replica2):  # NOQA
    rw_dev = common.get_dev(grpc_replica1, grpc_replica2,
                            grpc_controller)

    replies = grpc_controller.metric_get()
    # skip the first metric since its fields are 0
    next(replies).metric

    for i in range(0, 5):
        base = random.randint(1, SIZE - PAGE_SIZE)
        offset = (base / PAGE_SIZE) * PAGE_SIZE
        length = base - offset
        data = common.random_string(length)
        common.verify_data(rw_dev, offset, data)

        while 1:
            try:
                metric = next(replies).metric
                # it's hard to confirm the accurate value of metric
                assert metric.readBandwidth != 0
                assert metric.writeBandwidth != 0
                assert metric.iOPS != 0
                break
            except StopIteration:
                time.sleep(1)


def test_beyond_boundary(dev):  # NOQA
    # check write at the boundary
    data = common.random_string(128)
    common.verify_data(dev, SIZE - 1 - 128, data)

    # out of bounds
    with pytest.raises(EnvironmentError) as err:
        write_dev(dev, SIZE, "1")
    assert 'No space left' in str(err.value)
    assert len(read_dev(dev, SIZE, 1)) == 0

    # normal writes to verify controller/replica survival
    test_basic_rw(dev)


def test_frontend_show(controller, grpc_controller,  # NOQA
                       grpc_replica1, grpc_replica2):  # NOQA
    common.open_replica(grpc_replica1)
    common.open_replica(grpc_replica2)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    v = grpc_controller.volume_start(replicas=[
        common.REPLICA1,
        common.REPLICA2
    ])

    ft = v.frontend
    if ft == "tgt" or ft == "tcmu":
        assert v.endpoint == path.join(common.LONGHORN_DEV_DIR,
                                       common.VOLUME_NAME)
    elif ft == "socket":
        assert v.endpoint == common.get_socket_path(common.VOLUME_NAME)
        launcher_info = launcher.info()
        assert launcher_info["endpoint"] == path.join(common.LONGHORN_DEV_DIR,
                                                      common.VOLUME_NAME)

    info = cmd.info()
    assert info["name"] == common.VOLUME_NAME
    assert info["endpoint"] == v.endpoint


# https://github.com/rancher/longhorn/issues/401
def test_cleanup_leftover_blockdev(controller, grpc_controller,  # NOQA
                                   grpc_replica1, grpc_replica2):  # NOQA
    common.open_replica(grpc_replica1)
    common.open_replica(grpc_replica2)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    blockdev = path.join(frontend.LONGHORN_DEV_DIR, common.VOLUME_NAME)
    assert not path.exists(blockdev)
    open(blockdev, 'a').close()

    grpc_controller.volume_start(replicas=[
        common.REPLICA1,
        common.REPLICA2
    ])

    info = cmd.info()
    assert info["name"] == common.VOLUME_NAME
