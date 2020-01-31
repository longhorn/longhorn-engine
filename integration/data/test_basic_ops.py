import random
import time
from os import path

import pytest

import common.cmd as cmd
from common.core import (  # NOQA
    get_dev, read_dev, write_dev,  # NOQA
    random_string, verify_data,
    open_replica,
)
from common.frontend import get_socket_path
from common.constants import (
    LONGHORN_DEV_DIR, PAGE_SIZE, SIZE,
    VOLUME_NAME, ENGINE_NAME,
)


def test_basic_rw(dev):  # NOQA
    for i in range(0, 10):
        base = random.randint(1, SIZE - PAGE_SIZE)
        offset = (base // PAGE_SIZE) * PAGE_SIZE
        length = base - offset
        data = random_string(length)
        verify_data(dev, offset, data)


def test_rw_with_metric(grpc_controller,  # NOQA
                        grpc_replica1, grpc_replica2):  # NOQA
    rw_dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    replies = grpc_controller.metric_get()
    # skip the first metric since its fields are 0
    next(replies).metric

    for i in range(0, 5):
        base = random.randint(1, SIZE - PAGE_SIZE)
        offset = (base // PAGE_SIZE) * PAGE_SIZE
        length = base - offset
        data = random_string(length)
        verify_data(rw_dev, offset, data)

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
    data = random_string(128)
    verify_data(dev, SIZE - 1 - 128, data)

    # out of bounds
    with pytest.raises(EnvironmentError) as err:
        write_dev(dev, SIZE, "1")
    assert 'No space left' in str(err.value)
    assert len(read_dev(dev, SIZE, 1)) == 0

    # normal writes to verify controller/replica survival
    test_basic_rw(dev)


def test_frontend_show(grpc_engine_manager, grpc_controller,  # NOQA
                       grpc_replica1, grpc_replica2):  # NOQA
    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    r1_url = grpc_replica1.url
    r2_url = grpc_replica2.url
    v = grpc_controller.volume_start(replicas=[r1_url, r2_url])

    ft = v.frontend
    if ft == "tgt":
        assert v.endpoint == path.join(LONGHORN_DEV_DIR, VOLUME_NAME)
    elif ft == "socket":
        assert v.endpoint == get_socket_path(VOLUME_NAME)
        engine = grpc_engine_manager.engine_get(ENGINE_NAME)
        assert engine.status.endpoint == path.join(LONGHORN_DEV_DIR,
                                                   VOLUME_NAME)

    info = cmd.info_get(grpc_controller.address)
    assert info["name"] == VOLUME_NAME
    assert info["endpoint"] == v.endpoint


# https://github.com/rancher/longhorn/issues/401
def test_cleanup_leftover_blockdev(grpc_controller,  # NOQA
                                   grpc_replica1, grpc_replica2):  # NOQA
    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    blockdev = path.join(LONGHORN_DEV_DIR, VOLUME_NAME)
    assert not path.exists(blockdev)
    open(blockdev, 'a').close()

    r1_url = grpc_replica1.url
    r2_url = grpc_replica2.url
    grpc_controller.volume_start(replicas=[r1_url, r2_url])

    info = cmd.info_get(grpc_controller.address)
    assert info["name"] == VOLUME_NAME
