import random
import time
import os
from os import path

import pytest

import common.cmd as cmd
from common.core import (  # NOQA
    get_dev, read_dev, write_dev,  # NOQA
    random_string, verify_data,
    open_replica,
    get_dev_path, check_dev_existence,
)
from common.frontend import get_socket_path
from common.constants import (
    LONGHORN_DEV_DIR, PAGE_SIZE, SIZE,
    VOLUME_NAME, ENGINE_NAME
)


def test_device_creation(first_available_device,
                         grpc_controller_device_name_test,  # NOQA
                         grpc_replica1, grpc_replica2):  # NOQA
    block_device = get_dev(grpc_replica1, grpc_replica2,
                           grpc_controller_device_name_test)
    assert block_device
    check_dev_existence(VOLUME_NAME)
    longhorn_dev = get_dev_path(VOLUME_NAME)
    dev_info = os.stat(first_available_device)
    assert dev_info.st_rdev == os.stat(os.devnull).st_rdev
    assert dev_info.st_rdev != os.stat(longhorn_dev).st_rdev
    test_basic_rw(block_device)


def test_basic_rw(dev):  # NOQA
    for i in range(0, 10):
        base = random.randint(1, SIZE - PAGE_SIZE)
        offset = (base // PAGE_SIZE) * PAGE_SIZE
        length = base - offset
        data = random_string(length)
        verify_data(dev, offset, data)


def test_metrics(grpc_replica1, grpc_replica2, grpc_controller):  # NOQA
    rw_dev = get_dev(grpc_replica1, grpc_replica2,
                     grpc_controller)
    metrics = grpc_controller.metrics_get()
    metrics = metrics.metrics

    # No r/w IO
    assert metrics.readThroughput == 0
    assert metrics.writeThroughput == 0
    assert metrics.readIOPS == 0
    assert metrics.writeIOPS == 0

    # Create r/w IO
    base = random.randint(1, SIZE - PAGE_SIZE)
    offset = (base // PAGE_SIZE) * PAGE_SIZE
    length = base - offset
    data = random_string(length)
    verify_data(rw_dev, offset, data)

    # metrics update period is 1 second
    max_iters = 10
    for i in range(max_iters):
        try:
            metrics = grpc_controller.metrics_get()
            metrics = metrics.metrics

            # it's hard to confirm the accurate value of metric
            assert metrics.readThroughput != 0
            assert metrics.writeThroughput != 0
            assert metrics.readIOPS != 0
            assert metrics.writeIOPS != 0
            break
        except Exception as e:
            if i == max_iters - 1:
                raise e
            time.sleep(0.1)


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
    v = grpc_controller.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])

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
    grpc_controller.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])

    info = cmd.info_get(grpc_controller.address)
    assert info["name"] == VOLUME_NAME
