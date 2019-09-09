import pytest
import grpc

from common import (  # NOQA
    grpc_engine_manager, grpc_controller,  # NOQA
    grpc_fixed_dir_replica1, grpc_fixed_dir_replica2,  # NOQA
    grpc_extra_replica1, grpc_extra_replica2,  # NOQA
    get_dev, read_dev, write_dev,
    random_string, verify_data,
    wait_for_process_running,
    open_replica, cleanup_replica,
)
from setting import (
    SIZE, ENGINE_NAME,
    LONGHORN_BINARY, LONGHORN_UPGRADE_BINARY,
    INSTANCE_MANAGER_TYPE_ENGINE,
)


def test_upgrade(grpc_engine_manager,  # NOQA
                 grpc_controller,  # NOQA
                 grpc_fixed_dir_replica1, grpc_fixed_dir_replica2,  # NOQA
                 grpc_extra_replica1, grpc_extra_replica2):  # NOQA

    dev = get_dev(grpc_fixed_dir_replica1, grpc_fixed_dir_replica2,
                  grpc_controller)

    offset = 0
    length = 128

    data = random_string(length)
    verify_data(dev, offset, data)

    # both set pointed to the same volume underlying
    r1_url = grpc_fixed_dir_replica1.url
    r2_url = grpc_fixed_dir_replica2.url
    upgrade_r1_url = grpc_extra_replica1.url
    upgrade_r2_url = grpc_extra_replica2.url

    v = grpc_controller.volume_start(replicas=[r1_url, r2_url])
    assert v.replicaCount == 2

    upgrade_e = grpc_engine_manager.engine_upgrade(
        ENGINE_NAME, LONGHORN_UPGRADE_BINARY,
        SIZE, [upgrade_r1_url, upgrade_r2_url])
    assert upgrade_e.spec.binary == LONGHORN_UPGRADE_BINARY

    verify_data(dev, offset, data)

    grpc_controller.client_upgrade(upgrade_e.spec.listen)
    wait_for_process_running(grpc_engine_manager, ENGINE_NAME,
                             INSTANCE_MANAGER_TYPE_ENGINE)

    # cannot start with same binary
    with pytest.raises(grpc.RpcError):
        grpc_engine_manager.engine_upgrade(
            ENGINE_NAME, LONGHORN_UPGRADE_BINARY,
            SIZE, [r1_url, r2_url])
    verify_data(dev, offset, data)

    # cannot start with wrong replica, would trigger rollback
    with pytest.raises(grpc.RpcError):
        grpc_engine_manager.engine_upgrade(
            ENGINE_NAME, LONGHORN_UPGRADE_BINARY,
            SIZE, ["random"])
    verify_data(dev, offset, data)

    grpc_fixed_dir_replica1 = cleanup_replica(grpc_fixed_dir_replica1)
    grpc_fixed_dir_replica2 = cleanup_replica(grpc_fixed_dir_replica2)
    open_replica(grpc_fixed_dir_replica1)
    open_replica(grpc_fixed_dir_replica2)

    e = grpc_engine_manager.engine_upgrade(
        ENGINE_NAME, LONGHORN_BINARY, SIZE, [r1_url, r2_url])
    assert e.spec.binary == LONGHORN_BINARY

    verify_data(dev, offset, data)

    grpc_controller.client_upgrade(e.spec.listen)
    wait_for_process_running(grpc_engine_manager, ENGINE_NAME,
                             INSTANCE_MANAGER_TYPE_ENGINE)
