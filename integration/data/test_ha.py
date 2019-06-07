import cmd
import common
from common import controller, grpc_controller, grpc_replica1, grpc_replica2  # NOQA
from common import grpc_backing_replica1, grpc_backing_replica2  # NOQA
from common import prepare_backup_dir, BACKUP_DIR  # NOQA
from common import open_replica, get_blockdev, cleanup_replica
from common import verify_read, verify_data, verify_async, VOLUME_HEAD
from snapshot_tree import snapshot_tree_build, snapshot_tree_verify


def test_ha_single_replica_failure(controller, grpc_controller,  # NOQA
                                   grpc_replica1, grpc_replica2):  # NOQA
    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = grpc_controller.volume_start(replicas=[
        common.REPLICA1,
        common.REPLICA2
    ])
    assert v.replicaCount == 2

    replicas = controller.list_replica()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev()

    data = common.random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, data)

    cleanup_replica(grpc_replica2)

    verify_async(dev, 10, 128, 1)

    common.verify_replica_state(controller, 1, "ERR")

    verify_read(dev, data_offset, data)


def test_ha_single_replica_rebuild(controller, grpc_controller,  # NOQA
                                   grpc_replica1, grpc_replica2):  # NOQA
    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = grpc_controller.volume_start(replicas=[
        common.REPLICA1,
        common.REPLICA2
    ])
    assert v.replicaCount == 2

    replicas = controller.list_replica()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev()

    data = common.random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, data)

    # Cleanup replica2
    cleanup_replica(grpc_replica2)

    verify_async(dev, 10, 128, 1)

    common.verify_replica_state(controller, 1, "ERR")

    verify_read(dev, data_offset, data)

    controller.delete(replicas[1])

    # Rebuild replica2
    open_replica(grpc_replica2)
    cmd.add_replica(common.REPLICA2)

    verify_async(dev, 10, 128, 1)

    common.verify_replica_state(controller, 1, "RW")

    verify_read(dev, data_offset, data)

    # WORKAROUND for unable to remove the parent of volume head
    newsnap = cmd.snapshot_create()

    info = cmd.snapshot_info()
    assert len(info) == 3
    sysnap = info[newsnap]["parent"]
    assert info[sysnap]["parent"] == ""
    assert newsnap in info[sysnap]["children"]
    assert info[sysnap]["usercreated"] is False
    assert info[sysnap]["removed"] is False

    cmd.snapshot_purge()
    info = cmd.snapshot_info()
    assert len(info) == 2
    assert info[newsnap] is not None
    assert info[VOLUME_HEAD] is not None


def test_ha_double_replica_rebuild(controller, grpc_controller,  # NOQA
                                   grpc_replica1, grpc_replica2):  # NOQA
    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = grpc_controller.volume_start(replicas=[
        common.REPLICA1,
        common.REPLICA2
    ])
    assert v.replicaCount == 2

    replicas = controller.list_replica()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev()

    data1 = common.random_string(128)
    data1_offset = 1024
    verify_data(dev, data1_offset, data1)

    # Close replica2
    r2 = grpc_replica2.replica_get()
    assert r2.revisionCounter == 1
    grpc_replica2.replica_close()

    verify_async(dev, 10, 128, 1)

    common.verify_replica_state(controller, 1, "ERR")

    verify_read(dev, data1_offset, data1)

    data2 = common.random_string(128)
    data2_offset = 512
    verify_data(dev, data2_offset, data2)

    # Close replica1
    r1 = grpc_replica1.replica_get()
    assert r1.revisionCounter == 12  # 1 + 10 + 1
    grpc_replica1.replica_close()

    # Restart volume
    common.cleanup_controller(controller)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    # NOTE the order is reversed here
    v = grpc_controller.volume_start(replicas=[
        common.REPLICA2,
        common.REPLICA1
    ])
    assert v.replicaCount == 2

    # replica2 is out because of lower revision counter
    replicas = controller.list_replica()
    assert len(replicas) == 2
    assert replicas[0].mode == "ERR"
    assert replicas[1].mode == "RW"

    verify_read(dev, data1_offset, data1)
    verify_read(dev, data2_offset, data2)

    # Rebuild replica2
    r2 = grpc_replica2.replica_get()
    assert r2.revisionCounter == 1
    grpc_replica2.replica_close()

    controller.delete(replicas[0])

    cmd.add_replica(common.REPLICA2)

    verify_async(dev, 10, 128, 1)

    common.verify_replica_state(controller, 1, "RW")

    verify_read(dev, data1_offset, data1)
    verify_read(dev, data2_offset, data2)

    r1 = grpc_replica1.replica_get()
    r2 = grpc_replica2.replica_get()
    assert r1.revisionCounter == 22  # 1 + 10 + 1 + 10
    assert r2.revisionCounter == 22  # must be in sync with r1


def test_ha_revision_counter_consistency(controller, grpc_controller,  # NOQA
                                         grpc_replica1, grpc_replica2):  # NOQA
    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = grpc_controller.volume_start(replicas=[
        common.REPLICA1,
        common.REPLICA2
    ])
    assert v.replicaCount == 2

    replicas = controller.list_replica()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev()

    common.verify_async(dev, 10, 128, 100)

    r1 = grpc_replica1.replica_get()
    r2 = grpc_replica2.replica_get()
    # kernel can merge requests so backend may not receive 1000 writes
    assert r1.revisionCounter > 0
    assert r1.revisionCounter == r2.revisionCounter


def test_snapshot_tree_rebuild(controller, grpc_controller,  # NOQA
                               grpc_replica1, grpc_replica2):  # NOQA
    offset = 0
    length = 128

    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = grpc_controller.volume_start(replicas=[
        common.REPLICA1,
        common.REPLICA2
    ])
    assert v.replicaCount == 2

    replicas = controller.list_replica()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev()

    snap, snap_data = snapshot_tree_build(dev, offset, length)

    data = common.random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, data)

    # Cleanup replica2
    cleanup_replica(grpc_replica2)

    verify_async(dev, 10, 128, 1)

    common.verify_replica_state(controller, 1, "ERR")

    verify_read(dev, data_offset, data)

    controller.delete(replicas[1])

    # Rebuild replica2
    open_replica(grpc_replica2)
    cmd.add_replica(common.REPLICA2)

    verify_async(dev, 10, 128, 1)

    common.verify_replica_state(controller, 1, "RW")

    snapshot_tree_verify(dev, offset, length, snap, snap_data)


def test_ha_single_backing_replica_rebuild(controller, grpc_controller,  # NOQA
                                           grpc_backing_replica1,  # NOQA
                                           grpc_backing_replica2):  # NOQA
    prepare_backup_dir(BACKUP_DIR)
    open_replica(grpc_backing_replica1)
    open_replica(grpc_backing_replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = grpc_controller.volume_start(replicas=[
        common.BACKED_REPLICA1,
        common.BACKED_REPLICA2
    ])
    assert v.replicaCount == 2

    replicas = controller.list_replica()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev()

    data = common.random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, data)

    # Cleanup replica2
    cleanup_replica(grpc_backing_replica2)

    verify_async(dev, 10, 128, 1)

    common.verify_replica_state(controller, 1, "ERR")

    verify_read(dev, data_offset, data)

    controller.delete(replicas[1])

    # Rebuild replica2
    open_replica(grpc_backing_replica2)
    cmd.add_replica(common.BACKED_REPLICA2)

    verify_async(dev, 10, 128, 1)

    common.verify_replica_state(controller, 1, "RW")

    verify_read(dev, data_offset, data)

    # WORKAROUND for unable to remove the parent of volume head
    newsnap = cmd.snapshot_create()

    info = cmd.snapshot_info()
    assert len(info) == 3
    sysnap = info[newsnap]["parent"]
    assert info[sysnap]["parent"] == ""
    assert newsnap in info[sysnap]["children"]
    assert info[sysnap]["usercreated"] is False
    assert info[sysnap]["removed"] is False

    cmd.snapshot_purge()
    info = cmd.snapshot_info()
    assert len(info) == 2
    assert info[newsnap] is not None
    assert info[VOLUME_HEAD] is not None


def test_ha_remove_extra_disks(controller, grpc_controller,  # NOQA
                               grpc_replica1, grpc_replica2):  # NOQA
    prepare_backup_dir(BACKUP_DIR)
    open_replica(grpc_replica1)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = grpc_controller.volume_start(replicas=[
        common.REPLICA1,
    ])
    assert v.replicaCount == 1

    replicas = controller.list_replica()
    assert len(replicas) == 1
    assert replicas[0].mode == "RW"

    dev = get_blockdev()

    wasted_data = common.random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, wasted_data)

    # now replica1 contains extra data in a snapshot
    cmd.snapshot_create()

    common.cleanup_controller(controller)

    open_replica(grpc_replica2)
    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = grpc_controller.volume_start(replicas=[
        common.REPLICA2,
    ])
    assert v.replicaCount == 1

    replicas = controller.list_replica()
    assert len(replicas) == 1
    assert replicas[0].mode == "RW"

    dev = get_blockdev()

    data = common.random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, data)

    r1 = grpc_replica1.replica_reload()
    print(r1)

    cmd.add_replica(common.REPLICA1)

    verify_data(dev, data_offset, data)
