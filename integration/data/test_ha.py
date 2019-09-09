import cmd
from common import (  # NOQA
    grpc_controller, grpc_replica1, grpc_replica2,  # NOQA
    grpc_backing_controller,  # NOQA
    grpc_backing_replica1, grpc_backing_replica2,  # NOQA
    open_replica, cleanup_replica, cleanup_controller,
    get_blockdev, prepare_backup_dir,
    random_string, verify_read, verify_data, verify_async,
    verify_replica_state, wait_for_purge_completion

)
from snapshot_tree import (
    snapshot_tree_build, snapshot_tree_verify
)
from setting import (
    VOLUME_NAME, VOLUME_BACKING_NAME,
    ENGINE_NAME, BACKUP_DIR, VOLUME_HEAD,
)


def test_ha_single_replica_failure(grpc_controller,  # NOQA
                                   grpc_replica1, grpc_replica2):  # NOQA
    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    r1_url = grpc_replica1.url
    r2_url = grpc_replica2.url
    v = grpc_controller.volume_start(replicas=[r1_url, r2_url])
    assert v.replicaCount == 2

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev(VOLUME_NAME)

    data = random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, data)

    cleanup_replica(grpc_replica2)

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_controller, 1, "ERR")

    verify_read(dev, data_offset, data)


def test_ha_single_replica_rebuild(grpc_controller,  # NOQA
                                   grpc_replica1, grpc_replica2):  # NOQA
    address = grpc_controller.address

    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    r1_url = grpc_replica1.url
    r2_url = grpc_replica2.url
    v = grpc_controller.volume_start(replicas=[r1_url, r2_url])
    assert v.replicaCount == 2

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev(VOLUME_NAME)

    data = random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, data)

    # Cleanup replica2
    cleanup_replica(grpc_replica2)

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_controller, 1, "ERR")

    verify_read(dev, data_offset, data)

    grpc_controller.replica_delete(replicas[1].address)

    # Rebuild replica2
    open_replica(grpc_replica2)
    cmd.add_replica(address, r2_url)

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_controller, 1, "RW")

    verify_read(dev, data_offset, data)

    # WORKAROUND for unable to remove the parent of volume head
    newsnap = cmd.snapshot_create(address)

    info = cmd.snapshot_info(address)
    assert len(info) == 3
    sysnap = info[newsnap]["parent"]
    assert info[sysnap]["parent"] == ""
    assert newsnap in info[sysnap]["children"]
    assert info[sysnap]["usercreated"] is False
    assert info[sysnap]["removed"] is False

    cmd.snapshot_purge(address)
    wait_for_purge_completion(address)

    info = cmd.snapshot_info(address)
    assert len(info) == 2
    assert info[newsnap] is not None
    assert info[VOLUME_HEAD] is not None


def test_ha_double_replica_rebuild(grpc_controller,  # NOQA
                                   grpc_replica1, grpc_replica2):  # NOQA
    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    r1_url = grpc_replica1.url
    r2_url = grpc_replica2.url
    v = grpc_controller.volume_start(replicas=[r1_url, r2_url])
    assert v.name == VOLUME_NAME
    assert v.replicaCount == 2

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev(VOLUME_NAME)

    data1 = random_string(128)
    data1_offset = 1024
    verify_data(dev, data1_offset, data1)

    # Close replica2
    r2 = grpc_replica2.replica_get()
    assert r2.revisionCounter == 1
    grpc_replica2.replica_close()

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_controller, 1, "ERR")

    verify_read(dev, data1_offset, data1)

    data2 = random_string(128)
    data2_offset = 512
    verify_data(dev, data2_offset, data2)

    # Close replica1
    r1 = grpc_replica1.replica_get()
    assert r1.revisionCounter == 12  # 1 + 10 + 1
    grpc_replica1.replica_close()

    # Restart volume
    cleanup_controller(grpc_controller)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    # NOTE the order is reversed here
    r1_url = grpc_replica1.url
    r2_url = grpc_replica2.url
    v = grpc_controller.volume_start(replicas=[r2_url, r1_url])
    assert v.replicaCount == 2

    # replica2 is out because of lower revision counter
    replicas = grpc_controller.replica_list()
    assert len(replicas) == 2
    assert replicas[0].mode == "ERR"
    assert replicas[1].mode == "RW"

    verify_read(dev, data1_offset, data1)
    verify_read(dev, data2_offset, data2)

    # Rebuild replica2
    r2 = grpc_replica2.replica_get()
    assert r2.revisionCounter == 1
    grpc_replica2.replica_close()

    grpc_controller.replica_delete(replicas[0].address)

    cmd.add_replica(grpc_controller.address, r2_url)

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_controller, 1, "RW")

    verify_read(dev, data1_offset, data1)
    verify_read(dev, data2_offset, data2)

    r1 = grpc_replica1.replica_get()
    r2 = grpc_replica2.replica_get()
    assert r1.revisionCounter == 22  # 1 + 10 + 1 + 10
    assert r2.revisionCounter == 22  # must be in sync with r1


def test_ha_revision_counter_consistency(grpc_controller,  # NOQA
                                         grpc_replica1, grpc_replica2):  # NOQA
    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    r1_url = grpc_replica1.url
    r2_url = grpc_replica2.url
    v = grpc_controller.volume_start(replicas=[r1_url, r2_url])
    assert v.name == VOLUME_NAME
    assert v.replicaCount == 2

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev(VOLUME_NAME)

    verify_async(dev, 10, 128, 100)

    r1 = grpc_replica1.replica_get()
    r2 = grpc_replica2.replica_get()
    # kernel can merge requests so backend may not receive 1000 writes
    assert r1.revisionCounter > 0
    assert r1.revisionCounter == r2.revisionCounter


def test_snapshot_tree_rebuild(grpc_controller,  # NOQA
                               grpc_replica1, grpc_replica2):  # NOQA
    address = grpc_controller.address

    offset = 0
    length = 128

    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    r1_url = grpc_replica1.url
    r2_url = grpc_replica2.url
    v = grpc_controller.volume_start(replicas=[r1_url, r2_url])
    assert v.name == VOLUME_NAME
    assert v.replicaCount == 2

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev(VOLUME_NAME)

    snap, snap_data = snapshot_tree_build(dev, address, ENGINE_NAME,
                                          offset, length)

    data = random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, data)

    # Cleanup replica2
    cleanup_replica(grpc_replica2)

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_controller, 1, "ERR")

    verify_read(dev, data_offset, data)

    grpc_controller.replica_delete(replicas[1].address)

    # Rebuild replica2
    open_replica(grpc_replica2)
    cmd.add_replica(address, r2_url)

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_controller, 1, "RW")

    snapshot_tree_verify(dev, address, ENGINE_NAME,
                         offset, length, snap, snap_data)


def test_ha_single_backing_replica_rebuild(grpc_backing_controller,  # NOQA
                                           grpc_backing_replica1,  # NOQA
                                           grpc_backing_replica2):  # NOQA
    address = grpc_backing_controller.address

    prepare_backup_dir(BACKUP_DIR)
    open_replica(grpc_backing_replica1)
    open_replica(grpc_backing_replica2)

    replicas = grpc_backing_controller.replica_list()
    assert len(replicas) == 0

    r1_url = grpc_backing_replica1.url
    r2_url = grpc_backing_replica2.url
    v = grpc_backing_controller.volume_start(
        replicas=[r1_url, r2_url])
    assert v.replicaCount == 2
    assert v.name == VOLUME_BACKING_NAME

    replicas = grpc_backing_controller.replica_list()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev(VOLUME_BACKING_NAME)

    data = random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, data)

    # Cleanup replica2
    cleanup_replica(grpc_backing_replica2)

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_backing_controller, 1, "ERR")

    verify_read(dev, data_offset, data)

    grpc_backing_controller.replica_delete(replicas[1].address)

    # Rebuild replica2
    open_replica(grpc_backing_replica2)
    cmd.add_replica(address, r2_url)

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_backing_controller, 1, "RW")

    verify_read(dev, data_offset, data)

    # WORKAROUND for unable to remove the parent of volume head
    newsnap = cmd.snapshot_create(address)

    info = cmd.snapshot_info(address)
    assert len(info) == 3
    sysnap = info[newsnap]["parent"]
    assert info[sysnap]["parent"] == ""
    assert newsnap in info[sysnap]["children"]
    assert info[sysnap]["usercreated"] is False
    assert info[sysnap]["removed"] is False

    cmd.snapshot_purge(address)
    wait_for_purge_completion(address)

    info = cmd.snapshot_info(address)
    assert len(info) == 2
    assert info[newsnap] is not None
    assert info[VOLUME_HEAD] is not None


def test_ha_remove_extra_disks(grpc_controller,  # NOQA
                               grpc_replica1, grpc_replica2):  # NOQA
    address = grpc_controller.address

    prepare_backup_dir(BACKUP_DIR)
    open_replica(grpc_replica1)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    r1_url = grpc_replica1.url
    v = grpc_controller.volume_start(replicas=[r1_url])
    assert v.name == VOLUME_NAME
    assert v.replicaCount == 1

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 1
    assert replicas[0].mode == "RW"

    dev = get_blockdev(VOLUME_NAME)

    wasted_data = random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, wasted_data)

    # now replica1 contains extra data in a snapshot
    cmd.snapshot_create(address)

    cleanup_controller(grpc_controller)

    open_replica(grpc_replica2)
    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    r2_url = grpc_replica2.url
    v = grpc_controller.volume_start(replicas=[r2_url])
    assert v.name == VOLUME_NAME
    assert v.replicaCount == 1

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 1
    assert replicas[0].mode == "RW"

    dev = get_blockdev(VOLUME_NAME)

    data = random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, data)

    r1 = grpc_replica1.replica_reload()
    print(r1)

    cmd.add_replica(address, r1_url)

    verify_data(dev, data_offset, data)
