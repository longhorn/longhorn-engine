import random
import os
import json

import common.cmd as cmd

from common.core import (  # NOQA
    open_replica, cleanup_replica, cleanup_controller,
    get_dev, get_blockdev, prepare_backup_dir,
    random_string, verify_read, verify_data, verify_async,
    verify_replica_state, wait_for_purge_completion,
    Snapshot, Data, random_length,
    expand_volume_with_frontend,
    wait_and_check_volume_expansion,
    wait_for_volume_expansion,
    wait_for_rebuild_complete, verify_replica_mode,

)

from data.snapshot_tree import (
    snapshot_tree_build, snapshot_tree_verify
)

from common.constants import (
    VOLUME_NAME, VOLUME_BACKING_NAME,
    ENGINE_NAME, BACKUP_DIR, VOLUME_HEAD,
    PAGE_SIZE, SIZE, EXPANDED_SIZE,
    EXPANDED_SIZE_STR,
    FIXED_REPLICA_PATH1, FIXED_REPLICA_PATH2,
    FRONTEND_TGT_BLOCKDEV,
    REPLICA_META_FILE_NAME,
    EXPANSION_DISK_TMP_META_NAME, EXPANSION_DISK_NAME,
)


def test_ha_single_replica_failure(grpc_controller,  # NOQA
                                   grpc_replica1, grpc_replica2):  # NOQA
    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    r1_url = grpc_replica1.url
    r2_url = grpc_replica2.url
    v = grpc_controller.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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

    verify_replica_state(grpc_controller,
                         r2_url, "ERR")

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
    v = grpc_controller.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
    assert v.replicaCount == 2

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev(VOLUME_NAME)

    data = random_string(128)
    data_offset = 1024
    verify_data(dev, data_offset, data)

    # Clean up replica2
    cleanup_replica(grpc_replica2)

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_controller,
                         r2_url, "ERR")

    verify_read(dev, data_offset, data)

    grpc_controller.replica_delete(replicas[1].address)

    # Rebuild replica2
    open_replica(grpc_replica2)
    cmd.add_replica(address, r2_url)
    wait_for_rebuild_complete(address)

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_controller,
                         r2_url, "RW")

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
    v = grpc_controller.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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
    assert r2.revision_counter == 1
    grpc_replica2.replica_close()

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_controller,
                         r2_url, "ERR")

    verify_read(dev, data1_offset, data1)

    data2 = random_string(128)
    data2_offset = 512
    verify_data(dev, data2_offset, data2)

    # Close replica1
    r1 = grpc_replica1.replica_get()
    assert r1.revision_counter == 12  # 1 + 10 + 1
    grpc_replica1.replica_close()

    # Restart volume
    cleanup_controller(grpc_controller)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    # NOTE the order is reversed here
    r1_url = grpc_replica1.url
    r2_url = grpc_replica2.url
    v = grpc_controller.volume_start(
        SIZE, SIZE, replicas=[r2_url, r1_url])
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
    assert r2.revision_counter == 1
    grpc_replica2.replica_close()

    grpc_controller.replica_delete(replicas[0].address)

    cmd.add_replica(grpc_controller.address, r2_url)
    wait_for_rebuild_complete(grpc_controller.address)

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_controller,
                         r2_url, "RW")

    verify_read(dev, data1_offset, data1)
    verify_read(dev, data2_offset, data2)

    r1 = grpc_replica1.replica_get()
    r2 = grpc_replica2.replica_get()
    assert r1.revision_counter == 22  # 1 + 10 + 1 + 10
    assert r2.revision_counter == 22  # must be in sync with r1


def test_ha_revision_counter_consistency(grpc_controller,  # NOQA
                                         grpc_replica1, grpc_replica2):  # NOQA
    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    r1_url = grpc_replica1.url
    r2_url = grpc_replica2.url
    v = grpc_controller.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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
    assert r1.revision_counter > 0
    assert r1.revision_counter == r2.revision_counter


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
    v = grpc_controller.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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

    # Clean up replica2
    cleanup_replica(grpc_replica2)

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_controller,
                         r2_url, "ERR")

    verify_read(dev, data_offset, data)

    grpc_controller.replica_delete(replicas[1].address)

    # Rebuild replica2
    open_replica(grpc_replica2)
    cmd.add_replica(address, r2_url)
    wait_for_rebuild_complete(address)

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_controller,
                         r2_url, "RW")

    snapshot_tree_verify(dev, address, ENGINE_NAME,
                         offset, length, snap, snap_data)


def test_ha_single_backing_qcow2_replica_rebuild(grpc_backing_controller, grpc_backing_qcow2_replica1, grpc_backing_qcow2_replica2):  # NOQA
    ha_single_backing_replica_rebuild_test(grpc_backing_controller,
                                           grpc_backing_qcow2_replica1,
                                           grpc_backing_qcow2_replica2)


def test_ha_single_backing_raw_replica_rebuild(grpc_backing_controller, grpc_backing_raw_replica1, grpc_backing_raw_replica2):  # NOQA
    ha_single_backing_replica_rebuild_test(grpc_backing_controller,
                                           grpc_backing_raw_replica1,
                                           grpc_backing_raw_replica2)


def ha_single_backing_replica_rebuild_test(grpc_backing_controller, grpc_backing_replica1, grpc_backing_replica2):  # NOQA
    address = grpc_backing_controller.address

    prepare_backup_dir(BACKUP_DIR)
    open_replica(grpc_backing_replica1)
    open_replica(grpc_backing_replica2)

    replicas = grpc_backing_controller.replica_list()
    assert len(replicas) == 0

    r1_url = grpc_backing_replica1.url
    r2_url = grpc_backing_replica2.url
    v = grpc_backing_controller.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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

    # Clean up replica2
    cleanup_replica(grpc_backing_replica2)

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_backing_controller,
                         r2_url, "ERR")

    verify_read(dev, data_offset, data)

    grpc_backing_controller.replica_delete(replicas[1].address)

    # Rebuild replica2
    open_replica(grpc_backing_replica2)
    cmd.add_replica(address, r2_url)
    wait_for_rebuild_complete(address)

    verify_async(dev, 10, 128, 1)

    verify_replica_state(grpc_backing_controller,
                         r2_url, "RW")

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
    v = grpc_controller.volume_start(
        SIZE, SIZE, replicas=[r1_url])
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
    v = grpc_controller.volume_start(
        SIZE, SIZE, replicas=[r2_url])
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
    wait_for_rebuild_complete(address)

    verify_data(dev, data_offset, data)


def test_expansion_with_rebuild(grpc_controller,  # NOQA
                                grpc_replica1, grpc_replica2):  # NOQA
    address = grpc_controller.address
    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    # the default size is 4MB, will expand it to 8MB
    address = grpc_controller.address
    zero_char = b'\x00'.decode('utf-8')
    original_data = zero_char * SIZE

    # write the data to the original part then do expansion
    data1_len = random_length(PAGE_SIZE)
    data1 = Data(random.randrange(0, SIZE-2*PAGE_SIZE, PAGE_SIZE),
                 data1_len, random_string(data1_len))
    snap1 = Snapshot(dev, data1, address)

    expand_volume_with_frontend(grpc_controller, EXPANDED_SIZE)
    wait_and_check_volume_expansion(
        grpc_controller, EXPANDED_SIZE)

    snap1.verify_data()
    assert \
        dev.readat(0, SIZE) == \
        original_data[0:data1.offset] + data1.content + \
        original_data[data1.offset+data1.length:]
    assert dev.readat(SIZE, SIZE) == zero_char*SIZE

    # write the data to both the original part and the expanded part
    data2_len = random_length(PAGE_SIZE)
    data2 = Data(SIZE-PAGE_SIZE,
                 data2_len, random_string(data2_len))
    snap2 = Snapshot(dev, data2, address)
    data3_len = random_length(PAGE_SIZE)
    data3 = Data(random.randrange(SIZE, EXPANDED_SIZE-PAGE_SIZE, PAGE_SIZE),
                 data3_len, random_string(data3_len))
    snap3 = Snapshot(dev, data3, address)
    snap1.verify_data()
    snap2.verify_data()
    snap3.verify_data()
    assert \
        dev.readat(SIZE, SIZE) == zero_char*(data3.offset-SIZE) + \
        data3.content + zero_char*(EXPANDED_SIZE-data3.offset-data3.length)

    # Clean up replica2
    cleanup_replica(grpc_replica2)
    verify_replica_state(grpc_controller,
                         grpc_replica2.address, "ERR")
    grpc_controller.replica_delete(replicas[1].address)

    # Rebuild replica2.
    open_replica(grpc_replica2)
    # The newly opened replica2 will be expanded automatically
    cmd.add_replica(address, grpc_replica2.url)
    wait_for_rebuild_complete(address)
    verify_replica_state(grpc_controller,
                         grpc_replica2.address, "RW")

    # Clean up replica1 then check if the rebuilt replica2 works fine
    cleanup_replica(grpc_replica1)
    verify_replica_state(grpc_controller,
                         grpc_replica1.address, "ERR")
    grpc_controller.replica_delete(replicas[0].address)

    assert \
        dev.readat(0, SIZE) == \
        original_data[0:data1.offset] + data1.content + \
        original_data[data1.offset+data1.length:data2.offset] + \
        data2.content + \
        original_data[data2.offset+data2.length:]
    assert \
        dev.readat(SIZE, SIZE) == zero_char*(data3.offset-SIZE) + \
        data3.content + zero_char*(EXPANDED_SIZE-data3.offset-data3.length)

    data4_len = random_length(PAGE_SIZE)
    data4 = Data(data1.offset,
                 data4_len, random_string(data4_len))
    snap4 = Snapshot(dev, data4, address)
    snap4.verify_data()


def test_expansion_rollback_with_rebuild(
        grpc_controller, grpc_fixed_dir_replica1, grpc_fixed_dir_replica2):  # NOQA
    """
    The test flow:
    1. Write random data into the block device.
    2. Create the 1st snapshot.
    3. Create an empty directory using the tmp meta file path of
       the expansion disk for each replica.
       This will fail the following expansion and trigger expansion rollback.
    4. Try to expand the volume but fails. Then the automatic rollback will
       be applied implicitly.
    5. Check the volume status and if there are leftovers of
       the failed expansion.
    6. Check if the volume is still usable by r/w data,
       then create the 2nd snapshot.
    7. Retry expansion. It should succeed.
    8. Verify the data and try data r/w.
    9. Delete then rebuild the replica2.
       Then rebuilt replica2 will be expanded automatically.
    10. Delete the replica1 then check if the rebuilt replica2 works fine.
    """
    address = grpc_controller.address
    r1_url = grpc_fixed_dir_replica1.address
    r2_url = grpc_fixed_dir_replica2.address
    dev = get_dev(grpc_fixed_dir_replica1, grpc_fixed_dir_replica2,
                  grpc_controller)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    # the default size is 4MB, will expand it to 8MB
    zero_char = b'\x00'.decode('utf-8')
    original_data = zero_char * SIZE

    # write the data to the original part then do expansion
    data1_len = random_length(PAGE_SIZE)
    data1 = Data(random.randrange(0, SIZE-2*PAGE_SIZE, PAGE_SIZE),
                 data1_len, random_string(data1_len))
    snap1 = Snapshot(dev, data1, address)

    # use the tmp meta file path of expansion disks to create empty directories
    # so that the expansion disk meta data update will fail.
    # Then expansion will fail and the rollback will be triggered.
    disk_meta_tmp_1 = os.path.join(
        FIXED_REPLICA_PATH1, EXPANSION_DISK_TMP_META_NAME)
    disk_meta_tmp_2 = os.path.join(
        FIXED_REPLICA_PATH2, EXPANSION_DISK_TMP_META_NAME)
    os.mkdir(disk_meta_tmp_1)
    os.mkdir(disk_meta_tmp_2)

    # All replicas' expansion will fail
    # then engine will do rollback automatically
    grpc_controller.volume_frontend_shutdown()
    grpc_controller.volume_expand(EXPANDED_SIZE)
    wait_for_volume_expansion(grpc_controller, SIZE)
    grpc_controller.volume_frontend_start(FRONTEND_TGT_BLOCKDEV)

    # Expansion should fail but the expansion rollback should succeed
    volume_info = grpc_controller.volume_get()
    assert volume_info.last_expansion_error != ""
    assert volume_info.last_expansion_failed_at != ""
    verify_replica_state(grpc_controller, r1_url, "RW")
    verify_replica_state(grpc_controller, r2_url, "RW")

    # The invalid disk and head will be cleaned up automatically
    # after the rollback
    expansion_disk_1 = os.path.join(FIXED_REPLICA_PATH1, EXPANSION_DISK_NAME)
    expansion_disk_2 = os.path.join(FIXED_REPLICA_PATH2, EXPANSION_DISK_NAME)
    assert not os.path.exists(expansion_disk_1)
    assert not os.path.exists(expansion_disk_2)
    assert not os.path.exists(disk_meta_tmp_1)
    assert not os.path.exists(disk_meta_tmp_2)
    # The meta info file should keep unchanged
    replica_meta_file_1 = os.path.join(FIXED_REPLICA_PATH1,
                                       REPLICA_META_FILE_NAME)
    replica_meta_file_2 = os.path.join(FIXED_REPLICA_PATH2,
                                       REPLICA_META_FILE_NAME)
    with open(replica_meta_file_1) as f:
        replica_meta_1 = json.load(f)
    assert replica_meta_1["Size"] == SIZE
    with open(replica_meta_file_2) as f:
        replica_meta_2 = json.load(f)
    assert replica_meta_2["Size"] == SIZE

    # try to check then write new data
    snap1.verify_data()
    data2_len = random_length(PAGE_SIZE)
    data2 = Data(SIZE-PAGE_SIZE,
                 data2_len, random_string(data2_len))
    snap2 = Snapshot(dev, data2, address)

    # Retry expansion
    expand_volume_with_frontend(grpc_controller, EXPANDED_SIZE)
    wait_and_check_volume_expansion(
        grpc_controller, EXPANDED_SIZE)
    with open(replica_meta_file_1) as f:
        replica_meta_1 = json.load(f)
    assert replica_meta_1["Size"] == EXPANDED_SIZE
    with open(replica_meta_file_2) as f:
        replica_meta_2 = json.load(f)
    assert replica_meta_2["Size"] == EXPANDED_SIZE

    assert os.path.exists(expansion_disk_1)
    assert os.path.exists(expansion_disk_2)

    snap1.verify_data()
    snap2.verify_data()
    assert dev.readat(SIZE, SIZE) == zero_char*SIZE

    data3_len = random_length(PAGE_SIZE)
    data3 = Data(random.randrange(SIZE, EXPANDED_SIZE-PAGE_SIZE, PAGE_SIZE),
                 data3_len, random_string(data3_len))
    snap3 = Snapshot(dev, data3, address)
    snap1.verify_data()
    snap2.verify_data()
    snap3.verify_data()
    assert \
        dev.readat(SIZE, SIZE) == zero_char*(data3.offset-SIZE) + \
        data3.content + zero_char*(EXPANDED_SIZE-data3.offset-data3.length)

    # Delete replica2
    cleanup_replica(grpc_fixed_dir_replica2)
    verify_replica_state(grpc_controller, r2_url, "ERR")
    grpc_controller.replica_delete(replicas[1].address)

    # Rebuild replica2.
    open_replica(grpc_fixed_dir_replica2)
    # The newly opened replica2 will be expanded automatically
    cmd.add_replica(address, grpc_fixed_dir_replica2.url)
    wait_for_rebuild_complete(address)
    verify_replica_state(grpc_controller, r2_url, "RW")

    # Clean up replica1 then check if the rebuilt replica2 works fine
    cleanup_replica(grpc_fixed_dir_replica1)
    verify_replica_state(grpc_controller, r1_url, "ERR")
    grpc_controller.replica_delete(replicas[0].address)

    assert \
        dev.readat(0, SIZE) == \
        original_data[0:data1.offset] + data1.content + \
        original_data[data1.offset+data1.length:data2.offset] + \
        data2.content + \
        original_data[data2.offset+data2.length:]
    assert \
        dev.readat(SIZE, SIZE) == zero_char*(data3.offset-SIZE) + \
        data3.content + zero_char*(EXPANDED_SIZE-data3.offset-data3.length)

    data4_len = random_length(PAGE_SIZE)
    data4 = Data(data1.offset,
                 data4_len, random_string(data4_len))
    snap4 = Snapshot(dev, data4, address)
    snap4.verify_data()


def test_single_replica_expansion_failed(
        grpc_controller, grpc_fixed_dir_replica1, grpc_fixed_dir_replica2):  # NOQA
    """
    The test flow:
    1. Write random data into the block device.
    2. Create the 1st snapshot.
    3. Create an empty directory using the tmp meta file path of
       the expansion disk for replica1.
    4. Try to expand the volume. replica1 will be directly marked as ERR state.
       Finally the volume expansion should succeed.
    5. Check the volume status, and if the expanded volume works fine:
       r/w data then create the 2nd snapshot.
    6. Rebuild replica1 and check the replica1 is expanded automatically.
    7. Delete replica2 then check if the rebuilt replica1 works fine.
    """
    address = grpc_controller.address
    r1_url = grpc_fixed_dir_replica1.address
    r2_url = grpc_fixed_dir_replica2.address
    dev = get_dev(grpc_fixed_dir_replica1, grpc_fixed_dir_replica2,
                  grpc_controller)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    # the default size is 4MB, will expand it to 8MB
    zero_char = b'\x00'.decode('utf-8')

    # write the data to the original part then do expansion
    data1_len = random_length(PAGE_SIZE)
    data1 = Data(random.randrange(0, SIZE-2*PAGE_SIZE, PAGE_SIZE),
                 data1_len, random_string(data1_len))
    snap1 = Snapshot(dev, data1, address)

    disk_meta_tmp_1 = os.path.join(
        FIXED_REPLICA_PATH1, EXPANSION_DISK_TMP_META_NAME)
    os.mkdir(disk_meta_tmp_1)

    # replica1 will fail to expand the size,
    # then engine will directly mark it as ERR state.
    # Finally, The volume expansion should succeed since replica2 works fine.
    grpc_controller.volume_frontend_shutdown()
    grpc_controller.volume_expand(EXPANDED_SIZE)
    wait_for_volume_expansion(grpc_controller, EXPANDED_SIZE)
    grpc_controller.volume_frontend_start(FRONTEND_TGT_BLOCKDEV)

    volume_info = grpc_controller.volume_get()
    assert volume_info.last_expansion_error != ""
    assert volume_info.last_expansion_failed_at != ""
    verify_replica_state(grpc_controller, r1_url, "ERR")
    verify_replica_state(grpc_controller, r2_url, "RW")

    expansion_disk_2 = os.path.join(FIXED_REPLICA_PATH2, EXPANSION_DISK_NAME)
    disk_meta_tmp_2 = os.path.join(
        FIXED_REPLICA_PATH2, EXPANSION_DISK_TMP_META_NAME)
    assert os.path.exists(expansion_disk_2)
    assert not os.path.exists(disk_meta_tmp_2)
    # The meta info file should keep unchanged
    replica_meta_file_2 = os.path.join(FIXED_REPLICA_PATH2,
                                       REPLICA_META_FILE_NAME)
    with open(replica_meta_file_2) as f:
        replica_meta_2 = json.load(f)
    assert replica_meta_2["Size"] == EXPANDED_SIZE

    # Clean up replica1 then check if replica2 works fine
    cleanup_replica(grpc_fixed_dir_replica1)
    verify_replica_state(grpc_controller, r1_url, "ERR")
    grpc_controller.replica_delete(replicas[0].address)

    snap1.verify_data()
    data2_len = random_length(PAGE_SIZE)
    data2 = Data(SIZE-PAGE_SIZE,
                 data2_len, random_string(data2_len))
    snap2 = Snapshot(dev, data2, address)
    snap2.verify_data()
    assert dev.readat(SIZE, SIZE) == zero_char*SIZE

    # Rebuild replica1.
    # The newly opened replica1 will be expanded automatically
    open_replica(grpc_fixed_dir_replica1)
    cmd.add_replica(address, grpc_fixed_dir_replica1.url)
    wait_for_rebuild_complete(address)
    r1 = grpc_fixed_dir_replica1.replica_get()
    assert r1.size == EXPANDED_SIZE_STR
    verify_replica_state(grpc_controller, r1_url, "RW")
    replica_meta_file_1 = os.path.join(FIXED_REPLICA_PATH1,
                                       REPLICA_META_FILE_NAME)
    with open(replica_meta_file_1) as f:
        replica_meta_1 = json.load(f)
    assert replica_meta_1["Size"] == EXPANDED_SIZE

    # Delete replica2 then check if the rebuilt replica1 works fine
    cleanup_replica(grpc_fixed_dir_replica2)
    verify_replica_state(grpc_controller, r2_url, "ERR")
    grpc_controller.replica_delete(replicas[1].address)

    data3_len = random_length(PAGE_SIZE)
    data3 = Data(random.randrange(SIZE, EXPANDED_SIZE-PAGE_SIZE, PAGE_SIZE),
                 data3_len, random_string(data3_len))
    snap3 = Snapshot(dev, data3, address)
    snap1.verify_data()
    snap2.verify_data()
    snap3.verify_data()
    assert \
        dev.readat(SIZE, SIZE) == zero_char*(data3.offset-SIZE) + \
        data3.content + zero_char*(EXPANDED_SIZE-data3.offset-data3.length)


def test_replica_crashed_update_state_error(grpc_controller,
        grpc_fixed_dir_replica1):  # NOQA
    """
    The test flow:
    1. Create a fixed directory replica1, since we need to remove a file
    manually.
    2. Remove file 'volume-head-000.img' manually from fixed directory
    replica1.
    3. Check this fixed diretory replica1 should be in 'ERR' state.
    4. Clean up created replica.
    """
    # Create a fixed directory replica1
    open_replica(grpc_fixed_dir_replica1)

    # Before created a volume, the engine controller should have no replica.
    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    # Create a volume on this engine controller with fixed_dir_replica1.
    r1_url = grpc_fixed_dir_replica1.url
    v = grpc_controller.volume_start(
        SIZE, SIZE, replicas=[r1_url])
    assert v.replicaCount == 1

    # Check engine controller should have 1 replica in 'RW' mode.
    replicas = grpc_controller.replica_list()
    assert len(replicas) == 1
    assert replicas[0].mode == "RW"

    # Get the replica object
    r = grpc_fixed_dir_replica1.replica_get()
    assert r.chain == ['volume-head-000.img']
    assert r.state == 'open'
    assert r.sector_size == 512

    # Removing a file from this replica directory
    remove_file = os.path.join(
            FIXED_REPLICA_PATH1, "volume-head-000.img")

    assert os.path.exists(remove_file)
    os.remove(remove_file)

    # After removing the file, the replica should be in 'ERR' mode.
    verify_replica_mode(grpc_controller, r1_url, "ERR")

    # Clean up created replica.
    cleanup_replica(grpc_fixed_dir_replica1)
