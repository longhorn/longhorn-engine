import random
import subprocess
import time

import pytest

import common.cmd as cmd

from common.core import (  # NOQA
    cleanup_controller, cleanup_replica,
    get_dev, read_dev,
    generate_random_data, read_from_backing_file,
    Snapshot, snapshot_revert_with_frontend, wait_for_purge_completion,
    Data, random_length, random_string,
    expand_volume_with_frontend,
    wait_and_check_volume_expansion,
    checksum_dev, verify_data,
    get_filesystem_block_size,
    get_nsenter_cmd,
    checksum_filesystem_file, write_filesystem_file, remove_filesystem_file,
)

from common.constants import (
    VOLUME_HEAD, ENGINE_NAME, ENGINE_BACKING_NAME,
    VOLUME_NAME, VOLUME_BACKING_NAME,
    PAGE_SIZE, SIZE, EXPANDED_SIZE,
    FRONTEND_TGT_BLOCKDEV,
    RETRY_COUNTS_SHORT, RETRY_INTERVAL_SHORT
)

from data.snapshot_tree import snapshot_tree_build, snapshot_tree_verify_node


def snapshot_revert_test(dev, address, engine_name):  # NOQA
    existings = {}

    snap1 = Snapshot(dev, generate_random_data(existings),
                     address)
    snap2 = Snapshot(dev, generate_random_data(existings),
                     address)
    snap3 = Snapshot(dev, generate_random_data(existings),
                     address)

    snapList = cmd.snapshot_ls(address)
    assert snap1.name in snapList
    assert snap2.name in snapList
    assert snap3.name in snapList

    snapshot_revert_with_frontend(address, engine_name, snap2.name)
    snap3.refute_data()
    snap2.verify_checksum()
    snap1.verify_data()

    snapshot_revert_with_frontend(address, engine_name, snap1.name)
    snap3.refute_data()
    snap2.refute_data()
    snap1.verify_checksum()

    cmd.snapshot_rm(address, snap2.name)
    with pytest.raises(subprocess.CalledProcessError):
        snapshot_revert_with_frontend(address, engine_name, snap2.name)

    with pytest.raises(subprocess.CalledProcessError):
        snapshot_revert_with_frontend(address, engine_name, "non-existing")

def test_snapshot_revert(grpc_controller,  # NOQA
                         grpc_replica1, grpc_replica2):  # NOQA
    address = grpc_controller.address
    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)
    snapshot_revert_test(dev, address, ENGINE_NAME)


# BUG: https://github.com/rancher/longhorn/issues/108
def test_snapshot_rm_basic(grpc_controller,  # NOQA
                           grpc_replica1, grpc_replica2):  # NOQA
    address = grpc_controller.address

    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    existings = {}

    snap1 = Snapshot(dev, generate_random_data(existings),
                     address)
    snap2 = Snapshot(dev, generate_random_data(existings),
                     address)
    snap3 = Snapshot(dev, generate_random_data(existings),
                     address)

    info = cmd.snapshot_info(address)
    assert len(info) == 4
    assert VOLUME_HEAD in info
    assert snap1.name in info
    assert snap2.name in info
    assert snap3.name in info

    cmd.snapshot_rm(address, snap2.name)
    cmd.snapshot_purge(address)
    wait_for_purge_completion(address)

    info = cmd.snapshot_info(address)
    assert len(info) == 3
    assert snap1.name in info
    assert snap3.name in info

    snap3.verify_checksum()
    snap2.verify_data()
    snap1.verify_data()

    snapshot_revert_with_frontend(address, ENGINE_NAME, snap1.name)
    snap3.refute_data()
    snap2.refute_data()
    snap1.verify_checksum()


def test_snapshot_revert_with_backing_file(grpc_backing_controller,  # NOQA
                                           grpc_backing_qcow2_replica1,  # NOQA
                                           grpc_backing_qcow2_replica2):  # NOQA
    address = grpc_backing_controller.address

    dev = get_dev(grpc_backing_qcow2_replica1, grpc_backing_qcow2_replica2,
                  grpc_backing_controller)

    offset = 0
    length = 256

    snap0 = cmd.snapshot_create(address)
    before = read_dev(dev, offset, length)
    assert before != ""

    info = cmd.snapshot_info(address)
    assert len(info) == 2
    assert VOLUME_HEAD in info
    assert snap0 in info

    exists = read_from_backing_file(offset, length)
    assert before == exists

    snapshot_revert_test(dev, address, ENGINE_BACKING_NAME)

    snapshot_revert_with_frontend(address, ENGINE_BACKING_NAME, snap0)
    after = read_dev(dev, offset, length)
    assert before == after


def test_snapshot_rm_rolling(grpc_controller,  # NOQA
                             grpc_replica1, grpc_replica2):  # NOQA
    address = grpc_controller.address

    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    existings = {}

    snap1 = Snapshot(dev, generate_random_data(existings),
                     address)

    snapList = cmd.snapshot_ls(address)
    assert snap1.name in snapList

    cmd.snapshot_rm(address, snap1.name)
    # cannot do anything because it's the parent of volume head
    cmd.snapshot_purge(address)
    wait_for_purge_completion(address)

    snap2 = Snapshot(dev, generate_random_data(existings),
                     address)

    info = cmd.snapshot_info(address)
    assert len(info) == 3
    assert snap1.name in info
    assert snap2.name in info
    assert info[snap1.name]["removed"] is True
    assert info[snap2.name]["removed"] is False

    cmd.snapshot_rm(address, snap2.name)
    # this should trigger the deletion of snap1
    cmd.snapshot_purge(address)
    wait_for_purge_completion(address)

    snap2.verify_checksum()
    snap1.verify_data()

    snap3 = Snapshot(dev, generate_random_data(existings),
                     address)
    snap4 = Snapshot(dev, generate_random_data(existings),
                     address)
    snap5 = Snapshot(dev, generate_random_data(existings),
                     address)

    snapList = cmd.snapshot_ls(address)
    assert snap1.name not in snapList
    assert snap2.name not in snapList
    assert snap3.name in snapList
    assert snap4.name in snapList
    assert snap5.name in snapList

    info = cmd.snapshot_info(address)
    assert len(info) == 5
    assert snap1.name not in info
    assert snap2.name in info
    assert snap3.name in info
    assert snap4.name in info
    assert snap5.name in info
    assert info[snap2.name]["removed"] is True

    cmd.snapshot_rm(address, snap3.name)
    cmd.snapshot_rm(address, snap4.name)
    cmd.snapshot_rm(address, snap5.name)
    # this should trigger the deletion of snap2 - snap4
    # and snap5 marked as removed
    cmd.snapshot_purge(address)
    wait_for_purge_completion(address)

    info = cmd.snapshot_info(address)
    assert len(info) == 2
    assert snap1.name not in info
    assert snap2.name not in info
    assert snap3.name not in info
    assert snap4.name not in info
    assert snap5.name in info
    assert info[snap5.name]["removed"] is True

    snap5.verify_checksum()
    snap4.verify_data()
    snap3.verify_data()
    snap2.verify_data()
    snap1.verify_data()


def test_snapshot_tree_basic(grpc_controller,  # NOQA
                             grpc_replica1, grpc_replica2):  # NOQA
    address = grpc_controller.address

    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    offset = 0
    length = 128

    snap, data = snapshot_tree_build(dev, address, ENGINE_NAME,
                                     offset, length)

    snapshot_revert_with_frontend(address, ENGINE_NAME, snap["1b"])
    cmd.snapshot_rm(address, snap["0a"])
    cmd.snapshot_rm(address, snap["0b"])
    cmd.snapshot_rm(address, snap["1c"])
    cmd.snapshot_rm(address, snap["2a"])
    cmd.snapshot_rm(address, snap["2b"])
    cmd.snapshot_rm(address, snap["2c"])
    cmd.snapshot_rm(address, snap["3a"])
    cmd.snapshot_rm(address, snap["3b"])
    cmd.snapshot_rm(address, snap["3c"])
    cmd.snapshot_purge(address)
    wait_for_purge_completion(address)

    # the result should looks like this
    # snap["0b"](r) -> snap["0c"]
    #   \-> snap["1a"] -> snap["1b"] -> head
    info = cmd.snapshot_info(address)
    assert len(info) == 5

    assert snap["0b"] in info
    assert info[snap["0b"]]["parent"] == ""
    assert len(info[snap["0b"]]["children"]) == 2
    assert snap["0c"] in info[snap["0b"]]["children"]
    assert snap["1a"] in info[snap["0b"]]["children"]
    assert info[snap["0b"]]["removed"] is True

    assert snap["0c"] in info
    assert info[snap["0c"]]["parent"] == snap["0b"]
    assert not info[snap["0c"]]["children"]

    assert snap["1a"] in info
    assert info[snap["1a"]]["parent"] == snap["0b"]
    assert snap["1b"] in info[snap["1a"]]["children"]

    assert snap["1b"] in info
    assert info[snap["1b"]]["parent"] == snap["1a"]
    assert VOLUME_HEAD in info[snap["1b"]]["children"]

    assert VOLUME_HEAD in info
    assert info[VOLUME_HEAD]["parent"] == snap["1b"]

    # Reverting to a removing snapshot would fail
    with pytest.raises(subprocess.CalledProcessError):
        snapshot_tree_verify_node(dev, address, ENGINE_NAME,
                                  offset, length, snap, data, "0b")
    snapshot_tree_verify_node(dev, address, ENGINE_NAME,
                              offset, length, snap, data, "0c")
    snapshot_tree_verify_node(dev, address, ENGINE_NAME,
                              offset, length, snap, data, "1a")
    snapshot_tree_verify_node(dev, address, ENGINE_NAME,
                              offset, length, snap, data, "1b")


def volume_expansion_with_snapshots_test(dev, grpc_controller,  # NOQA
                                         volume_name, engine_name,
                                         original_data):
    # the default size is 4MB, will expand it to 8MB
    address = grpc_controller.address
    zero_char = b'\x00'.decode('utf-8')

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

    data4_len = random_length(PAGE_SIZE)
    data4 = Data(data1.offset,
                 data4_len, random_string(data4_len))
    snap4 = Snapshot(dev, data4, address)
    snap4.verify_data()

    # revert to snap1 then see if we can still r/w the existing data
    # and expanded part
    snapshot_revert_with_frontend(address, engine_name, snap1.name)
    assert \
        dev.readat(0, SIZE) == \
        original_data[0:data1.offset] + data1.content + \
        original_data[data1.offset+data1.length:]
    assert dev.readat(SIZE, SIZE) == zero_char*SIZE

    data5_len = random_length(PAGE_SIZE)
    data5 = Data(random.randrange(SIZE, EXPANDED_SIZE-PAGE_SIZE, PAGE_SIZE),
                 data5_len, random_string(data5_len))
    snap5 = Snapshot(dev, data5, address)
    snap5.verify_data()
    assert \
        dev.readat(SIZE, SIZE) == zero_char*(data5.offset-SIZE) + \
        data5.content + zero_char*(EXPANDED_SIZE-data5.offset-data5.length)

    # delete and purge the snap1. it will coalesce with the larger snap2
    cmd.snapshot_rm(address, snap1.name)
    cmd.snapshot_purge(address)
    wait_for_purge_completion(address)
    assert \
        dev.readat(0, SIZE) == \
        original_data[0:data1.offset] + data1.content + \
        original_data[data1.offset+data1.length:]
    assert \
        dev.readat(SIZE, SIZE) == zero_char*(data5.offset-SIZE) + \
        data5.content + zero_char*(EXPANDED_SIZE-data5.offset-data5.length)


def test_expansion_without_backing_file(grpc_controller,  # NOQA
                                        grpc_replica1, grpc_replica2):  # NOQA
    zero_char = b'\x00'.decode('utf-8')
    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)
    volume_expansion_with_snapshots_test(dev, grpc_controller,
                                         VOLUME_NAME, ENGINE_NAME,
                                         zero_char*SIZE)


def test_expansion_with_backing_file(grpc_backing_controller,  # NOQA
                                     grpc_backing_qcow2_replica1, grpc_backing_qcow2_replica2):  # NOQA
    dev = get_dev(grpc_backing_qcow2_replica1, grpc_backing_qcow2_replica2,
                  grpc_backing_controller)
    volume_expansion_with_snapshots_test(dev, grpc_backing_controller,
                                         VOLUME_BACKING_NAME,
                                         ENGINE_BACKING_NAME,
                                         read_from_backing_file(0, SIZE))


def test_snapshot_mounted_filesystem(grpc_controller,  # NOQA
                         grpc_replica1, grpc_replica2):  # NOQA
    """
    Test that the filesystem on a currently mounted volume
    will be frozen when a snapshot is requested and that
    this does not cause issues for the snapshot process.

    1. Create & attach a longhorn volume
    2. Create & mount an ext4 filesystem on that volume in host namespace
    3. Ensure volume mount point can be found in host namespace
    4. Take snapshot 1 (empty filesystem)
    5. Write test file on the filesystem
    6. Take snapshot 2 (test file included)
    7. Overwrite test file on the filesystem
    8. Take snapshot 3 (test file changed)
    9. Observe that logs of engine `Filesystem frozen / unfrozen`
    10. Validate snapshots are created
    11. Restore snapshot 1, validate test file missing
    12. Restore snapshot 2, validate test file original content
    13. Restore snapshot 3, validate test file changed content
    """
    address = grpc_controller.address
    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)
    snapshot_mounted_filesystem_test(VOLUME_NAME, dev, address, ENGINE_NAME)


def snapshot_mounted_filesystem_test(volume_name, dev, address, engine_name):  # NOQA
    dev_path = dev.dev
    mnt_path = "/tmp/mnt-" + volume_name
    test_file = mnt_path + "/test"
    length = 128

    print("dev_path: " + dev_path + "\n")
    print("mnt_path: " + mnt_path + "\n")

    # create & mount a ext4 filesystem on dev
    nsenter_cmd = get_nsenter_cmd()
    mount_cmd = nsenter_cmd + ["mount", "--make-shared", dev_path, mnt_path]
    umount_cmd = nsenter_cmd + ["umount", mnt_path]
    findmnt_cmd = nsenter_cmd + ["findmnt", dev_path]
    subprocess.check_call(nsenter_cmd + ["mkfs.ext4", dev_path])
    subprocess.check_call(nsenter_cmd + ["mkdir", "-p", mnt_path])
    subprocess.check_call(mount_cmd)
    subprocess.check_call(findmnt_cmd)

    # create snapshot1 with empty fs
    # NOTE: we cannot use checksum_dev since it assumes
    #  asci data for device data instead of raw bytes
    snap1 = cmd.snapshot_create(address)

    # create snapshot2 with a new test file
    test2_checksum = write_filesystem_file(length, test_file)
    snap2 = cmd.snapshot_create(address)

    # create snapshot3 overwriting the test file
    test3_checksum = write_filesystem_file(length, test_file)
    snap3 = cmd.snapshot_create(address)

    # verify existence of the snapshots
    snapshots = cmd.snapshot_ls(address)
    assert snap1 in snapshots
    assert snap2 in snapshots
    assert snap3 in snapshots

    # unmount the volume, since each revert will shutdown the device
    subprocess.check_call(umount_cmd)

    # restore snapshots 1,2,3 & verify filesystem state
    print("\nsnapshot_revert_with_frontend snap1 begin")
    snapshot_revert_with_frontend(address, engine_name, snap1)
    print("snapshot_revert_with_frontend snap1 finish\n")
    subprocess.check_call(mount_cmd)
    # should error since the file does not exist in snapshot 1
    with pytest.raises(subprocess.CalledProcessError):
        print("is expected error, since the file does not exist.")
        checksum_filesystem_file(test_file)
    subprocess.check_call(umount_cmd)

    print("\nsnapshot_revert_with_frontend snap2 begin")
    snapshot_revert_with_frontend(address, engine_name, snap2)
    print("snapshot_revert_with_frontend snap2 finish\n")
    subprocess.check_call(mount_cmd)
    assert checksum_filesystem_file(test_file) == test2_checksum
    subprocess.check_call(umount_cmd)

    print("\nsnapshot_revert_with_frontend snap3 begin")
    snapshot_revert_with_frontend(address, engine_name, snap3)
    print("snapshot_revert_with_frontend snap3 finish\n")
    subprocess.check_call(mount_cmd)
    assert checksum_filesystem_file(test_file) == test3_checksum
    subprocess.check_call(umount_cmd)

    # remove the created mount folder
    subprocess.check_call(nsenter_cmd + ["rmdir", mnt_path])


def test_snapshot_prune(grpc_controller, grpc_replica1, grpc_replica2):  # NOQA
    """
    Test removing the snapshot directly behinds the volume head would trigger
    snapshot prune. Snapshot pruning means removing the overlapping part from
    the snapshot based on the volume head content.

    Context:

    We need to verify that removing the snapshot directly behinds the volume
    head would trigger snapshot prune. And the data after the purge is intact.
    Snapshot pruning means removing the overlapping part from the snapshot
    based on the volume head content.

    Steps:

    1.  Create a volume and attach to the current node
    2.  Write some data to the volume.
    3.  Take snapshot `snap1`.
    4.  Write some data to the volume then compute the checksum.
    5.  Delete snapshot `snap1`.
    6.  Verify the snapshot is marked as Removed.
    7.  Wait for the snapshot purge (pruning) complete.
    8.  Verify the volume data is intact.
    9.  Verify Snapshot size is decreased by the overlapping size.
    10. Create one more snapshot `snap2`.
    11. Try to revert to snapshot `snap1`. This should fail.
    12. Trigger the snapshot purge.
        Verify snapshot `snap1` can be removed. And the data is still intact.
    """

    address = grpc_controller.address

    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    # Create a snapshot that contains 4Ki data at offset 0.
    offset1 = 0
    page_count1 = 8
    length1 = page_count1*PAGE_SIZE
    data1 = random_string(length1)
    for i in range(page_count1):
        verify_data(dev, i*PAGE_SIZE, data1[i*PAGE_SIZE: (i+1)*PAGE_SIZE])
    snap1 = cmd.snapshot_create(address)

    # Write 512B actual data at offset 512 to the volume head.
    offset2 = PAGE_SIZE
    length2 = PAGE_SIZE
    data2 = random_string(length2)
    verify_data(dev, offset2, data2)

    # the result should look like this
    # snap1(4Ki) -> volume head(4Ki rather than 512B)
    info = cmd.snapshot_info(address)
    assert len(info) == 2

    assert snap1 in info
    assert info[snap1]["parent"] == ""
    assert VOLUME_HEAD in info[snap1]["children"]
    assert not info[snap1]["removed"]

    assert VOLUME_HEAD in info
    assert info[VOLUME_HEAD]["parent"] == snap1

    snap1_size1 = int(info[snap1]["size"])
    head_size1 = int(info[VOLUME_HEAD]["size"])

    cmd.snapshot_rm(address, snap1)
    cmd.snapshot_purge(address)
    wait_for_purge_completion(address)

    info = cmd.snapshot_info(address)
    assert len(info) == 2

    assert snap1 in info
    assert info[snap1]["parent"] == ""
    assert VOLUME_HEAD in info[snap1]["children"]
    assert info[snap1]["removed"]
    assert int(info[snap1]["size"]) == snap1_size1 - 4096

    assert VOLUME_HEAD in info
    assert info[VOLUME_HEAD]["parent"] == snap1
    assert int(info[VOLUME_HEAD]["size"]) == head_size1

    # Verify the data content
    assert read_dev(dev, offset1, length1) == \
           data1[:offset2] + data2 + data1[offset2+length2:]

    snap2 = cmd.snapshot_create(address)

    with pytest.raises(subprocess.CalledProcessError):
        snapshot_revert_with_frontend(address, ENGINE_NAME, snap1)
    grpc_controller.volume_frontend_start(FRONTEND_TGT_BLOCKDEV)

    cmd.snapshot_purge(address)
    wait_for_purge_completion(address)

    info = cmd.snapshot_info(address)
    assert len(info) == 2

    assert snap1 not in info
    assert snap2 in info
    assert info[snap2]["parent"] == ""
    assert VOLUME_HEAD in info[snap2]["children"]
    assert not info[snap2]["removed"]
    assert int(info[snap2]["size"]) == length1


def test_snapshot_prune_with_coalesce(grpc_controller, grpc_replica1, grpc_replica2):  # NOQA
    """
    Test the prune for the snapshot directly behinds the volume head would be
    handled after all snapshot coalescing done.

    Steps:

    1.  Create a volume and attach to the current node
    2.  Write some data to the 1st block of the volume.
    3.  Take snapshot `snap1`.
    4.  Write some data to the 2nd block of the volume.
    5.  Take snapshot `snap2`.
    6.  Write some data to the 3rd block of the volume.
    7.  Take snapshot `snap3`.
    8.  Overwrite the previous data to the volume head
        then compute the checksum.
    9.  Mark all snapshot as Removed then start snapshot purge.
    10. Verify there is only one empty snapshot after purge.
        And the data is still intact.
    """

    fs_block_size = get_filesystem_block_size()

    address = grpc_controller.address

    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    # snap1
    verify_data(dev, 0*fs_block_size, random_string(PAGE_SIZE))
    snap1 = cmd.snapshot_create(address)

    verify_data(dev, 1*fs_block_size, random_string(PAGE_SIZE))
    snap2 = cmd.snapshot_create(address)

    verify_data(dev, 2*fs_block_size, random_string(PAGE_SIZE))
    snap3 = cmd.snapshot_create(address)

    # Overwrite the data in the volume head
    verify_data(dev, 0*fs_block_size, random_string(PAGE_SIZE))
    verify_data(dev, 1*fs_block_size, random_string(PAGE_SIZE))
    verify_data(dev, 2*fs_block_size, random_string(PAGE_SIZE))
    cksum = read_dev(dev, 0, 3*fs_block_size)

    cmd.snapshot_rm(address, snap1)
    cmd.snapshot_rm(address, snap2)
    cmd.snapshot_rm(address, snap3)
    cmd.snapshot_purge(address)
    wait_for_purge_completion(address)

    info = cmd.snapshot_info(address)
    assert len(info) == 2

    assert snap1 not in info
    assert snap2 not in info
    assert snap3 in info
    assert info[snap3]["parent"] == ""
    assert VOLUME_HEAD in info[snap3]["children"]
    assert info[snap3]["removed"]
    assert int(info[snap3]["size"]) == 0

    assert VOLUME_HEAD in info
    assert info[VOLUME_HEAD]["parent"] == snap3
    assert int(info[VOLUME_HEAD]["size"]) == 3*fs_block_size

    # Verify the data content
    assert cksum == read_dev(dev, 0, 3*fs_block_size)


def test_snapshot_trim_filesystem(grpc_controller,  # NOQA
                                  grpc_replica1, grpc_replica2):  # NOQA
    """
    Test that the trimming against the filesystem on a
    currently mounted volume can be applied to a removing/system snapshot
    or the volume head.

    1. Create & attach a longhorn volume.
    2. Create & mount an ext4 filesystem on that volume in host namespace.
    3. Ensure volume mount point can be found in host namespace.
    4. Write file1 on the filesystem and take snapshot1.
    5. Write file_branch on the filesystem and take snapshot_branch.
    6. Revert to snapshot1.
    7. Write file2 on the filesystem and take snapshot2.
    8. Write file3 on the filesystem and take snapshot3.
    9. Modify file1 on the filesystem (in volume head).
    10. Delete file1, file2, and file3.
    11. Trim the filesystem, and validate:
        only the size of the volume head is decreased.
    12. Enable unmap-mark-snap-chain-removed for the volume.
    13. Trim the filesystem again, and validate:
        1. snapshot3 and snapshot2 are marked as removed
        2. snapshot3 and snapshot2 size should decrease.
        3. snapshot 1 is unchanged.
    14. Revert to snapshot_branch,
        validate file1 and file_branch original content.
    """
    address = grpc_controller.address

    # TODO: Increase the default volume size of the engine integration test to
    #  meet the min requirement of xfs. But increasing the volume size means
    #  we need to re-check all expansion related tests.
    for fs_type in ["ext4"]:
        dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)
        filesystem_trim_test(dev, address, VOLUME_NAME, ENGINE_NAME, fs_type)
        cmd.sync_agent_server_reset(address)
        cleanup_controller(grpc_controller)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)


def filesystem_trim_test(dev, address, volume_name, engine_name, fs_type):  # NOQA
    dev_path = dev.dev
    mnt_path = "/tmp/mnt-" + VOLUME_NAME

    # create & mount a ext4 filesystem on dev
    nsenter_cmd = get_nsenter_cmd()
    mount_cmd = nsenter_cmd + ["mount", "--make-shared", dev_path, mnt_path]
    umount_cmd = nsenter_cmd + ["umount", mnt_path]
    findmnt_cmd = nsenter_cmd + ["findmnt", dev_path]
    trim_cmd = nsenter_cmd + ["fstrim", mnt_path]
    if fs_type == "ext4":
        subprocess.check_call(nsenter_cmd + ["mkfs.ext4", dev_path])
    elif fs_type == "xfs":
        subprocess.check_call(nsenter_cmd + ["mkfs.xfs", dev_path])
    else:
        raise Exception("unexpected filesystem type")
    subprocess.check_call(nsenter_cmd + ["mkdir", "-p", mnt_path])
    subprocess.check_call(mount_cmd)
    subprocess.check_call(findmnt_cmd)

    fs_block_size = 4 * 1024
    subprocess.check_call(nsenter_cmd + ["sync"])

    # For ext4, the filesystem metadata contains at least 2 blocks:
    #     super block + GDT info, inode bitmap.
    # For xfs, the filesystem metadata contains multiple blocks (> 2 blocks):
    #     super block, free space B+ tree, inode B+ tree.
    info = wait_for_snap_size_no_less_than_value(
        address, VOLUME_HEAD, 2*fs_block_size)
    assert len(info) == 1
    fs_meta_size = int(info[VOLUME_HEAD]["size"])

    length = 3 * 1024
    # create snapshot1 with fs and file 1
    file_path1 = mnt_path + "/test1"
    test1_checksum = write_filesystem_file(length, file_path1)
    subprocess.check_call(nsenter_cmd + ["sync"])
    # To guarantee that the data is flushed into the file, we need to check
    # the size before snapshot creation.
    # It is hard to predict the exact blocks the filesystem would consume
    # when writing a file. but writing a 3Ki file requires at least 3 blocks:
    # the data itself, inode info update, and free space info (for xfs)
    wait_for_snap_size_no_less_than_value(
        address, VOLUME_HEAD, fs_meta_size + 3*fs_block_size)
    snap1 = cmd.snapshot_create(address)

    info = wait_for_snap_size_no_less_than_value(
        address, snap1, fs_meta_size + 3*fs_block_size)
    assert info[snap1]["parent"] == ""
    assert not info[snap1]["removed"]
    snap1_size = int(info[snap1]["size"])

    # create snapshot-branch with file branch
    file_path_branch = mnt_path + "/test-branch"
    test_branch_checksum = write_filesystem_file(length, file_path_branch)
    subprocess.check_call(nsenter_cmd + ["sync"])
    wait_for_snap_size_no_less_than_value(
        address, VOLUME_HEAD, 3*fs_block_size)
    snap_branch = cmd.snapshot_create(address)

    info = wait_for_snap_size_no_less_than_value(
        address, snap_branch, 3*fs_block_size)
    assert info[snap_branch]["parent"] == snap1
    assert VOLUME_HEAD in info[snap_branch]["children"]
    assert not info[snap_branch]["removed"]

    # revert to snapshot1
    # unmount the volume, since each revert will shutdown the device
    subprocess.check_call(umount_cmd)
    snapshot_revert_with_frontend(address, engine_name, snap1)
    subprocess.check_call(mount_cmd)

    # create snapshot2 with file 2
    file_path2 = mnt_path + "/test2"
    write_filesystem_file(length, file_path2)
    subprocess.check_call(nsenter_cmd + ["sync"])
    wait_for_snap_size_no_less_than_value(
        address, VOLUME_HEAD, 3*fs_block_size)
    snap2 = cmd.snapshot_create(address)

    info = wait_for_snap_size_no_less_than_value(
        address, snap2, 3*fs_block_size)
    assert info[snap2]["parent"] == snap1
    snap2_size = int(info[snap2]["size"])

    # create snapshot3 with file 3
    file_path3 = mnt_path + "/test3"
    write_filesystem_file(length, file_path3)
    subprocess.check_call(nsenter_cmd + ["sync"])
    wait_for_snap_size_no_less_than_value(
        address, VOLUME_HEAD, 3*fs_block_size)
    snap3 = cmd.snapshot_create(address)

    info = wait_for_snap_size_no_less_than_value(
        address, snap3, 3*fs_block_size)
    assert info[snap3]["parent"] == snap2
    snap3_size = int(info[snap3]["size"])

    # append new data to file 1 in the volume head
    write_filesystem_file(length, file_path1)
    subprocess.check_call(nsenter_cmd + ["sync"])
    wait_for_snap_size_no_less_than_value(
        address, VOLUME_HEAD, 3*fs_block_size)

    remove_filesystem_file(file_path1)
    remove_filesystem_file(file_path2)
    remove_filesystem_file(file_path3)
    subprocess.check_call(nsenter_cmd + ["sync"])

    # verify the snapshots before and after the fs trim
    info = cmd.snapshot_info(address)
    assert len(info) == 5

    assert snap1 in info
    assert info[snap1]["parent"] == ""
    assert snap2 in info[snap1]["children"]
    assert not info[snap1]["removed"]
    assert int(info[snap1]["size"]) == snap1_size

    assert snap2 in info
    assert info[snap2]["parent"] == snap1
    assert snap3 in info[snap2]["children"]
    assert not info[snap2]["removed"]
    assert int(info[snap2]["size"]) == snap2_size

    assert snap3 in info
    assert info[snap3]["parent"] == snap2
    assert VOLUME_HEAD in info[snap3]["children"]
    assert not info[snap3]["removed"]
    assert int(info[snap3]["size"]) == snap3_size

    assert VOLUME_HEAD in info
    assert info[VOLUME_HEAD]["parent"] == snap3
    head_size = int(info[VOLUME_HEAD]["size"])
    assert head_size >= fs_block_size

    # trim the filesystem
    subprocess.check_call(trim_cmd)

    info = cmd.snapshot_info(address)
    assert len(info) == 5

    assert snap1 in info
    assert not info[snap1]["removed"]
    assert int(info[snap1]["size"]) == snap1_size

    assert snap2 in info
    assert not info[snap2]["removed"]
    assert int(info[snap2]["size"]) == snap2_size

    assert snap3 in info
    assert not info[snap3]["removed"]
    assert int(info[snap3]["size"]) == snap3_size

    assert VOLUME_HEAD in info
    # similarly, it's hard to predict the exact trimmed blocks of a filesystem.
    assert int(info[VOLUME_HEAD]["size"]) <= head_size - fs_block_size

    cmd.set_unmap_mark_snap_chain_removed(address, True)

    # Re-mount the filesystem then re-do trim
    subprocess.check_call(umount_cmd)
    subprocess.check_call(mount_cmd)
    subprocess.check_call(trim_cmd)

    info = cmd.snapshot_info(address)
    assert len(info) == 5

    assert snap1 in info
    assert not info[snap1]["removed"]
    assert int(info[snap1]["size"]) == snap1_size

    assert snap2 in info
    assert info[snap2]["removed"]
    assert int(info[snap2]["size"]) <= snap2_size - fs_block_size

    assert snap3 in info
    assert info[snap3]["removed"]
    assert int(info[snap3]["size"]) <= snap3_size - fs_block_size

    assert VOLUME_HEAD in info
    assert int(info[VOLUME_HEAD]["size"]) <= head_size - fs_block_size

    # unmount the volume, since each revert will shutdown the device
    subprocess.check_call(umount_cmd)

    # restore to another snapshot branch and verify the data integrity
    snapshot_revert_with_frontend(address, engine_name, snap_branch)
    subprocess.check_call(mount_cmd)
    assert checksum_filesystem_file(file_path1) == test1_checksum
    assert checksum_filesystem_file(file_path_branch) == test_branch_checksum
    subprocess.check_call(umount_cmd)


def wait_for_snap_size_no_less_than_value(controller_addr, snap_name, size):
    for i in range(RETRY_COUNTS_SHORT):
        info = cmd.snapshot_info(controller_addr)
        if snap_name in info and int(info[snap_name]["size"]) >= size:
            break
        time.sleep(RETRY_INTERVAL_SHORT)
    assert snap_name in info
    assert int(info[snap_name]["size"]) >= size
    return info
