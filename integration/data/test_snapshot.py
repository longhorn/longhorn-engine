import random
import subprocess
import pytest

import common.cmd as cmd

from common.core import (  # NOQA
    get_dev, read_dev,
    generate_random_data, read_from_backing_file,
    Snapshot, snapshot_revert_with_frontend, wait_for_purge_completion,
    Data, random_length, random_string,
    expand_volume_with_frontend,
    wait_and_check_volume_expansion,
    checksum_dev
)

from common.constants import (
    VOLUME_HEAD, ENGINE_NAME, ENGINE_BACKING_NAME,
    VOLUME_NAME, VOLUME_BACKING_NAME,
    PAGE_SIZE, SIZE, EXPANDED_SIZE,
)

from common.util import checksum_data

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
    nsenter_cmd = ["nsenter", "--mount=/host/proc/1/ns/mnt",
                   "--net=/host/proc/1/ns/net", "--"]
    mount_cmd = nsenter_cmd + ["mount", "--make-shared", dev_path, mnt_path]
    umount_cmd = nsenter_cmd + ["umount", mnt_path]
    findmnt_cmd = nsenter_cmd + ["findmnt", dev_path]
    subprocess.check_call(nsenter_cmd + ["mkfs.ext4", dev_path])
    subprocess.check_call(nsenter_cmd + ["mkdir", "-p", mnt_path])
    subprocess.check_call(mount_cmd)
    subprocess.check_call(findmnt_cmd)

    def checksum_test_file():
        read_cmd = nsenter_cmd + ["cat", test_file]
        data = subprocess.check_output(read_cmd)
        return checksum_data(str(data).encode('utf-8'))

    def write_test_file():
        # beware don't touch this write command
        data = random_string(length)
        write_cmd = ["/bin/sh -c '/bin/echo",
                     '"' + data + '"', ">", test_file + "'"]
        shell_cmd = " ".join(nsenter_cmd + write_cmd)

        subprocess.check_call(shell_cmd, shell=True)
        return checksum_test_file()

    # create snapshot1 with empty fs
    # NOTE: we cannot use checksum_dev since it assumes
    #  asci data for device data instead of raw bytes
    snap1 = cmd.snapshot_create(address)

    # create snapshot2 with a new test file
    test2_checksum = write_test_file()
    snap2 = cmd.snapshot_create(address)

    # create snapshot3 overwriting the test file
    test3_checksum = write_test_file()
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
        checksum_test_file()
    subprocess.check_call(umount_cmd)

    print("\nsnapshot_revert_with_frontend snap2 begin")
    snapshot_revert_with_frontend(address, engine_name, snap2)
    print("snapshot_revert_with_frontend snap2 finish\n")
    subprocess.check_call(mount_cmd)
    assert checksum_test_file() == test2_checksum
    subprocess.check_call(umount_cmd)

    print("\nsnapshot_revert_with_frontend snap3 begin")
    snapshot_revert_with_frontend(address, engine_name, snap3)
    print("snapshot_revert_with_frontend snap3 finish\n")
    subprocess.check_call(mount_cmd)
    assert checksum_test_file() == test3_checksum
    subprocess.check_call(umount_cmd)

    # remove the created mount folder
    subprocess.check_call(nsenter_cmd + ["rmdir", mnt_path])
