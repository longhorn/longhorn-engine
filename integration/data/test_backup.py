import subprocess

import pytest

import cmd
import common
from common import replica1, replica2, controller, backing_replica1, backing_replica2, backup_targets  # NOQA
from common import read_dev, read_from_backing_file
from snapshot_tree import snapshot_tree_build, snapshot_tree_verify_backup_node

VOLUME_NAME = 'test-volume_1.0'
VOLUME_SIZE = str(4 * 1024 * 1024)  # 4M
BLOCK_SIZE = str(2 * 1024 * 1024)  # 2M


def backup_test(dev, backup_target):  # NOQA
    offset = 0
    length = 128

    snap1_data = common.random_string(length)
    common.verify_data(dev, offset, snap1_data)
    snap1_checksum = common.checksum_dev(dev)
    snap1 = cmd.snapshot_create()

    backup1 = cmd.backup_create(snap1, backup_target)
    backup1_info = cmd.backup_inspect(backup1)
    assert backup1_info["URL"] == backup1
    assert backup1_info["VolumeName"] == VOLUME_NAME
    assert backup1_info["VolumeSize"] == VOLUME_SIZE
    assert backup1_info["Size"] == BLOCK_SIZE
    assert snap1 in backup1_info["SnapshotName"]

    snap2_data = common.random_string(length)
    common.verify_data(dev, offset, snap2_data)
    snap2_checksum = common.checksum_dev(dev)
    snap2 = cmd.snapshot_create()

    backup2 = cmd.backup_create(snap2, backup_target)
    backup2_info = cmd.backup_inspect(backup2)
    assert backup2_info["URL"] == backup2
    assert backup2_info["VolumeName"] == VOLUME_NAME
    assert backup2_info["VolumeSize"] == VOLUME_SIZE
    assert backup2_info["Size"] == BLOCK_SIZE
    assert snap2 in backup2_info["SnapshotName"]

    snap3_data = common.random_string(length)
    common.verify_data(dev, offset, snap3_data)
    snap3_checksum = common.checksum_dev(dev)
    snap3 = cmd.snapshot_create()

    backup3 = cmd.backup_create(snap3, backup_target)
    backup3_info = cmd.backup_inspect(backup3)
    assert backup3_info["URL"] == backup3
    assert backup3_info["VolumeName"] == VOLUME_NAME
    assert backup3_info["VolumeSize"] == VOLUME_SIZE
    assert backup3_info["Size"] == BLOCK_SIZE
    assert snap3 in backup3_info["SnapshotName"]

    cmd.backup_restore(backup3)
    readed = read_dev(dev, offset, length)
    assert readed == snap3_data
    c = common.checksum_dev(dev)
    assert c == snap3_checksum

    cmd.backup_rm(backup3)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup3)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_inspect(backup3)

    cmd.backup_restore(backup1)
    readed = read_dev(dev, offset, length)
    assert readed == snap1_data
    c = common.checksum_dev(dev)
    assert c == snap1_checksum

    cmd.backup_rm(backup1)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup1)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_inspect(backup1)

    cmd.backup_restore(backup2)
    readed = read_dev(dev, offset, length)
    assert readed == snap2_data
    c = common.checksum_dev(dev)
    assert c == snap2_checksum

    cmd.backup_rm(backup2)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup2)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_inspect(backup2)


def backup_with_backing_file_test(backing_dev, backup_target):  # NOQA
    dev = backing_dev  # NOQA

    offset = 0
    length = 256

    snap0 = cmd.snapshot_create()
    before = read_dev(dev, offset, length)
    assert before != ""
    snap0_checksum = common.checksum_dev(dev)

    exists = read_from_backing_file(offset, length)
    assert before == exists

    backup0 = cmd.backup_create(snap0, backup_target)
    backup0_info = cmd.backup_inspect(backup0)
    assert backup0_info["URL"] == backup0
    assert backup0_info["VolumeName"] == VOLUME_NAME
    assert backup0_info["VolumeSize"] == VOLUME_SIZE
    assert snap0 in backup0_info["SnapshotName"]

    backup_test(dev, backup_target)

    cmd.backup_restore(backup0)
    after = read_dev(dev, offset, length)
    assert before == after
    c = common.checksum_dev(dev)
    assert c == snap0_checksum

    cmd.backup_rm(backup0)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup0)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_inspect(backup0)


def backup_hole_with_backing_file_test(backing_dev, backup_target):  # NOQA
    dev = backing_dev  # NOQA

    offset1 = 512
    length1 = 256

    offset2 = 640
    length2 = 256

    boundary_offset = 0
    boundary_length = 4100  # just pass 4096 into next 4k

    hole_offset = 2 * 1024 * 1024
    hole_length = 1024

    snap1_data = common.random_string(length1)
    common.verify_data(dev, offset1, snap1_data)
    snap1_checksum = common.checksum_dev(dev)
    snap1 = cmd.snapshot_create()

    boundary_data_backup1 = read_dev(dev, boundary_offset, boundary_length)
    hole_data_backup1 = read_dev(dev, hole_offset, hole_length)
    backup1 = cmd.backup_create(snap1, backup_target)

    snap2_data = common.random_string(length2)
    common.verify_data(dev, offset2, snap2_data)
    snap2_checksum = common.checksum_dev(dev)
    snap2 = cmd.snapshot_create()

    boundary_data_backup2 = read_dev(dev, boundary_offset, boundary_length)
    hole_data_backup2 = read_dev(dev, hole_offset, hole_length)
    backup2 = cmd.backup_create(snap2, backup_target)

    cmd.backup_restore(backup1)
    readed = read_dev(dev, boundary_offset, boundary_length)
    assert readed == boundary_data_backup1
    readed = read_dev(dev, hole_offset, hole_length)
    assert readed == hole_data_backup1
    c = common.checksum_dev(dev)
    assert c == snap1_checksum

    cmd.backup_restore(backup2)
    readed = read_dev(dev, boundary_offset, boundary_length)
    assert readed == boundary_data_backup2
    readed = read_dev(dev, hole_offset, hole_length)
    assert readed == hole_data_backup2
    c = common.checksum_dev(dev)
    assert c == snap2_checksum


def snapshot_tree_backup_test(dev, backup_target):  # NOQA
    offset = 0
    length = 128
    backup = {}

    snap, data = snapshot_tree_build(dev, offset, length)

    backup["0b"] = cmd.backup_create(snap["0b"], backup_target)
    backup["0c"] = cmd.backup_create(snap["0c"], backup_target)
    backup["1c"] = cmd.backup_create(snap["1c"], backup_target)
    backup["2b"] = cmd.backup_create(snap["2b"], backup_target)
    backup["2c"] = cmd.backup_create(snap["2c"], backup_target)
    backup["3c"] = cmd.backup_create(snap["3c"], backup_target)

    snapshot_tree_verify_backup_node(dev, offset, length, backup, data, "0b")
    snapshot_tree_verify_backup_node(dev, offset, length, backup, data, "0c")
    snapshot_tree_verify_backup_node(dev, offset, length, backup, data, "1c")
    snapshot_tree_verify_backup_node(dev, offset, length, backup, data, "2b")
    snapshot_tree_verify_backup_node(dev, offset, length, backup, data, "2c")
    snapshot_tree_verify_backup_node(dev, offset, length, backup, data, "3c")


def test_backup(replica1, replica2, controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        dev = common.get_dev(replica1, replica2, controller)
        backup_test(dev, backup_target)
        common.cleanup_replica(replica1)
        common.cleanup_replica(replica2)
        common.cleanup_controller(controller)


def test_snapshot_tree_backup(replica1, replica2, controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        dev = common.get_dev(replica1, replica2, controller)
        snapshot_tree_backup_test(dev, backup_target)
        common.cleanup_replica(replica1)
        common.cleanup_replica(replica2)
        common.cleanup_controller(controller)


def test_backup_with_backing_file(backing_replica1, backing_replica2, controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        backing_dev = common.get_backing_dev(backing_replica1,
                                             backing_replica2,
                                             controller)
        backup_with_backing_file_test(backing_dev, backup_target)
        common.cleanup_replica(backing_replica1)
        common.cleanup_replica(backing_replica2)
        common.cleanup_controller(controller)


def test_backup_hole_with_backing_file(backing_replica1, backing_replica2, controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        backing_dev = common.get_backing_dev(backing_replica1,
                                             backing_replica2,
                                             controller)
        backup_hole_with_backing_file_test(backing_dev, backup_target)
        common.cleanup_replica(backing_replica1)
        common.cleanup_replica(backing_replica2)
        common.cleanup_controller(controller)
