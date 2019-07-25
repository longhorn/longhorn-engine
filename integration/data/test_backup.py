import subprocess

import pytest

import cmd
import common
import launcher
from common import grpc_controller, backup_targets  # NOQA
from common import grpc_replica1, grpc_replica2  # NOQA
from common import grpc_backing_replica1, grpc_backing_replica2  # NOQA
from common import read_dev, read_from_backing_file
from snapshot_tree import snapshot_tree_build, snapshot_tree_verify_backup_node
from common import restore_with_no_frontend, \
    FRONTEND_TGT_BLOCKDEV # NOQA

VOLUME_NAME = 'test-volume_1.0'
VOLUME_SIZE = str(4 * 1024 * 1024)  # 4M
BLOCK_SIZE = str(2 * 1024 * 1024)  # 2M


def test_backup_volume_deletion(grpc_replica1, grpc_replica2,  # NOQA
                                grpc_controller, backup_targets):  # NOQA
    offset = 0
    length = 128

    for backup_target in backup_targets:
        dev = common.get_dev(grpc_replica1, grpc_replica2,
                             grpc_controller)
        snap_data = common.random_string(length)
        common.verify_data(dev, offset, snap_data)
        snap = cmd.snapshot_create()

        backup = cmd.backup_create(snap, backup_target)
        backup_info = cmd.backup_inspect(backup)
        assert backup_info["URL"] == backup
        assert backup_info["VolumeName"] == VOLUME_NAME
        assert backup_info["VolumeSize"] == VOLUME_SIZE
        assert backup_info["Size"] == BLOCK_SIZE
        assert snap in backup_info["SnapshotName"]

        cmd.backup_volume_rm(VOLUME_NAME, backup_target)
        info = cmd.backup_volume_list(VOLUME_NAME, backup_target)
        assert "cannot find" in info[VOLUME_NAME]["Messages"]["error"]

        cmd.sync_agent_server_reset()
        common.cleanup_replica(grpc_replica1)
        common.cleanup_replica(grpc_replica2)
        common.cleanup_controller(grpc_controller)


def backup_test(dev, backup_target, grpc_c):  # NOQA
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

    restore_with_no_frontend(backup3, grpc_c)
    readed = read_dev(dev, offset, length)
    assert readed == snap3_data
    c = common.checksum_dev(dev)
    assert c == snap3_checksum

    cmd.backup_rm(backup3)
    with pytest.raises(subprocess.CalledProcessError):
        restore_with_no_frontend(backup3, grpc_c)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_inspect(backup3)

    restore_with_no_frontend(backup1, grpc_c)
    readed = read_dev(dev, offset, length)
    assert readed == snap1_data
    c = common.checksum_dev(dev)
    assert c == snap1_checksum

    cmd.backup_rm(backup1)
    with pytest.raises(subprocess.CalledProcessError):
        restore_with_no_frontend(backup1, grpc_c)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_inspect(backup1)

    restore_with_no_frontend(backup2, grpc_c)
    readed = read_dev(dev, offset, length)
    assert readed == snap2_data
    c = common.checksum_dev(dev)
    assert c == snap2_checksum

    cmd.backup_rm(backup2)
    with pytest.raises(subprocess.CalledProcessError):
        restore_with_no_frontend(backup2, grpc_c)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_inspect(backup2)
    # Engine frontend is down, Start it up
    launcher.start_engine_frontend(FRONTEND_TGT_BLOCKDEV)
    v = grpc_c.volume_get()
    assert v.frontendState == "up"


def backup_with_backing_file_test(backing_dev, backup_target, grpc_c):  # NOQA
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

    backup_test(dev, backup_target, grpc_c)

    restore_with_no_frontend(backup0, grpc_c)
    after = read_dev(dev, offset, length)
    assert before == after
    c = common.checksum_dev(dev)
    assert c == snap0_checksum

    cmd.backup_rm(backup0)
    with pytest.raises(subprocess.CalledProcessError):
        restore_with_no_frontend(backup0, grpc_c)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_inspect(backup0)
    # Engine frontend is down, Start it up
    launcher.start_engine_frontend(FRONTEND_TGT_BLOCKDEV)
    v = grpc_c.volume_get()
    assert v.frontendState == "up"


def backup_hole_with_backing_file_test(backing_dev, backup_target, grpc_c):  # NOQA
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

    restore_with_no_frontend(backup1, grpc_c)
    readed = read_dev(dev, boundary_offset, boundary_length)
    assert readed == boundary_data_backup1
    readed = read_dev(dev, hole_offset, hole_length)
    assert readed == hole_data_backup1
    c = common.checksum_dev(dev)
    assert c == snap1_checksum

    restore_with_no_frontend(backup2, grpc_c)
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


def test_backup(grpc_replica1, grpc_replica2,  # NOQA
                grpc_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        dev = common.get_dev(grpc_replica1, grpc_replica2,
                             grpc_controller)
        backup_test(dev, backup_target, grpc_controller)
        cmd.sync_agent_server_reset()
        common.cleanup_replica(grpc_replica1)
        common.cleanup_replica(grpc_replica2)
        common.cleanup_controller(grpc_controller)


def test_snapshot_tree_backup(grpc_replica1, grpc_replica2,  # NOQA
                              grpc_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        dev = common.get_dev(grpc_replica1, grpc_replica2,
                             grpc_controller)
        snapshot_tree_backup_test(dev, backup_target)
        cmd.sync_agent_server_reset()
        common.cleanup_replica(grpc_replica1)
        common.cleanup_replica(grpc_replica2)
        common.cleanup_controller(grpc_controller)


def test_backup_with_backing_file(grpc_backing_replica1, grpc_backing_replica2,  # NOQA
                                  grpc_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        backing_dev = common.get_backing_dev(grpc_backing_replica1,
                                             grpc_backing_replica2,
                                             grpc_controller)
        backup_with_backing_file_test(backing_dev, backup_target,  # NOQA
                                      grpc_controller)
        cmd.sync_agent_server_reset()
        common.cleanup_replica(grpc_backing_replica1)
        common.cleanup_replica(grpc_backing_replica2)
        common.cleanup_controller(grpc_controller)


def test_backup_hole_with_backing_file(grpc_backing_replica1, grpc_backing_replica2,  # NOQA
                                       grpc_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        backing_dev = common.get_backing_dev(grpc_backing_replica1,
                                             grpc_backing_replica2,
                                             grpc_controller)
        backup_hole_with_backing_file_test(backing_dev, backup_target, # NOQA
                                           grpc_controller) # NOQA
        cmd.sync_agent_server_reset()
        common.cleanup_replica(grpc_backing_replica1)
        common.cleanup_replica(grpc_backing_replica2)
        common.cleanup_controller(grpc_controller)
