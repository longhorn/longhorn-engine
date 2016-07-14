import subprocess

import pytest

import cmd
import common
from common import dev, backing_dev  # NOQA
from common import read_dev, read_from_backing_file, BACKUP_DEST

VOLUME_NAME = 'test-volume'
VOLUME_SIZE = str(4 * 1024 * 1024)  # 4M


def test_backup(dev):  # NOQA
    offset = 0
    length = 128

    snap1_data = common.random_string(length)
    common.verify_data(dev, offset, snap1_data)
    snap1_checksum = common.checksum_dev(dev)
    snap1 = cmd.snapshot_create()

    backup1 = cmd.backup_create(snap1, BACKUP_DEST)
    backup1_info = cmd.backup_inspect(backup1)
    assert backup1_info["BackupURL"] == backup1
    assert backup1_info["VolumeName"] == VOLUME_NAME
    assert backup1_info["VolumeSize"] == VOLUME_SIZE
    assert snap1 in backup1_info["SnapshotName"]

    snap2_data = common.random_string(length)
    common.verify_data(dev, offset, snap2_data)
    snap2_checksum = common.checksum_dev(dev)
    snap2 = cmd.snapshot_create()

    backup2 = cmd.backup_create(snap2, BACKUP_DEST)
    backup2_info = cmd.backup_inspect(backup2)
    assert backup2_info["BackupURL"] == backup2
    assert backup2_info["VolumeName"] == VOLUME_NAME
    assert backup2_info["VolumeSize"] == VOLUME_SIZE
    assert snap2 in backup2_info["SnapshotName"]

    snap3_data = common.random_string(length)
    common.verify_data(dev, offset, snap3_data)
    snap3_checksum = common.checksum_dev(dev)
    snap3 = cmd.snapshot_create()

    backup3 = cmd.backup_create(snap3, BACKUP_DEST)
    backup3_info = cmd.backup_inspect(backup3)
    assert backup3_info["BackupURL"] == backup3
    assert backup3_info["VolumeName"] == VOLUME_NAME
    assert backup3_info["VolumeSize"] == VOLUME_SIZE
    assert snap3 in backup3_info["SnapshotName"]

    cmd.backup_restore(backup3)
    readed = read_dev(dev, offset, length)
    assert readed == snap3_data
    c = common.checksum_dev(dev)
    assert c == snap3_checksum

    cmd.backup_rm(backup3)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup3)
    with pytest.raises(subprocess.CalledProcessError) as e:
        cmd.backup_inspect(backup3)
        assert 'cannot find' in str(e.value)

    cmd.backup_restore(backup1)
    readed = read_dev(dev, offset, length)
    assert readed == snap1_data
    c = common.checksum_dev(dev)
    assert c == snap1_checksum

    cmd.backup_rm(backup1)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup1)
    with pytest.raises(subprocess.CalledProcessError) as e:
        cmd.backup_inspect(backup1)
        assert 'cannot find' in str(e.value)

    cmd.backup_restore(backup2)
    readed = read_dev(dev, offset, length)
    assert readed == snap2_data
    c = common.checksum_dev(dev)
    assert c == snap2_checksum

    cmd.backup_rm(backup2)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup2)
    with pytest.raises(subprocess.CalledProcessError) as e:
        cmd.backup_inspect(backup2)
        assert 'cannot find' in str(e.value)


def test_backup_with_backing_file(backing_dev):  # NOQA
    dev = backing_dev  # NOQA

    offset = 0
    length = 256

    snap0 = cmd.snapshot_create()
    before = read_dev(dev, offset, length)
    assert before != ""
    snap0_checksum = common.checksum_dev(dev)

    exists = read_from_backing_file(offset, length)
    assert before == exists

    backup0 = cmd.backup_create(snap0, BACKUP_DEST)
    backup0_info = cmd.backup_inspect(backup0)
    assert backup0_info["BackupURL"] == backup0
    assert backup0_info["VolumeName"] == VOLUME_NAME
    assert backup0_info["VolumeSize"] == VOLUME_SIZE
    assert snap0 in backup0_info["SnapshotName"]

    test_backup(dev)

    cmd.backup_restore(backup0)
    after = read_dev(dev, offset, length)
    assert before == after
    c = common.checksum_dev(dev)
    assert c == snap0_checksum

    cmd.backup_rm(backup0)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup0)
    with pytest.raises(subprocess.CalledProcessError) as e:
        cmd.backup_inspect(backup0)
        assert 'cannot find' in str(e.value)


def test_backup_hole_with_backing_file(backing_dev):  # NOQA
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
    backup1 = cmd.backup_create(snap1, BACKUP_DEST)

    snap2_data = common.random_string(length2)
    common.verify_data(dev, offset2, snap2_data)
    snap2_checksum = common.checksum_dev(dev)
    snap2 = cmd.snapshot_create()

    boundary_data_backup2 = read_dev(dev, boundary_offset, boundary_length)
    hole_data_backup2 = read_dev(dev, hole_offset, hole_length)
    backup2 = cmd.backup_create(snap2, BACKUP_DEST)

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
