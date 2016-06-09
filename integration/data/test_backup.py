import subprocess

import pytest

import cmd
import common
from common import dev, backing_dev  # NOQA
from common import read_dev, read_from_backing_file, BACKUP_DEST


def test_backup(dev):  # NOQA
    offset = 0
    length = 128

    snap1_data = common.random_string(length)
    common.verify_data(dev, offset, snap1_data)
    snap1 = cmd.snapshot_create()

    backup1 = cmd.backup_create(snap1, BACKUP_DEST)

    snap2_data = common.random_string(length)
    common.verify_data(dev, offset, snap2_data)
    snap2 = cmd.snapshot_create()

    backup2 = cmd.backup_create(snap2, BACKUP_DEST)

    snap3_data = common.random_string(length)
    common.verify_data(dev, offset, snap3_data)
    snap3 = cmd.snapshot_create()

    backup3 = cmd.backup_create(snap3, BACKUP_DEST)

    cmd.backup_restore(backup3)
    readed = read_dev(dev, offset, length)
    assert readed == snap3_data

    cmd.backup_rm(backup3)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup3)

    cmd.backup_restore(backup1)
    readed = read_dev(dev, offset, length)
    assert readed == snap1_data

    cmd.backup_rm(backup1)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup1)

    cmd.backup_restore(backup2)
    readed = read_dev(dev, offset, length)
    assert readed == snap2_data

    cmd.backup_rm(backup2)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup2)


def test_backup_with_backing_file(backing_dev):  # NOQA
    dev = backing_dev  # NOQA

    offset = 0
    length = 256

    snap0 = cmd.snapshot_create()
    before = read_dev(dev, offset, length)
    assert before != ""

    exists = read_from_backing_file(offset, length)
    assert before == exists

    backup0 = cmd.backup_create(snap0, BACKUP_DEST)

    test_backup(dev)

    cmd.backup_restore(backup0)
    after = read_dev(dev, offset, length)
    assert before == after

    cmd.backup_rm(backup0)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup0)


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
    snap1 = cmd.snapshot_create()

    boundary_data_backup1 = read_dev(dev, boundary_offset, boundary_length)
    hole_data_backup1 = read_dev(dev, hole_offset, hole_length)
    backup1 = cmd.backup_create(snap1, BACKUP_DEST)

    snap2_data = common.random_string(length2)
    common.verify_data(dev, offset2, snap2_data)
    snap2 = cmd.snapshot_create()

    boundary_data_backup2 = read_dev(dev, boundary_offset, boundary_length)
    hole_data_backup2 = read_dev(dev, hole_offset, hole_length)
    backup2 = cmd.backup_create(snap2, BACKUP_DEST)

    cmd.backup_restore(backup1)
    readed = read_dev(dev, boundary_offset, boundary_length)
    assert readed == boundary_data_backup1
    readed = read_dev(dev, hole_offset, hole_length)
    assert readed == hole_data_backup1

    cmd.backup_restore(backup2)
    readed = read_dev(dev, boundary_offset, boundary_length)
    assert readed == boundary_data_backup2
    readed = read_dev(dev, hole_offset, hole_length)
    assert readed == hole_data_backup2
