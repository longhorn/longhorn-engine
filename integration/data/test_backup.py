import subprocess

import pytest

import cmd
import common
from common import dev, backing_dev, read_dev, write_dev, \
        read_from_backing_file, BACKUP_DEST


def test_backup(dev):
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

    res3 = cmd.backup_restore(backup3)
    readed = read_dev(dev, offset, length)
    assert readed == snap3_data

    cmd.backup_rm(backup3)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup3)

    res1 = cmd.backup_restore(backup1)
    readed = read_dev(dev, offset, length)
    assert readed == snap1_data

    cmd.backup_rm(backup1)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup1)

    res2 = cmd.backup_restore(backup2)
    readed = read_dev(dev, offset, length)
    assert readed == snap2_data

    cmd.backup_rm(backup2)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup2)


def test_backup_with_backing_file(backing_dev):
    dev = backing_dev

    offset = 0
    length = 256

    snap0 = cmd.snapshot_create()
    before = read_dev(dev, offset, length)
    assert before != ""

    exists = read_from_backing_file(offset, length)
    assert before == exists

    backup0 = cmd.backup_create(snap0, BACKUP_DEST)

    test_backup(dev)

    res0 = cmd.backup_restore(backup0)
    after = read_dev(dev, offset, length)
    assert before == after

    cmd.backup_rm(backup0)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_restore(backup0)

