import subprocess

import pytest

import cmd
import common
from common import dev, SIZE, read_dev, write_dev


BACKUP_DIR = '/tmp/longhorn-backup'
BACKUP_DEST = 'vfs://' + BACKUP_DIR


def setup_module():
    common.prepare_backup_dir(BACKUP_DIR)


def test_backup_create_rm_restore(dev):
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
