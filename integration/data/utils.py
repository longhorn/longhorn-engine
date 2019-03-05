import os
import subprocess
import pytest
import cmd
import hashlib


VOLUME_NAME = 'test-volume_1.0'
VOLUME_SIZE = str(4 * 1024 * 1024)  # 4M
BLOCK_SIZE = str(2 * 1024 * 1024)  # 2M
SIZE = 4 * 1024 * 1024


def file(f):
    return os.path.join(_base(), '../../{}'.format(f))


def _base():
    return os.path.dirname(__file__)


def read_file(file_path, offset, length):
    assert os.path.exists(file_path)
    f = open(file_path, 'r')
    f.seek(offset)
    data = f.read(length)
    f.close()
    return data


def create_backup(backup_target, snap):
    backup = cmd.backup_create(snap, backup_target)
    backup_info = cmd.backup_inspect(backup)
    assert backup_info["URL"] == backup
    assert backup_info["VolumeName"] == VOLUME_NAME
    assert backup_info["VolumeSize"] == VOLUME_SIZE
    assert backup_info["SnapshotName"]
    return backup


def rm_backups(*backups):
    for b in backups:
        cmd.backup_rm(b)
        with pytest.raises(subprocess.CalledProcessError):
            cmd.backup_restore(b)
        with pytest.raises(subprocess.CalledProcessError):
            cmd.backup_inspect(b)


def rm_snaps(*snaps):
    for s in snaps:
        cmd.snapshot_rm(s)
        cmd.snapshot_purge()
    snap_info_list = cmd.snapshot_info()
    for s in snaps:
        assert s not in snap_info_list


def checksum_data(data):
    return hashlib.sha512(data).hexdigest()
