from os import path

import pytest
import subprocess
import common
import cmd
import launcher
from launcher import LAUNCHER, LAUNCHER_NO_FRONTEND
from common import grpc_controller, grpc_controller_no_frontend  # NOQA
from common import grpc_replica1, grpc_replica2   # NOQA
from common import grpc_standby_replica1, grpc_standby_replica2   # NOQA
from common import backup_targets  # NOQA
from common import open_replica
from common import VOLUME_NAME, VOLUME2_NAME
from common import STANDBY_REPLICA1_PATH, STANDBY_REPLICA2_PATH
from common import get_blockdev, verify_read, verify_data
from cmd import CONTROLLER_NO_FRONTEND
from utils import create_backup, rm_backups


FRONTEND_TGT_BLOCKDEV = "tgt-blockdev"
BLOCK_SIZE = 2 * 1024 * 1024
VFS_DIR = "/data/backupbucket/"


def test_restore_incrementally(grpc_controller, grpc_controller_no_frontend,  # NOQA
                               grpc_replica1, grpc_replica2,  # NOQA
                               grpc_standby_replica1, grpc_standby_replica2,  # NOQA
                               backup_targets):  # NOQA
    for backup_target in backup_targets:
        restore_inc_test(grpc_controller,
                         grpc_replica1, grpc_replica2,
                         grpc_controller_no_frontend,
                         grpc_standby_replica1, grpc_standby_replica2,
                         backup_target)
    launcher.start_engine_frontend(FRONTEND_TGT_BLOCKDEV, url=LAUNCHER)


def restore_inc_test(grpc_controller,  # NOQA
                     grpc_replica1, grpc_replica2,  # NOQA
                     grpc_sb_controller,  # NOQA
                     grpc_sb_replica1, grpc_sb_replica2,  # NOQA
                     backup_target):  # NOQA
    launcher.start_engine_frontend(FRONTEND_TGT_BLOCKDEV,
                                   url=LAUNCHER)
    dev = common.get_dev(grpc_replica1, grpc_replica2,
                         grpc_controller)

    zero_string = b'\x00'.decode('utf-8')

    # backup0: 256 random data in 1st block
    length0 = 256
    snap0_data = common.random_string(length0)
    verify_data(dev, 0, snap0_data)
    verify_data(dev, BLOCK_SIZE, snap0_data)
    snap0 = cmd.snapshot_create()
    backup0 = create_backup(backup_target, snap0)
    backup0_name = cmd.backup_inspect(backup0)['Name']

    # backup1: 32 random data + 32 zero data + 192 random data in 1st block
    length1 = 32
    offset1 = 32
    snap1_data = zero_string * length1
    verify_data(dev, offset1, snap1_data)
    snap1 = cmd.snapshot_create()
    backup1 = create_backup(backup_target, snap1)
    backup1_name = cmd.backup_inspect(backup1)['Name']

    # backup2: 32 random data + 256 random data in 1st block,
    #          256 random data in 2nd block
    length2 = 256
    offset2 = 32
    snap2_data = common.random_string(length2)
    verify_data(dev, offset2, snap2_data)
    verify_data(dev, BLOCK_SIZE, snap2_data)
    snap2 = cmd.snapshot_create()
    backup2 = create_backup(backup_target, snap2)
    backup2_name = cmd.backup_inspect(backup2)['Name']

    # backup3: 64 zero data + 192 random data in 1st block
    length3 = 64
    offset3 = 0
    verify_data(dev, offset3, zero_string * length3)
    verify_data(dev, length2, zero_string * offset2)
    verify_data(dev, BLOCK_SIZE, zero_string * length2)
    snap3 = cmd.snapshot_create()
    backup3 = create_backup(backup_target, snap3)
    backup3_name = cmd.backup_inspect(backup3)['Name']

    # backup4: 256 random data in 1st block
    length4 = 256
    offset4 = 0
    snap4_data = common.random_string(length4)
    verify_data(dev, offset4, snap4_data)
    snap4 = cmd.snapshot_create()
    backup4 = create_backup(backup_target, snap4)
    backup4_name = cmd.backup_inspect(backup4)['Name']

    common.cleanup_replica(grpc_replica1)
    common.cleanup_replica(grpc_replica2)
    common.cleanup_controller(grpc_controller)
    launcher.shutdown_engine_frontend(url=LAUNCHER)

    # start no-frontend volume
    # start standby volume (no frontend)
    start_no_frontend_volume(grpc_sb_controller,
                             grpc_sb_replica1, grpc_sb_replica2)

    restore_for_no_frontend_volume(backup0, grpc_sb_controller)
    verify_no_frontend_data(0, snap0_data, grpc_sb_controller)

    # mock restore crash/error
    delta_file1 = "volume-delta-" + backup0_name + ".img"
    if "vfs" in backup_target:
        command = ["find", VFS_DIR, "-type", "d", "-name", VOLUME_NAME]
        backup_volume_path = subprocess.check_output(command).strip()
        command = ["find", backup_volume_path, "-name", "*blk"]
        blocks = subprocess.check_output(command).split()
        assert len(blocks) != 0
        for blk in blocks:
            command = ["mv", blk, blk+".tmp"]
            subprocess.check_output(command).strip()
        with pytest.raises(subprocess.CalledProcessError):
            cmd.restore_inc(backup1, backup0_name, CONTROLLER_NO_FRONTEND)
        assert path.exists(STANDBY_REPLICA1_PATH + delta_file1)
        assert path.exists(STANDBY_REPLICA2_PATH + delta_file1)
        for blk in blocks:
            command = ["mv", blk+".tmp", blk]
            subprocess.check_output(command)

    data1 = \
        snap0_data[0:offset1] + snap1_data + \
        snap0_data[offset1+length1:]
    cmd.restore_inc(backup1, backup0_name, CONTROLLER_NO_FRONTEND)
    verify_no_frontend_data(0, data1, grpc_sb_controller)

    assert not path.exists(STANDBY_REPLICA1_PATH + delta_file1)
    assert not path.exists(STANDBY_REPLICA2_PATH + delta_file1)
    volume_info = cmd.info(CONTROLLER_NO_FRONTEND)
    assert volume_info['lastRestored'] == backup1_name

    data2 = \
        data1[0:offset2] + snap2_data + \
        zero_string * (BLOCK_SIZE - length2 - offset2) + snap2_data
    cmd.restore_inc(backup2, backup1_name, CONTROLLER_NO_FRONTEND)
    verify_no_frontend_data(0, data2, grpc_sb_controller)

    delta_file2 = "volume-delta-" + backup1_name + ".img"
    assert not path.exists(STANDBY_REPLICA1_PATH + delta_file2)
    assert not path.exists(STANDBY_REPLICA2_PATH + delta_file2)
    volume_info = cmd.info(CONTROLLER_NO_FRONTEND)
    assert volume_info['lastRestored'] == backup2_name

    # mock race condition
    with pytest.raises(subprocess.CalledProcessError) as e:
        cmd.restore_inc(backup1, backup0_name, CONTROLLER_NO_FRONTEND)
        assert "doesn't match lastRestored" in e

    data3 = zero_string * length3 + data2[length3:length2]
    cmd.restore_inc(backup3, backup2_name, CONTROLLER_NO_FRONTEND)
    verify_no_frontend_data(0, data3, grpc_sb_controller)

    delta_file3 = "volume-delta-" + backup3_name + ".img"
    assert not path.exists(STANDBY_REPLICA1_PATH + delta_file3)
    assert not path.exists(STANDBY_REPLICA2_PATH + delta_file3)
    volume_info = cmd.info(CONTROLLER_NO_FRONTEND)
    assert volume_info['lastRestored'] == backup3_name

    # mock corner case: invalid last-restored backup
    rm_backups([backup3])
    # actually it is full restoration
    cmd.restore_inc(backup4, backup3_name, CONTROLLER_NO_FRONTEND)
    verify_no_frontend_data(0, snap4_data, grpc_sb_controller)
    volume_info = cmd.info(CONTROLLER_NO_FRONTEND)
    assert volume_info['lastRestored'] == backup4_name
    if "vfs" in backup_target:
        command = ["find", VFS_DIR, "-type", "d", "-name", VOLUME_NAME]
        backup_volume_path = subprocess.check_output(command).strip()
        command = ["find", backup_volume_path, "-name", "*tempoary"]
        tmp_files = subprocess.check_output(command).split()
        assert len(tmp_files) == 0

    cleanup_no_frontend_volume(grpc_sb_controller,
                               grpc_sb_replica1, grpc_sb_replica2)

    rm_backups([backup0, backup1, backup2, backup4])


def restore_for_no_frontend_volume(backup, grpc_c):
    launcher.start_engine_frontend(FRONTEND_TGT_BLOCKDEV,
                                   url=LAUNCHER_NO_FRONTEND)
    v = grpc_c.volume_get()
    assert v.frontendState == "up"

    cmd.backup_restore(backup, CONTROLLER_NO_FRONTEND)

    launcher.shutdown_engine_frontend(url=LAUNCHER_NO_FRONTEND)
    v = grpc_c.volume_get()
    assert v.frontendState == "down"


def verify_no_frontend_data(data_offset, data, grpc_c):
    launcher.start_engine_frontend(FRONTEND_TGT_BLOCKDEV,
                                   url=LAUNCHER_NO_FRONTEND)
    v = grpc_c.volume_get()
    assert v.frontendState == "up"

    dev = get_blockdev(volume=VOLUME2_NAME)
    verify_read(dev, data_offset, data)

    launcher.shutdown_engine_frontend(url=LAUNCHER_NO_FRONTEND)
    v = grpc_c.volume_get()
    assert v.frontendState == "down"


def start_no_frontend_volume(grpc_c, grpc_r1, grpc_r2):
    launcher.start_engine_frontend(FRONTEND_TGT_BLOCKDEV,
                                   url=LAUNCHER_NO_FRONTEND)

    open_replica(grpc_r1)
    open_replica(grpc_r2)

    standby_replicas = grpc_c.replica_list()
    assert len(standby_replicas) == 0

    grpc_c.volume_get()
    v = grpc_c.volume_start(replicas=[
        common.STANDBY_REPLICA1,
        common.STANDBY_REPLICA2
    ])
    assert v.replicaCount == 2

    launcher.shutdown_engine_frontend(url=LAUNCHER_NO_FRONTEND)
    v = grpc_c.volume_get()
    assert v.frontendState == "down"


def cleanup_no_frontend_volume(grpc_c, grpc_r1, grpc_r2):
    launcher.start_engine_frontend(FRONTEND_TGT_BLOCKDEV,
                                   url=LAUNCHER_NO_FRONTEND)
    v = grpc_c.volume_get()
    assert v.frontendState == "up"

    common.cleanup_replica(grpc_r1)
    common.cleanup_replica(grpc_r2)
    common.cleanup_controller(grpc_c)

    launcher.shutdown_engine_frontend(url=LAUNCHER_NO_FRONTEND)
    v = grpc_c.volume_get()
    assert v.frontendState == "down"

    cleanup_replica_path(STANDBY_REPLICA1_PATH)
    cleanup_replica_path(STANDBY_REPLICA2_PATH)


def cleanup_replica_path(path):
    command = ["find", path]
    files = subprocess.check_output(command).split()
    for f in files:
        if f == path:
            continue
        command = ["rm", "-f", f]
        subprocess.check_output(command).strip()
