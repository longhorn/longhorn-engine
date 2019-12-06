import random
from os import path
import pytest
import subprocess
import time

import cmd
from common import (  # NOQA
    backup_targets,  # NOQA
    grpc_engine_manager,  # NOQA
    grpc_controller, grpc_controller_no_frontend,  # NOQA
    grpc_replica1, grpc_replica2,  # NOQA
    grpc_fixed_dir_replica1, grpc_fixed_dir_replica2,  # NOQA
    open_replica, cleanup_controller, cleanup_replica,
    get_dev, get_blockdev, verify_read, verify_data,
    random_string,
    cleanup_replica_dir,
    create_backup, rm_backups,
    restore_incrementally, wait_for_restore_completion,
    random_length, Snapshot, Data,
    wait_for_volume_expansion, check_block_device_size,
)
from setting import (
    FIXED_REPLICA_PATH1, FIXED_REPLICA_PATH2,
    VOLUME_NAME, VOLUME_NO_FRONTEND_NAME,
    ENGINE_NAME, ENGINE_NO_FRONTEND_NAME,
    BLOCK_SIZE, FRONTEND_TGT_BLOCKDEV, VFS_DIR,
    RETRY_COUNTS, RETRY_INTERVAL,
    PAGE_SIZE, SIZE, EXPAND_SIZE, EXPAND_SIZE_STR,
)


def test_restore_incrementally(grpc_engine_manager,  # NOQA
                               grpc_controller,  # NOQA
                               grpc_controller_no_frontend,  # NOQA
                               grpc_replica1, grpc_replica2,  # NOQA
                               grpc_fixed_dir_replica1,  # NOQA
                               grpc_fixed_dir_replica2,  # NOQA
                               backup_targets):  # NOQA
    for backup_target in backup_targets:
        restore_inc_test(grpc_engine_manager,
                         grpc_controller,
                         grpc_replica1, grpc_replica2,
                         grpc_controller_no_frontend,
                         grpc_fixed_dir_replica1,
                         grpc_fixed_dir_replica2,
                         backup_target)


def restore_inc_test(grpc_engine_manager,  # NOQA
                     grpc_controller,  # NOQA
                     grpc_replica1, grpc_replica2,  # NOQA
                     grpc_dr_controller,  # NOQA
                     grpc_dr_replica1, grpc_dr_replica2,  # NOQA
                     backup_target):  # NOQA
    address = grpc_controller.address

    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    zero_string = b'\x00'.decode('utf-8')

    # backup0: 256 random data in 1st block
    length0 = 256
    snap0_data = random_string(length0)
    verify_data(dev, 0, snap0_data)
    verify_data(dev, BLOCK_SIZE, snap0_data)
    snap0 = cmd.snapshot_create(address)
    backup0 = create_backup(address, snap0, backup_target)["URL"]
    backup0_name = cmd.backup_inspect(address, backup0)['Name']

    # backup1: 32 random data + 32 zero data + 192 random data in 1st block
    length1 = 32
    offset1 = 32
    snap1_data = zero_string * length1
    verify_data(dev, offset1, snap1_data)
    snap1 = cmd.snapshot_create(address)
    backup1 = create_backup(address, snap1, backup_target)["URL"]
    backup1_name = cmd.backup_inspect(address, backup1)['Name']

    # backup2: 32 random data + 256 random data in 1st block,
    #          256 random data in 2nd block
    length2 = 256
    offset2 = 32
    snap2_data = random_string(length2)
    verify_data(dev, offset2, snap2_data)
    verify_data(dev, BLOCK_SIZE, snap2_data)
    snap2 = cmd.snapshot_create(address)
    backup2 = create_backup(address, snap2, backup_target)["URL"]
    backup2_name = cmd.backup_inspect(address, backup2)['Name']

    # backup3: 64 zero data + 192 random data in 1st block
    length3 = 64
    offset3 = 0
    verify_data(dev, offset3, zero_string * length3)
    verify_data(dev, length2, zero_string * offset2)
    verify_data(dev, BLOCK_SIZE, zero_string * length2)
    snap3 = cmd.snapshot_create(address)
    backup3 = create_backup(address, snap3, backup_target)["URL"]
    backup3_name = cmd.backup_inspect(address, backup3)['Name']

    # backup4: 256 random data in 1st block
    length4 = 256
    offset4 = 0
    snap4_data = random_string(length4)
    verify_data(dev, offset4, snap4_data)
    snap4 = cmd.snapshot_create(address)
    backup4 = create_backup(address, snap4, backup_target)["URL"]
    backup4_name = cmd.backup_inspect(address, backup4)['Name']

    # start no-frontend volume
    # start dr volume (no frontend)
    dr_address = grpc_dr_controller.address
    start_no_frontend_volume(grpc_engine_manager,
                             grpc_dr_controller,
                             grpc_dr_replica1, grpc_dr_replica2)

    cmd.backup_restore(dr_address, backup0)
    wait_for_restore_completion(dr_address, backup0)
    verify_no_frontend_data(grpc_engine_manager,
                            0, snap0_data, grpc_dr_controller)

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
        # should fail
        is_failed = False
        cmd.restore_inc(dr_address, backup1, backup0_name)
        for i in range(RETRY_COUNTS):
            rs = cmd.restore_status(dr_address)
            for status in rs.values():
                if status['backupURL'] != backup1:
                    break
                if 'error' in status.keys():
                    if status['error'] != "":
                        assert 'no such file or directory' in \
                               status['error']
                        is_failed = True
            if is_failed:
                break
            time.sleep(RETRY_INTERVAL)
        assert is_failed

        assert path.exists(FIXED_REPLICA_PATH1 + delta_file1)
        assert path.exists(FIXED_REPLICA_PATH2 + delta_file1)
        for blk in blocks:
            command = ["mv", blk+".tmp", blk]
            subprocess.check_output(command)

    data1 = \
        snap0_data[0:offset1] + snap1_data + \
        snap0_data[offset1+length1:]
    # race condition: last restoration has failed
    # but `isRestoring` hasn't been cleanup
    for i in range(RETRY_COUNTS):
        try:
            restore_incrementally(dr_address, backup1, backup0_name)
            break
        except subprocess.CalledProcessError as e:
            if "already in progress" not in e.output:
                time.sleep(RETRY_INTERVAL)
            else:
                raise e

    verify_no_frontend_data(grpc_engine_manager,
                            0, data1, grpc_dr_controller)

    assert not path.exists(FIXED_REPLICA_PATH1 + delta_file1)
    assert not path.exists(FIXED_REPLICA_PATH2 + delta_file1)

    status = cmd.restore_status(dr_address)
    compare_last_restored_with_backup(status, backup1_name)

    data2 = \
        data1[0:offset2] + snap2_data + \
        zero_string * (BLOCK_SIZE - length2 - offset2) + snap2_data
    restore_incrementally(dr_address, backup2, backup1_name)
    verify_no_frontend_data(grpc_engine_manager,
                            0, data2, grpc_dr_controller)

    delta_file2 = "volume-delta-" + backup1_name + ".img"
    assert not path.exists(FIXED_REPLICA_PATH1 + delta_file2)
    assert not path.exists(FIXED_REPLICA_PATH2 + delta_file2)

    status = cmd.restore_status(dr_address)
    compare_last_restored_with_backup(status, backup2_name)

    # mock race condition
    with pytest.raises(subprocess.CalledProcessError) as e:
        restore_incrementally(dr_address, backup1, backup0_name)
        assert "doesn't match lastRestored" in e

    data3 = zero_string * length3 + data2[length3:length2]
    restore_incrementally(dr_address, backup3, backup2_name)
    verify_no_frontend_data(grpc_engine_manager,
                            0, data3, grpc_dr_controller)

    delta_file3 = "volume-delta-" + backup3_name + ".img"
    assert not path.exists(FIXED_REPLICA_PATH1 + delta_file3)
    assert not path.exists(FIXED_REPLICA_PATH2 + delta_file3)
    status = cmd.restore_status(dr_address)
    compare_last_restored_with_backup(status, backup3_name)

    # mock corner case: invalid last-restored backup
    rm_backups(address, ENGINE_NAME, [backup3])
    # actually it is full restoration
    restore_incrementally(dr_address, backup4, backup3_name)
    verify_no_frontend_data(grpc_engine_manager,
                            0, snap4_data, grpc_dr_controller)
    status = cmd.restore_status(dr_address)
    compare_last_restored_with_backup(status, backup4_name)

    if "vfs" in backup_target:
        command = ["find", VFS_DIR, "-type", "d", "-name", VOLUME_NAME]
        backup_volume_path = subprocess.check_output(command).strip()
        command = ["find", backup_volume_path, "-name", "*tempoary"]
        tmp_files = subprocess.check_output(command).split()
        assert len(tmp_files) == 0

    cleanup_no_frontend_volume(grpc_engine_manager,
                               grpc_dr_controller,
                               grpc_dr_replica1, grpc_dr_replica2)

    rm_backups(address, ENGINE_NAME, [backup0, backup1, backup2, backup4])

    cmd.sync_agent_server_reset(address)
    cleanup_controller(grpc_controller)
    cleanup_replica(grpc_replica1)
    cleanup_replica(grpc_replica2)


def compare_last_restored_with_backup(restore_status, backup):
    for status in restore_status.values():
        assert status['lastRestored'] == backup


def verify_no_frontend_data(grpc_em, data_offset, data, grpc_c):
    grpc_em.frontend_start(ENGINE_NO_FRONTEND_NAME,
                           FRONTEND_TGT_BLOCKDEV)
    v = grpc_c.volume_get()
    assert v.frontendState == "up"

    dev = get_blockdev(volume=VOLUME_NO_FRONTEND_NAME)
    verify_read(dev, data_offset, data)

    grpc_em.frontend_shutdown(ENGINE_NO_FRONTEND_NAME)
    v = grpc_c.volume_get()
    assert v.frontendState == "down"
    ep = grpc_em.engine_get(ENGINE_NO_FRONTEND_NAME)
    assert ep.spec.frontend == ""


def start_no_frontend_volume(grpc_em, grpc_c, grpc_r1, grpc_r2):
    grpc_em.frontend_start(ENGINE_NO_FRONTEND_NAME,
                           FRONTEND_TGT_BLOCKDEV)

    open_replica(grpc_r1)
    open_replica(grpc_r2)

    dr_replicas = grpc_c.replica_list()
    assert len(dr_replicas) == 0

    r1_url = grpc_r1.url
    r2_url = grpc_r2.url
    v = grpc_c.volume_start(replicas=[r1_url, r2_url])
    assert v.replicaCount == 2

    grpc_em.frontend_shutdown(ENGINE_NO_FRONTEND_NAME)
    v = grpc_c.volume_get()
    assert v.frontendState == "down"
    ep = grpc_em.engine_get(ENGINE_NO_FRONTEND_NAME)
    assert ep.spec.frontend == ""


def cleanup_no_frontend_volume(grpc_em, grpc_c, grpc_r1, grpc_r2):
    grpc_em.frontend_start(ENGINE_NO_FRONTEND_NAME,
                           FRONTEND_TGT_BLOCKDEV)
    v = grpc_c.volume_get()
    assert v.frontendState == "up"

    cmd.sync_agent_server_reset(grpc_c.address)

    grpc_em.frontend_shutdown(ENGINE_NO_FRONTEND_NAME)
    v = grpc_c.volume_get()
    assert v.frontendState == "down"
    ep = grpc_em.engine_get(ENGINE_NO_FRONTEND_NAME)
    assert ep.spec.frontend == ""

    cleanup_controller(grpc_c)
    cleanup_replica(grpc_r1)
    cleanup_replica(grpc_r2)

    cleanup_replica_dir(FIXED_REPLICA_PATH1)
    cleanup_replica_dir(FIXED_REPLICA_PATH2)


def volume_expansion_with_backup_test(grpc_engine_manager,  # NOQA
                                      grpc_controller,  # NOQA
                                      grpc_dr_controller,  # NOQA
                                      grpc_replica1, grpc_replica2,  # NOQA
                                      grpc_dr_replica1,  # NOQA
                                      grpc_dr_replica2,  # NOQA
                                      volume_name, engine_name, backup_target):  # NOQA
    address = grpc_controller.address
    dr_address = grpc_dr_controller.address
    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)
    start_no_frontend_volume(grpc_engine_manager, grpc_dr_controller,
                             grpc_dr_replica1, grpc_dr_replica2)

    try:
        cmd.backup_volume_rm(address, volume_name, backup_target)
    except Exception:
        pass

    data0_len = random_length(PAGE_SIZE)
    data0 = Data(random.randrange(0, SIZE-2*PAGE_SIZE, PAGE_SIZE),
                 data0_len, random_string(data0_len))
    snap0 = Snapshot(dev, data0, address)

    backup0_info = create_backup(address, snap0.name, backup_target)
    assert backup0_info["VolumeName"] == volume_name
    assert backup0_info["Size"] == str(BLOCK_SIZE)

    cmd.backup_restore(dr_address, backup0_info["URL"])
    wait_for_restore_completion(dr_address, backup0_info["URL"])
    verify_no_frontend_data(grpc_engine_manager,
                            data0.offset, data0.content,
                            grpc_dr_controller)

    grpc_controller.volume_expand(EXPAND_SIZE)
    wait_for_volume_expansion(grpc_controller, EXPAND_SIZE)
    check_block_device_size(volume_name, EXPAND_SIZE)

    data1_len = random_length(PAGE_SIZE)
    data1 = Data(random.randrange(SIZE, EXPAND_SIZE-PAGE_SIZE, PAGE_SIZE),
                 data1_len, random_string(data1_len))
    snap1 = Snapshot(dev, data1, address)

    backup1_info = create_backup(address, snap1.name,
                                 backup_target, EXPAND_SIZE_STR)
    assert backup1_info["VolumeName"] == volume_name
    assert backup1_info["Size"] == str(2*BLOCK_SIZE)

    backup_volumes = cmd.backup_volume_list(address, volume_name,
                                            backup_target)
    assert volume_name in backup_volumes
    assert backup_volumes[volume_name]["Size"] == EXPAND_SIZE_STR

    # incremental restoration will implicitly expand the volume first
    restore_incrementally(dr_address,
                          backup1_info["URL"], backup0_info["Name"])
    check_dr_volume_block_device_size(grpc_engine_manager,
                                      grpc_dr_controller, EXPAND_SIZE)
    verify_no_frontend_data(grpc_engine_manager,
                            data1.offset, data1.content,
                            grpc_dr_controller)

    cmd.backup_volume_rm(grpc_controller.address,
                         volume_name, backup_target)


def test_expansion_with_inc_restore(grpc_engine_manager,  # NOQA
                                    grpc_controller,  # NOQA
                                    grpc_controller_no_frontend,  # NOQA
                                    grpc_replica1, grpc_replica2,  # NOQA
                                    grpc_fixed_dir_replica1,  # NOQA
                                    grpc_fixed_dir_replica2,  # NOQA
                                    backup_targets):  # NOQA

    # Pick up a random backup target. We cannot test both targets in
    # one test case. Since the engine/replica processes in the instance
    # manager cannot be cleaned up inside the test case.
    backup_target = backup_targets[random.randint(0, 1)]
    volume_expansion_with_backup_test(grpc_engine_manager,
                                      grpc_controller,
                                      grpc_controller_no_frontend,
                                      grpc_replica1, grpc_replica2,
                                      grpc_fixed_dir_replica1,
                                      grpc_fixed_dir_replica2,
                                      VOLUME_NAME, ENGINE_NAME,
                                      backup_target)


def check_dr_volume_block_device_size(grpc_em, grpc_c, size):
    grpc_em.frontend_start(ENGINE_NO_FRONTEND_NAME,
                           FRONTEND_TGT_BLOCKDEV)
    v = grpc_c.volume_get()
    assert v.frontendState == "up"

    get_blockdev(volume=VOLUME_NO_FRONTEND_NAME)
    check_block_device_size(VOLUME_NO_FRONTEND_NAME, size)

    grpc_em.frontend_shutdown(ENGINE_NO_FRONTEND_NAME)
    v = grpc_c.volume_get()
    assert v.frontendState == "down"
    ep = grpc_em.engine_get(ENGINE_NO_FRONTEND_NAME)
    assert ep.spec.frontend == ""
