import random
from os import path
import pytest
import subprocess
import time

import common.cmd as cmd
from common.core import (  # NOQA
    open_replica, cleanup_controller, cleanup_replica,
    get_dev, get_blockdev, verify_read, verify_data,
    random_string,
    cleanup_replica_dir,
    create_backup, rm_backups,
    wait_for_restore_completion,
    random_length, Snapshot, Data,
    expand_volume_with_frontend,
    wait_and_check_volume_expansion, wait_for_volume_expansion,
    check_block_device_size,
    wait_for_purge_completion,
    verify_no_frontend_data,
    start_no_frontend_volume, cleanup_no_frontend_volume,
    get_backup_volume_url,
)
from common.constants import (
    FIXED_REPLICA_PATH1, FIXED_REPLICA_PATH2,
    VOLUME_NAME, ENGINE_NAME,
    BLOCK_SIZE, VFS_DIR,
    RETRY_COUNTS, RETRY_INTERVAL,
    PAGE_SIZE, SIZE, EXPANDED_SIZE, EXPANDED_SIZE_STR,
)


def test_restore_with_rebuild(
        grpc_controller, grpc_replica1, grpc_replica2,
        grpc_controller_no_frontend,
        grpc_fixed_dir_replica1, grpc_fixed_dir_replica2,
        backup_targets):  # NOQA

    # Pick up a random backup target.
    backup_target = backup_targets[random.randint(0, 1)]

    address = grpc_controller.address
    dr_address = grpc_controller_no_frontend.address

    try:
        cmd.backup_volume_rm(address, VOLUME_NAME, backup_target)
    except Exception:
        pass

    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    start_no_frontend_volume(grpc_controller_no_frontend,
                             grpc_fixed_dir_replica1)

    data0_len = random_length(PAGE_SIZE)
    data0 = Data(random.randrange(0, SIZE-2*PAGE_SIZE, PAGE_SIZE),
                 data0_len, random_string(data0_len))
    snap0 = Snapshot(dev, data0, address)
    backup0_info = create_backup(address, snap0.name, backup_target)
    assert backup0_info["VolumeName"] == VOLUME_NAME
    assert backup0_info["Size"] == str(BLOCK_SIZE)

    cmd.backup_restore(dr_address, backup0_info["URL"])
    wait_for_restore_completion(dr_address, backup0_info["URL"])
    verify_no_frontend_data(data0.offset, data0.content,
                            grpc_controller_no_frontend)

    open_replica(grpc_fixed_dir_replica2)
    cmd.add_replica(dr_address, grpc_fixed_dir_replica2.url,
                    restore=True)

    replicas = grpc_controller_no_frontend.replica_list()
    assert len(replicas) == 2
    rw_replica, wo_replica = 0, 0
    for r in replicas:
        if r.mode == 'RW':
            rw_replica += 1
        else:
            assert r.mode == "WO"
            wo_replica += 1
    assert rw_replica == 1 and wo_replica == 1

    # The old replica will fail the restore but the error won't be recorded.
    # Then rebuilding replica will start full restore.
    with pytest.raises(subprocess.CalledProcessError) as e:
        cmd.backup_restore(dr_address, backup0_info["URL"])
    assert "already restored backup" in e.value.stdout
    wait_for_restore_completion(dr_address, backup0_info["URL"])

    # Need to manually verify the rebuilding replica for the restore volume
    cmd.verify_rebuild_replica(dr_address, grpc_fixed_dir_replica2.url)
    replicas = grpc_controller_no_frontend.replica_list()
    assert len(replicas) == 2
    for r in replicas:
        assert r.mode == 'RW'

    # Delete the old replica then check if the rebuilt replica works fine.
    cleanup_replica(grpc_fixed_dir_replica1)
    grpc_controller_no_frontend.replica_delete(grpc_fixed_dir_replica1.address)
    verify_no_frontend_data(data0.offset, data0.content,
                            grpc_controller_no_frontend)

    cmd.backup_volume_rm(grpc_controller.address,
                         VOLUME_NAME, backup_target)


def test_restore_incrementally(
        grpc_controller, grpc_replica1, grpc_replica2,
        grpc_controller_no_frontend,
        grpc_fixed_dir_replica1, grpc_fixed_dir_replica2,
        backup_targets):  # NOQA
    for backup_target in backup_targets:
        restore_inc_test(
            grpc_controller, grpc_replica1, grpc_replica2,
            grpc_controller_no_frontend,
            grpc_fixed_dir_replica1, grpc_fixed_dir_replica2,
            backup_target)


def restore_inc_test(
        grpc_controller, grpc_replica1, grpc_replica2,
        grpc_dr_controller, grpc_dr_replica1, grpc_dr_replica2,
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
    start_no_frontend_volume(grpc_dr_controller,
                             grpc_dr_replica1, grpc_dr_replica2)

    # mock restore crash/error:
    # By adding attribute `immutable`, Longhorn cannot create a file
    # for the restore. Then the following restore command will fail.
    command = ["chattr", "+i", FIXED_REPLICA_PATH1]
    subprocess.check_output(command).strip()
    command = ["chattr", "+i", FIXED_REPLICA_PATH2]
    subprocess.check_output(command).strip()
    with pytest.raises(subprocess.CalledProcessError) as e:
        cmd.backup_restore(dr_address, backup0)
    assert "operation not permitted" in e.value.stdout

    # the restore status will be reverted/keep unchanged
    # if an error is triggered before the actual restore is performed
    rs = cmd.restore_status(dr_address)
    for status in rs.values():
        assert not status["isRestoring"]
        assert not status['backupURL']
        assert 'error' not in status
        assert 'progress' not in status
        assert not status['state']

    command = ["chattr", "-i", FIXED_REPLICA_PATH1]
    subprocess.check_output(command).strip()
    command = ["chattr", "-i", FIXED_REPLICA_PATH2]
    subprocess.check_output(command).strip()
    cmd.sync_agent_server_reset(dr_address)

    cmd.backup_restore(dr_address, backup0)
    wait_for_restore_completion(dr_address, backup0)
    verify_no_frontend_data(0, snap0_data, grpc_dr_controller)

    data1 = \
        snap0_data[0:offset1] + snap1_data + \
        snap0_data[offset1+length1:]
    cmd.backup_restore(dr_address, backup1)
    wait_for_restore_completion(dr_address, backup1)
    verify_no_frontend_data(0, data1, grpc_dr_controller)
    delta_file1 = "volume-delta-" + backup0_name + ".img"
    assert not path.exists(FIXED_REPLICA_PATH1 + delta_file1)
    assert not path.exists(FIXED_REPLICA_PATH2 + delta_file1)
    status = cmd.restore_status(dr_address)
    compare_last_restored_with_backup(status, backup1_name)

    data2 = \
        data1[0:offset2] + snap2_data + \
        zero_string * (BLOCK_SIZE - length2 - offset2) + snap2_data
    cmd.backup_restore(dr_address, backup2)
    wait_for_restore_completion(dr_address, backup2)
    verify_no_frontend_data(0, data2, grpc_dr_controller)
    delta_file2 = "volume-delta-" + backup1_name + ".img"
    assert not path.exists(FIXED_REPLICA_PATH1 + delta_file2)
    assert not path.exists(FIXED_REPLICA_PATH2 + delta_file2)
    status = cmd.restore_status(dr_address)
    compare_last_restored_with_backup(status, backup2_name)

    # mock race condition: duplicate inc restore calls
    with pytest.raises(subprocess.CalledProcessError) as e:
        cmd.backup_restore(dr_address, backup2)
        wait_for_restore_completion(dr_address, backup2)
    assert "already restored backup" in e.value.stdout

    data3 = zero_string * length3 + data2[length3:length2]
    cmd.backup_restore(dr_address, backup3)
    wait_for_restore_completion(dr_address, backup3)
    verify_no_frontend_data(0, data3, grpc_dr_controller)
    delta_file3 = "volume-delta-" + backup3_name + ".img"
    assert not path.exists(FIXED_REPLICA_PATH1 + delta_file3)
    assert not path.exists(FIXED_REPLICA_PATH2 + delta_file3)
    status = cmd.restore_status(dr_address)
    compare_last_restored_with_backup(status, backup3_name)

    # mock corner case: invalid last-restored backup
    rm_backups(address, ENGINE_NAME, [backup3])

    # This inc restore will fall back to full restore
    cmd.backup_restore(dr_address, backup4)
    wait_for_restore_completion(dr_address, backup4)
    verify_no_frontend_data(0, snap4_data, grpc_dr_controller)
    status = cmd.restore_status(dr_address)
    compare_last_restored_with_backup(status, backup4_name)
    # check if the tmp files during this special full restore are cleaned up.
    if "vfs" in backup_target:
        command = ["find", VFS_DIR, "-type", "d", "-name", VOLUME_NAME]
        backup_volume_path = subprocess.check_output(command).strip()
        command = ["find", backup_volume_path, "-name", "*snap_tmp"]
        tmp_files = subprocess.check_output(command).split()
        assert len(tmp_files) == 0

    cleanup_no_frontend_volume(grpc_dr_controller,
                               grpc_dr_replica1, grpc_dr_replica2)

    rm_backups(address, ENGINE_NAME, [backup0, backup1, backup2, backup4])

    cmd.sync_agent_server_reset(address)
    cmd.sync_agent_server_reset(dr_address)
    cleanup_controller(grpc_controller)
    cleanup_replica(grpc_replica1)
    cleanup_replica(grpc_replica2)


def test_inc_restore_with_rebuild_and_expansion(
        grpc_controller, grpc_replica1, grpc_replica2,
        grpc_controller_no_frontend,
        grpc_fixed_dir_replica1, grpc_fixed_dir_replica2,
        backup_targets):  # NOQA

    # Pick up a random backup target.
    backup_target = backup_targets[random.randint(0, 1)]

    address = grpc_controller.address
    dr_address = grpc_controller_no_frontend.address

    try:
        cmd.backup_volume_rm(address, VOLUME_NAME, backup_target)
    except Exception:
        pass

    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    start_no_frontend_volume(grpc_controller_no_frontend,
                             grpc_fixed_dir_replica1)

    data0_len = random_length(PAGE_SIZE)
    data0 = Data(random.randrange(0, SIZE-2*PAGE_SIZE, PAGE_SIZE),
                 data0_len, random_string(data0_len))
    snap0 = Snapshot(dev, data0, address)

    backup0_info = create_backup(address, snap0.name, backup_target)
    assert backup0_info["VolumeName"] == VOLUME_NAME
    assert backup0_info["Size"] == str(BLOCK_SIZE)

    cmd.backup_restore(dr_address, backup0_info["URL"])
    wait_for_restore_completion(dr_address, backup0_info["URL"])
    verify_no_frontend_data(data0.offset, data0.content,
                            grpc_controller_no_frontend)

    expand_volume_with_frontend(grpc_controller, EXPANDED_SIZE)
    wait_and_check_volume_expansion(
        grpc_controller, EXPANDED_SIZE)

    data1_len = random_length(PAGE_SIZE)
    data1 = Data(random.randrange(SIZE, EXPANDED_SIZE-PAGE_SIZE, PAGE_SIZE),
                 data1_len, random_string(data1_len))
    snap1 = Snapshot(dev, data1, address)

    backup1_info = create_backup(address, snap1.name,
                                 backup_target, EXPANDED_SIZE_STR)
    assert backup1_info["VolumeName"] == VOLUME_NAME
    assert backup1_info["Size"] == str(2*BLOCK_SIZE)

    backup_volumes = cmd.backup_volume_list(address, VOLUME_NAME,
                                            backup_target)
    assert VOLUME_NAME in backup_volumes
    url = get_backup_volume_url(backup_target, VOLUME_NAME)
    backup_info = cmd.backup_inspect_volume(address, url)
    assert backup_info["Size"] == EXPANDED_SIZE_STR

    # restore command invocation should error out
    with pytest.raises(subprocess.CalledProcessError) as e:
        cmd.backup_restore(dr_address, backup1_info["URL"])
    assert "need to expand the DR volume" in e.value.stdout

    # The above restore error is triggered before calling the replicas.
    # Hence the error won't be recorded in the restore status
    # and we can continue restoring backups for the DR volume.
    rs = cmd.restore_status(dr_address)
    for status in rs.values():
        assert status['backupURL'] == backup0_info["URL"]
        assert status['lastRestored'] == backup0_info["Name"]
        assert 'error' not in status.keys()
        assert not status["isRestoring"]

    grpc_controller_no_frontend.volume_expand(EXPANDED_SIZE)
    wait_for_volume_expansion(grpc_controller_no_frontend, EXPANDED_SIZE)

    # This restore command will trigger snapshot purge.
    # And the error is triggered before calling the replicas.
    with pytest.raises(subprocess.CalledProcessError) as e:
        cmd.backup_restore(dr_address, backup1_info["URL"])
    assert "found more than 1 snapshot in the replicas, " \
           "hence started to purge snapshots before the restore" \
           in e.value.stdout
    wait_for_purge_completion(dr_address)

    snaps_info = cmd.snapshot_info(dr_address)
    assert len(snaps_info) == 2
    volume_head_name = "volume-head"
    snap_name = "expand-" + EXPANDED_SIZE_STR
    head_info = snaps_info[volume_head_name]
    assert head_info["name"] == volume_head_name
    assert head_info["parent"] == snap_name
    assert not head_info["children"]
    assert head_info["usercreated"] is False
    snap_info = snaps_info[snap_name]
    assert snap_info["name"] == snap_name
    assert not snap_info["parent"]
    assert volume_head_name in snap_info["children"]
    assert snap_info["usercreated"] is False

    cmd.backup_restore(dr_address, backup1_info["URL"])
    wait_for_restore_completion(dr_address, backup1_info["URL"])
    verify_no_frontend_data(data1.offset, data1.content,
                            grpc_controller_no_frontend)

    # For DR volume, the rebuilding replica won't be expanded automatically.
    open_replica(grpc_fixed_dir_replica2)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.add_replica(dr_address, grpc_fixed_dir_replica2.url,
                        restore=True)

    # Manually expand the rebuilding replica then retry `add-replica`.
    grpc_fixed_dir_replica2.replica_open()
    grpc_fixed_dir_replica2.replica_expand(EXPANDED_SIZE)
    grpc_fixed_dir_replica2.replica_close()
    cmd.add_replica(dr_address, grpc_fixed_dir_replica2.url, restore=True)

    replicas = grpc_controller_no_frontend.replica_list()
    assert len(replicas) == 2
    rw_replica, wo_replica = 0, 0
    for r in replicas:
        if r.mode == 'RW':
            rw_replica += 1
        else:
            assert r.mode == "WO"
            wo_replica += 1
    assert rw_replica == 1 and wo_replica == 1

    # The old replica will fail the restore but the error won't be recorded.
    # Then rebuilding replica will start full restore.
    with pytest.raises(subprocess.CalledProcessError) as e:
        cmd.backup_restore(dr_address, backup1_info["URL"])
    assert "already restored backup" in e.value.stdout
    wait_for_restore_completion(dr_address, backup1_info["URL"])

    cmd.verify_rebuild_replica(dr_address, grpc_fixed_dir_replica2.url)
    replicas = grpc_controller_no_frontend.replica_list()
    assert len(replicas) == 2
    for r in replicas:
        assert r.mode == 'RW'

    verify_no_frontend_data(data1.offset, data1.content,
                            grpc_controller_no_frontend)

    cmd.backup_volume_rm(grpc_controller.address,
                         VOLUME_NAME, backup_target)


def test_inc_restore_failure_delta_file_cleanup_error(
        grpc_controller, grpc_replica1, grpc_replica2,
        grpc_controller_no_frontend,
        grpc_fixed_dir_replica1, grpc_fixed_dir_replica2,
        backup_targets):  # NOQA
    for backup_target in backup_targets:
        inc_restore_failure_cleanup_error_test(
            grpc_controller, grpc_replica1, grpc_replica2,
            grpc_controller_no_frontend,
            grpc_fixed_dir_replica1, grpc_fixed_dir_replica2,
            backup_target)


def inc_restore_failure_cleanup_error_test(
        grpc_controller, grpc_replica1, grpc_replica2,
        grpc_dr_controller, grpc_dr_replica1, grpc_dr_replica2,
        backup_target):  # NOQA
    address = grpc_controller.address

    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    zero_string = b'\x00'.decode('utf-8')

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

    # start dr volume (no frontend)
    dr_address = grpc_dr_controller.address
    start_no_frontend_volume(grpc_dr_controller,
                             grpc_dr_replica1, grpc_dr_replica2)

    cmd.backup_restore(dr_address, backup0)
    wait_for_restore_completion(dr_address, backup0)
    verify_no_frontend_data(0, snap0_data, grpc_dr_controller)

    # mock inc restore crash/error: cannot clean up the delta file path
    delta_file = "volume-delta-" + backup0_name + ".img"
    command = ["mkdir", "-p", FIXED_REPLICA_PATH1 + delta_file + "/dir"]
    subprocess.check_output(command).strip()
    command = ["mkdir", "-p", FIXED_REPLICA_PATH2 + delta_file + "/dir"]
    subprocess.check_output(command).strip()
    with pytest.raises(subprocess.CalledProcessError) as e:
        cmd.backup_restore(dr_address, backup1)
    assert "failed to clean up the existing file" in e.value.stdout
    command = ["rm", "-r", FIXED_REPLICA_PATH1 + delta_file]
    subprocess.check_output(command).strip()
    command = ["rm", "-r", FIXED_REPLICA_PATH2 + delta_file]
    subprocess.check_output(command).strip()

    # the restore status will be reverted/keep unchanged
    # if an error is triggered before the actual restore is performed
    rs = cmd.restore_status(dr_address)
    for status in rs.values():
        assert not status["isRestoring"]
        assert status['backupURL'] == backup0
        assert 'error' not in status
        assert status['progress'] == 100
        assert status['state'] == "complete"

    cleanup_no_frontend_volume(grpc_dr_controller,
                               grpc_dr_replica1, grpc_dr_replica2)
    rm_backups(address, ENGINE_NAME, [backup0, backup1])
    cmd.sync_agent_server_reset(address)
    cmd.sync_agent_server_reset(dr_address)
    cleanup_controller(grpc_controller)
    cleanup_replica(grpc_replica1)
    cleanup_replica(grpc_replica2)


def test_inc_restore_failure_invalid_block(
        grpc_controller, grpc_replica1, grpc_replica2,
        grpc_controller_no_frontend,
        grpc_fixed_dir_replica1, grpc_fixed_dir_replica2,
        backup_targets):  # NOQA
    # This case is for vfs backup only
    for backup_target in backup_targets:
        if "vfs" in backup_target:
            break
    assert backup_target

    address = grpc_controller.address

    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    zero_string = b'\x00'.decode('utf-8')

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

    # start dr volume (no frontend)
    dr_address = grpc_controller_no_frontend.address
    start_no_frontend_volume(grpc_controller_no_frontend,
                             grpc_fixed_dir_replica1, grpc_fixed_dir_replica2)

    cmd.backup_restore(dr_address, backup0)
    wait_for_restore_completion(dr_address, backup0)
    verify_no_frontend_data(0, snap0_data, grpc_controller_no_frontend)

    # mock inc restore error: invalid block
    delta_file = "volume-delta-" + backup0_name + ".img"
    command = ["find", VFS_DIR, "-type", "d", "-name", VOLUME_NAME]
    backup_volume_path = subprocess.check_output(command).strip()
    command = ["find", backup_volume_path, "-name", "*blk"]
    blocks = subprocess.check_output(command).split()
    assert len(blocks) != 0
    for blk in blocks:
        command = ["mv", blk, blk+".tmp".encode('utf-8')]
        subprocess.check_output(command).strip()
    cmd.backup_restore(dr_address, backup1)
    # restore status should contain the error info
    failed_restore, finished_restore = 0, 0
    for i in range(RETRY_COUNTS):
        failed_restore, finished_restore = 0, 0
        rs = cmd.restore_status(dr_address)
        for status in rs.values():
            if status['backupURL'] != backup1:
                break
            if 'error' in status.keys():
                if status['error'] != "":
                    assert 'no such file or directory' in \
                           status['error']
                    failed_restore += 1
            if not status["isRestoring"]:
                finished_restore += 1
        if failed_restore == 2 and finished_restore == 2:
            break
        time.sleep(RETRY_INTERVAL)
    assert failed_restore == 2 and finished_restore == 2

    assert path.exists(FIXED_REPLICA_PATH1 + delta_file)
    assert path.exists(FIXED_REPLICA_PATH2 + delta_file)
    for blk in blocks:
        command = ["mv", blk+".tmp".encode('utf-8'), blk]
        subprocess.check_output(command)

    cleanup_no_frontend_volume(grpc_controller_no_frontend,
                               grpc_fixed_dir_replica1,
                               grpc_fixed_dir_replica2)
    rm_backups(address, ENGINE_NAME, [backup0, backup1])
    cmd.sync_agent_server_reset(address)
    cmd.sync_agent_server_reset(dr_address)
    cleanup_controller(grpc_controller)
    cleanup_replica(grpc_replica1)
    cleanup_replica(grpc_replica2)


def compare_last_restored_with_backup(restore_status, backup):
    for status in restore_status.values():
        assert status['lastRestored'] == backup
