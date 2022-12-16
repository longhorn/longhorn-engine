import os
import json
import pytest
import subprocess
from pathlib import Path
import common.cmd as cmd
from common.core import (  # NOQA
    cleanup_controller, cleanup_replica,
    get_blockdev, get_dev, read_dev,
    read_from_backing_file,
    random_string, verify_data, checksum_dev,
    create_backup, rm_backups,
    restore_with_frontend, open_replica,
    reset_volume,
    get_backup_volume_url,
)
from data.snapshot_tree import (
    snapshot_tree_build, snapshot_tree_verify_backup_node
)
from common.constants import (
    VOLUME_NAME, VOLUME_BACKING_NAME,
    VOLUME2_NAME, ENGINE2_NAME, REPLICA_2_NAME,
    ENGINE_NAME, ENGINE_BACKING_NAME, BACKUP_DIR,
    BLOCK_SIZE, BLOCK_SIZE_STR,
    MESSAGE_TYPE_ERROR,
)
from common.util import (
    finddir, findfile
)

LOCK_REFRESH_INTERVAL = 60
LOCK_MAX_WAIT_TIME = 30
DELETE_LOCK = "lock_delete"
BACKUP_LOCK = "lock_backup"
RESTORE_LOCK = "lock_restore"
BACKING_IMAGE_NAME = "backing-image-test"
BACKING_IMAGE_CHECKSUM = \
    "FakeBackingImageSHA512Checksum"


def backup_test(
        grpc_r1, grpc_r2, grpc_c,
        volume_name, engine_name, backup_target):
    address = grpc_c.address
    dev = get_blockdev(volume_name)

    offset = 0
    length = 128

    snap1_data = random_string(length)
    verify_data(dev, offset, snap1_data)
    snap1_checksum = checksum_dev(dev)
    snap1 = cmd.snapshot_create(address)

    backup1_info = create_backup(address, snap1, backup_target)
    assert backup1_info["VolumeName"] == volume_name
    assert backup1_info["Size"] == BLOCK_SIZE_STR

    snap2_data = random_string(length)
    verify_data(dev, offset, snap2_data)
    snap2_checksum = checksum_dev(dev)
    snap2 = cmd.snapshot_create(address)

    backup2_info = create_backup(address, snap2, backup_target)
    assert backup2_info["VolumeName"] == volume_name
    assert backup2_info["Size"] == BLOCK_SIZE_STR

    snap3_data = random_string(length)
    verify_data(dev, offset, snap3_data)
    snap3_checksum = checksum_dev(dev)
    snap3 = cmd.snapshot_create(address)

    backup3_info = create_backup(address, snap3, backup_target)
    assert backup3_info["VolumeName"] == volume_name
    assert backup3_info["Size"] == BLOCK_SIZE_STR

    reset_volume(grpc_c, grpc_r1, grpc_r2)
    dev = get_blockdev(volume_name)
    restore_with_frontend(address, engine_name,
                          backup3_info["URL"])

    readed = read_dev(dev, offset, length)
    assert readed == snap3_data
    c = checksum_dev(dev)
    assert c == snap3_checksum

    rm_backups(address, engine_name, [backup3_info["URL"]])

    reset_volume(grpc_c, grpc_r1, grpc_r2)
    dev = get_blockdev(volume_name)
    restore_with_frontend(address, engine_name,
                          backup1_info["URL"])
    readed = read_dev(dev, offset, length)
    assert readed == snap1_data
    c = checksum_dev(dev)
    assert c == snap1_checksum

    rm_backups(address, engine_name, [backup1_info["URL"]])

    reset_volume(grpc_c, grpc_r1, grpc_r2)
    dev = get_blockdev(volume_name)
    restore_with_frontend(address, engine_name,
                          backup2_info["URL"])
    readed = read_dev(dev, offset, length)
    assert readed == snap2_data
    c = checksum_dev(dev)
    assert c == snap2_checksum

    rm_backups(address, engine_name, [backup2_info["URL"]])


def backup_with_backing_file_test(backup_target,  # NOQA
                                  grpc_backing_controller,  # NOQA
                                  grpc_backing_replica1,  # NOQA
                                  grpc_backing_replica2):  # NOQA
    address = grpc_backing_controller.address

    dev = get_dev(grpc_backing_replica1, grpc_backing_replica2,
                  grpc_backing_controller)

    offset = 0
    length = 256

    snap0 = cmd.snapshot_create(address)
    before = read_dev(dev, offset, length)
    assert before != ""
    snap0_checksum = checksum_dev(dev)

    exists = read_from_backing_file(offset, length)
    assert before == exists

    backup0_info = create_backup(address, snap0, backup_target,
                                 backing_image_name=BACKING_IMAGE_NAME,
                                 backing_image_checksum=BACKING_IMAGE_CHECKSUM)
    assert backup0_info["VolumeName"] == VOLUME_BACKING_NAME

    backup_test(
        grpc_backing_replica1, grpc_backing_replica2,
        grpc_backing_controller,
        VOLUME_BACKING_NAME, ENGINE_BACKING_NAME, backup_target)

    reset_volume(grpc_backing_controller,
                 grpc_backing_replica1, grpc_backing_replica2)
    dev = get_blockdev(VOLUME_BACKING_NAME)
    restore_with_frontend(address, ENGINE_BACKING_NAME,
                          backup0_info["URL"])
    after = read_dev(dev, offset, length)
    assert before == after
    c = checksum_dev(dev)
    assert c == snap0_checksum

    rm_backups(address, ENGINE_BACKING_NAME, [backup0_info["URL"]])


def backup_hole_with_backing_file_test(backup_target,  # NOQA
                                       grpc_backing_controller,  # NOQA
                                       grpc_backing_replica1,  # NOQA
                                       grpc_backing_replica2):  # NOQA
    address = grpc_backing_controller.address
    dev = get_dev(grpc_backing_replica1, grpc_backing_replica2,
                  grpc_backing_controller)
    volume_name = grpc_backing_controller.volume_get().name
    assert volume_name == VOLUME_BACKING_NAME

    offset1 = 512
    length1 = 256

    offset2 = 640
    length2 = 256

    boundary_offset = 0
    boundary_length = 4100  # just pass 4096 into next 4k

    hole_offset = 2 * 1024 * 1024
    hole_length = 1024

    snap1_data = random_string(length1)
    verify_data(dev, offset1, snap1_data)
    snap1_checksum = checksum_dev(dev)
    snap1 = cmd.snapshot_create(address)

    boundary_data_backup1 = read_dev(dev, boundary_offset, boundary_length)
    hole_data_backup1 = read_dev(dev, hole_offset, hole_length)
    backup1_info = create_backup(address, snap1, backup_target,
                                 backing_image_name=BACKING_IMAGE_NAME,
                                 backing_image_checksum=BACKING_IMAGE_CHECKSUM)

    snap2_data = random_string(length2)
    verify_data(dev, offset2, snap2_data)
    snap2_checksum = checksum_dev(dev)
    snap2 = cmd.snapshot_create(address)

    boundary_data_backup2 = read_dev(dev, boundary_offset, boundary_length)
    hole_data_backup2 = read_dev(dev, hole_offset, hole_length)
    backup2_info = create_backup(address, snap2, backup_target,
                                 backing_image_name=BACKING_IMAGE_NAME,
                                 backing_image_checksum=BACKING_IMAGE_CHECKSUM)
    assert backup2_info["VolumeBackingImageName"] == BACKING_IMAGE_NAME

    reset_volume(grpc_backing_controller,
                 grpc_backing_replica1, grpc_backing_replica2)
    dev = get_blockdev(volume_name)
    restore_with_frontend(address, ENGINE_BACKING_NAME,
                          backup1_info["URL"])
    readed = read_dev(dev, boundary_offset, boundary_length)
    assert readed == boundary_data_backup1
    readed = read_dev(dev, hole_offset, hole_length)
    assert readed == hole_data_backup1
    c = checksum_dev(dev)
    assert c == snap1_checksum

    reset_volume(grpc_backing_controller,
                 grpc_backing_replica1, grpc_backing_replica2)
    dev = get_blockdev(volume_name)
    restore_with_frontend(address, ENGINE_BACKING_NAME,
                          backup2_info["URL"])
    readed = read_dev(dev, boundary_offset, boundary_length)
    assert readed == boundary_data_backup2
    readed = read_dev(dev, hole_offset, hole_length)
    assert readed == hole_data_backup2
    c = checksum_dev(dev)
    assert c == snap2_checksum


def snapshot_tree_backup_test(backup_target, engine_name,  # NOQA
                              grpc_controller, grpc_replica1, grpc_replica2):  # NOQA
    address = grpc_controller.address

    dev = get_dev(grpc_replica1, grpc_replica2,
                  grpc_controller)
    offset = 0
    length = 128
    backup = {}

    snap, data = snapshot_tree_build(dev, address, engine_name,
                                     offset, length)

    backup["0b"] = create_backup(address, snap["0b"], backup_target)["URL"]
    backup["0c"] = create_backup(address, snap["0c"], backup_target)["URL"]
    backup["1c"] = create_backup(address, snap["1c"], backup_target)["URL"]
    backup["2b"] = create_backup(address, snap["2b"], backup_target)["URL"]
    backup["2c"] = create_backup(address, snap["2c"], backup_target)["URL"]
    backup["3c"] = create_backup(address, snap["3c"], backup_target)["URL"]

    snapshot_tree_verify_backup_node(grpc_controller,
                                     grpc_replica1, grpc_replica2,
                                     address, engine_name,
                                     offset, length, backup, data, "0b")
    snapshot_tree_verify_backup_node(grpc_controller,
                                     grpc_replica1, grpc_replica2,
                                     address, engine_name,
                                     offset, length, backup, data, "0c")
    snapshot_tree_verify_backup_node(grpc_controller,
                                     grpc_replica1, grpc_replica2,
                                     address, engine_name,
                                     offset, length, backup, data, "1c")
    snapshot_tree_verify_backup_node(grpc_controller,
                                     grpc_replica1, grpc_replica2,
                                     address, engine_name,
                                     offset, length, backup, data, "2b")
    snapshot_tree_verify_backup_node(grpc_controller,
                                     grpc_replica1, grpc_replica2,
                                     address, engine_name,
                                     offset, length, backup, data, "2c")
    snapshot_tree_verify_backup_node(grpc_controller,
                                     grpc_replica1, grpc_replica2,
                                     address, engine_name,
                                     offset, length, backup, data, "3c")


def test_backup(grpc_replica1, grpc_replica2,  # NOQA
                grpc_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        get_dev(grpc_replica1, grpc_replica2, grpc_controller)
        backup_test(
            grpc_replica1, grpc_replica2, grpc_controller,
            VOLUME_NAME, ENGINE_NAME, backup_target)
        cmd.sync_agent_server_reset(grpc_controller.address)
        cleanup_controller(grpc_controller)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)


def test_backup_S3_latest_unavailable(grpc_replica1, grpc_replica2,  # NOQA
                grpc_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        if "s3://" not in backup_target:
            continue
        dev = get_dev(grpc_replica1, grpc_replica2,
                      grpc_controller)
        address = grpc_controller.address
        volume_name = VOLUME_NAME
        engine_name = ENGINE_NAME
        offset = 0
        length = 128

        # initial backup
        snap1_data = random_string(length)
        verify_data(dev, offset, snap1_data)
        snap1_checksum = checksum_dev(dev)
        snap1 = cmd.snapshot_create(address)
        backup1_info = create_backup(address, snap1, backup_target)

        # backup to be unavailable
        snap2_data = random_string(length)
        verify_data(dev, offset, snap2_data)
        snap2 = cmd.snapshot_create(address)
        backup2_info = create_backup(address, snap2, backup_target)

        # the gc after the restore will clean up the missing backup
        cfg = findfile(BACKUP_DIR, "backup_" + backup2_info["Name"] + ".cfg")
        os.remove(cfg)

        # final full backup after unavailable backup
        snap3_data = random_string(length)
        verify_data(dev, offset, snap3_data)
        snap3_checksum = checksum_dev(dev)
        snap3 = cmd.snapshot_create(address)
        backup3_info = create_backup(address, snap3, backup_target)
        assert backup3_info["VolumeName"] == volume_name
        assert backup3_info["Size"] == BLOCK_SIZE_STR

        # write some stuff on head
        head_data = random_string(length)
        verify_data(dev, offset, head_data)

        # test restore of the initial backup
        reset_volume(grpc_controller,
                     grpc_replica1, grpc_replica2)
        dev = get_blockdev(volume_name)
        restore_with_frontend(address, engine_name,
                              backup1_info["URL"])
        readed = read_dev(dev, offset, length)
        assert readed == snap1_data
        c = checksum_dev(dev)
        assert c == snap1_checksum

        # test a restore for the final backup
        reset_volume(grpc_controller,
                     grpc_replica1, grpc_replica2)
        dev = get_blockdev(volume_name)
        restore_with_frontend(address, engine_name,
                              backup3_info["URL"])
        readed = read_dev(dev, offset, length)
        assert readed == snap3_data
        c = checksum_dev(dev)
        assert c == snap3_checksum

        rm_backups(address, engine_name, [backup1_info["URL"],
                                          backup3_info["URL"]])
        cmd.sync_agent_server_reset(address)
        cleanup_controller(grpc_controller)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)


def test_backup_incremental_logic(grpc_replica1, grpc_replica2,
                                  grpc_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        dev = get_dev(grpc_replica1, grpc_replica2,
                      grpc_controller)
        address = grpc_controller.address
        volume_name = VOLUME_NAME
        engine_name = ENGINE_NAME
        offset = 0
        length = 128

        # initial backup
        snap1_data = random_string(length)
        verify_data(dev, offset, snap1_data)
        snap1_checksum = checksum_dev(dev)
        snap1 = cmd.snapshot_create(address)
        backup1_info = create_backup(address, snap1, backup_target)
        assert backup1_info["IsIncremental"] is False

        # delta backup on top of initial backup
        snap2_data = random_string(int(length / 2))
        verify_data(dev, offset, snap2_data)
        snap2 = cmd.snapshot_create(address)
        backup2_info = create_backup(address, snap2, backup_target)
        assert backup2_info["IsIncremental"] is True

        # delete the volume
        cmd.sync_agent_server_reset(address)
        grpc_controller = cleanup_controller(grpc_controller)
        grpc_replica1 = cleanup_replica(grpc_replica1)
        grpc_replica2 = cleanup_replica(grpc_replica2)

        # recreate the volume
        dev = get_dev(grpc_replica1, grpc_replica2,
                      grpc_controller, clean_backup_dir=False)

        # empty initial backup after volume recreation
        snap3 = cmd.snapshot_create(address)
        backup3_info = create_backup(address, snap3, backup_target)
        assert backup3_info["VolumeName"] == volume_name
        assert backup3_info["Size"] == '0'
        assert backup3_info["IsIncremental"] is False

        # write half of snap1 onto head
        snap4_data = snap1_data[:int(length / 2)]
        assert len(snap4_data) == int(length / 2)
        verify_data(dev, offset, snap4_data)
        snap4_checksum = checksum_dev(dev)
        assert snap4_checksum != snap1_checksum
        snap4 = cmd.snapshot_create(address)
        backup4_info = create_backup(address, snap4, backup_target)
        assert backup4_info["IsIncremental"] is True

        # restore initial backup
        reset_volume(grpc_controller,
                     grpc_replica1, grpc_replica2)
        dev = get_blockdev(volume_name)
        restore_with_frontend(address, engine_name,
                              backup1_info["URL"])
        assert read_dev(dev, offset, length) == snap1_data
        assert checksum_dev(dev) == snap1_checksum

        # restore final backup (half of snap1)
        reset_volume(grpc_controller,
                     grpc_replica1, grpc_replica2)
        dev = get_blockdev(volume_name)
        restore_with_frontend(address, engine_name,
                              backup4_info["URL"])
        assert checksum_dev(dev) == snap4_checksum
        assert snap4_checksum != snap1_checksum
        data = read_dev(dev, offset, length)
        assert data[:int(length / 2)] == snap4_data
        assert data[int(length / 2):] == '\x00' * int(length / 2)

        rm_backups(address, engine_name, [backup1_info["URL"],
                                          backup2_info["URL"],
                                          backup3_info["URL"],
                                          backup4_info["URL"]])
        cmd.sync_agent_server_reset(address)
        cleanup_controller(grpc_controller)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)


def test_snapshot_tree_backup(grpc_replica1, grpc_replica2,  # NOQA
                              grpc_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        snapshot_tree_backup_test(backup_target, ENGINE_NAME,
                                  grpc_controller,
                                  grpc_replica1, grpc_replica2)
        cmd.sync_agent_server_reset(grpc_controller.address)
        cleanup_controller(grpc_controller)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)


def test_backup_with_backing_file_qcow2(grpc_backing_qcow2_replica1, grpc_backing_qcow2_replica2, grpc_backing_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        backup_with_backing_file_test(backup_target,
                                      grpc_backing_controller,
                                      grpc_backing_qcow2_replica1,
                                      grpc_backing_qcow2_replica2)
        cmd.sync_agent_server_reset(grpc_backing_controller.address)
        cleanup_controller(grpc_backing_controller)
        cleanup_replica(grpc_backing_qcow2_replica1)
        cleanup_replica(grpc_backing_qcow2_replica2)


def test_backup_with_backing_file_raw(grpc_backing_raw_replica1, grpc_backing_raw_replica2, grpc_backing_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        backup_with_backing_file_test(backup_target,
                                      grpc_backing_controller,
                                      grpc_backing_raw_replica1,
                                      grpc_backing_raw_replica2)
        cmd.sync_agent_server_reset(grpc_backing_controller.address)
        cleanup_controller(grpc_backing_controller)
        cleanup_replica(grpc_backing_raw_replica1)
        cleanup_replica(grpc_backing_raw_replica2)


def test_backup_hole_with_backing_file_qcow2(grpc_backing_qcow2_replica1, grpc_backing_qcow2_replica2, grpc_backing_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        backup_hole_with_backing_file_test(backup_target,
                                           grpc_backing_controller,
                                           grpc_backing_qcow2_replica1,
                                           grpc_backing_qcow2_replica2)
        cmd.sync_agent_server_reset(grpc_backing_controller.address)
        cleanup_controller(grpc_backing_controller)
        cleanup_replica(grpc_backing_qcow2_replica1)
        cleanup_replica(grpc_backing_qcow2_replica2)


def test_backup_hole_with_backing_file_raw(grpc_backing_raw_replica1, grpc_backing_raw_replica2, grpc_backing_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        backup_hole_with_backing_file_test(backup_target,
                                           grpc_backing_controller,
                                           grpc_backing_raw_replica1,
                                           grpc_backing_raw_replica2)
        cmd.sync_agent_server_reset(grpc_backing_controller.address)
        cleanup_controller(grpc_backing_controller)
        cleanup_replica(grpc_backing_raw_replica1)
        cleanup_replica(grpc_backing_raw_replica2)


def check_backup_volume_block_count(address, volume, backup_target, expected):
    # check the volume block & size
    url = get_backup_volume_url(backup_target, volume)
    info = cmd.backup_inspect_volume(address, url)
    assert info["DataStored"] == str(BLOCK_SIZE * expected)

    # check the blocks on disk
    volume_dir = finddir(BACKUP_DIR, volume)
    assert os.path.exists(volume_dir)
    block_count = 0
    block_dir = os.path.join(volume_dir, "blocks")
    if os.path.exists(block_dir):
        for _ in Path(block_dir).rglob("*.blk"):
            block_count += 1
    assert block_count == expected


def test_backup_block_deletion(grpc_replica1, grpc_replica2,  # NOQA
                               grpc_controller, backup_targets):  # NOQA
    address = grpc_controller.address
    length = 128

    for backup_target in backup_targets:
        dev = get_dev(grpc_replica1, grpc_replica2,
                      grpc_controller)

        # write two backup block
        verify_data(dev, 0, random_string(length))
        verify_data(dev, BLOCK_SIZE, random_string(length))
        snap = cmd.snapshot_create(address)

        backup1 = create_backup(address, snap, backup_target)
        assert backup1["VolumeName"] == VOLUME_NAME
        assert backup1["Size"] == str(BLOCK_SIZE * 2)
        assert snap in backup1["SnapshotName"]

        # test block deduplication
        backup1_duplicate = create_backup(address, snap, backup_target)
        assert backup1_duplicate["VolumeName"] == VOLUME_NAME
        assert backup1_duplicate["Size"] == str(BLOCK_SIZE * 2)
        assert snap in backup1_duplicate["SnapshotName"]
        check_backup_volume_block_count(address, VOLUME_NAME,
                                        backup_target, 2)

        # overwrite second backup block
        verify_data(dev, BLOCK_SIZE, random_string(length))
        snap = cmd.snapshot_create(address)

        backup2 = create_backup(address, snap, backup_target)
        assert backup2["VolumeName"] == VOLUME_NAME
        assert backup2["Size"] == str(BLOCK_SIZE * 2)
        assert snap in backup2["SnapshotName"]

        # check that the volume now has 3 blocks
        # backup1 and backup2 share the first block
        # and have different second blocks
        check_backup_volume_block_count(address, VOLUME_NAME,
                                        backup_target, 3)

        # remove backup 1 duplicate
        # this should not change the blocks on disk
        # since all blocks are still required
        cmd.backup_rm(address, backup1_duplicate["URL"])
        check_backup_volume_block_count(address, VOLUME_NAME,
                                        backup_target, 3)

        # remove backup 1
        # the volume should now have 2 blocks
        # blk1 from backup1 should still be present
        # since it's required by backup 2
        cmd.backup_rm(address, backup1["URL"])
        check_backup_volume_block_count(address, VOLUME_NAME,
                                        backup_target, 2)

        # remove the last remaining backup 2
        # this should remove all blocks
        cmd.backup_rm(address, backup2["URL"])
        check_backup_volume_block_count(address, VOLUME_NAME,
                                        backup_target, 0)

        # clean up the backup volume
        cmd.backup_volume_rm(address, VOLUME_NAME, backup_target)
        url = get_backup_volume_url(backup_target, VOLUME_NAME)
        with pytest.raises(subprocess.CalledProcessError):
            cmd.backup_inspect_volume(address, url)
        cmd.sync_agent_server_reset(address)
        cleanup_controller(grpc_controller)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)


def create_in_progress_backup_file(volume):
    volume_dir = finddir(BACKUP_DIR, volume)
    assert os.path.exists(volume_dir)
    backup_cfg_dir = os.path.join(volume_dir, "backups")

    name = "backup-" + random_string(16)
    backup_cfg_path = os.path.join(backup_cfg_dir, "backup_" + name + ".cfg")
    cfg = json.dumps({"Name": name, "VolumeName": volume, "CreatedTime": ""})
    file = open(backup_cfg_path, "w")
    file.write(cfg)
    file.close()
    return backup_cfg_path


def test_backup_block_no_cleanup(grpc_replica1, grpc_replica2,  # NOQA
                               grpc_controller, backup_targets):  # NOQA
    address = grpc_controller.address
    length = 128

    for backup_target in backup_targets:
        dev = get_dev(grpc_replica1, grpc_replica2,
                      grpc_controller)

        # write two backup blocks
        verify_data(dev, 0, random_string(length))
        verify_data(dev, BLOCK_SIZE, random_string(length))
        snap = cmd.snapshot_create(address)

        backup1 = create_backup(address, snap, backup_target)
        assert backup1["VolumeName"] == VOLUME_NAME
        assert backup1["Size"] == str(BLOCK_SIZE * 2)
        assert snap in backup1["SnapshotName"]
        check_backup_volume_block_count(address, VOLUME_NAME,
                                        backup_target, 2)

        # overwrite second backup block
        verify_data(dev, BLOCK_SIZE, random_string(length))
        snap = cmd.snapshot_create(address)

        backup2 = create_backup(address, snap, backup_target)
        assert backup2["VolumeName"] == VOLUME_NAME
        assert backup2["Size"] == str(BLOCK_SIZE * 2)
        assert snap in backup2["SnapshotName"]

        # check that the volume now has 3 blocks
        # backup1 and backup2 share the first block
        # and have different second blocks
        check_backup_volume_block_count(address, VOLUME_NAME,
                                        backup_target, 3)

        # create an artificial in progress backup
        # that will stop the gc from removing blocks
        in_progress_backup_file = create_in_progress_backup_file(VOLUME_NAME)

        # remove backup 1 the volume should still have 3 blocks
        cmd.backup_rm(address, backup1["URL"])
        check_backup_volume_block_count(address, VOLUME_NAME,
                                        backup_target, 3)

        # remove the in progress backup
        os.remove(in_progress_backup_file)

        # remove the last remaining backup 2
        # this should remove all blocks
        # including the orphaned block from backup 1
        cmd.backup_rm(address, backup2["URL"])
        check_backup_volume_block_count(address, VOLUME_NAME,
                                        backup_target, 0)

        # clean up the backup volume
        cmd.backup_volume_rm(address, VOLUME_NAME, backup_target)
        url = get_backup_volume_url(backup_target, VOLUME_NAME)
        with pytest.raises(subprocess.CalledProcessError):
            cmd.backup_inspect_volume(address, url)
        cmd.sync_agent_server_reset(address)
        cleanup_controller(grpc_controller)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)


def test_backup_corrupt_deletion(grpc_replica1, grpc_replica2,  # NOQA
                                  grpc_controller, backup_targets):  # NOQA
    address = grpc_controller.address
    length = 128

    for backup_target in backup_targets:
        dev = get_dev(grpc_replica1, grpc_replica2,
                      grpc_controller)

        # write two backup blocks
        verify_data(dev, 0, random_string(length))
        verify_data(dev, BLOCK_SIZE, random_string(length))
        snap = cmd.snapshot_create(address)
        backup1 = create_backup(address, snap, backup_target)

        # overwrite second backup block
        verify_data(dev, BLOCK_SIZE, random_string(length))
        snap = cmd.snapshot_create(address)
        backup2 = create_backup(address, snap, backup_target)

        # check that the volume now has 3 blocks
        # backup1 and backup2 share the first block
        # and have different second blocks
        check_backup_volume_block_count(address, VOLUME_NAME,
                                        backup_target, 3)

        # corrupt backup1 config
        cfg = findfile(BACKUP_DIR, "backup_" + backup1["Name"] + ".cfg")
        corrupt_backup = open(cfg, "w")
        assert corrupt_backup
        assert corrupt_backup.write("{corrupt: definitely") > 0
        corrupt_backup.close()
        cmd.backup_rm(address, backup1["URL"])

        # check that the volume now has 2 blocks
        # backup2 still relies on the backup1 first block
        check_backup_volume_block_count(address, VOLUME_NAME,
                                        backup_target, 2)

        # remove backup 2 and check that all blocks are deleted
        cmd.backup_rm(address, backup2["URL"])
        check_backup_volume_block_count(address, VOLUME_NAME,
                                        backup_target, 0)

        # remove volume.cfg then delete the backup volume
        cfg = findfile(finddir(BACKUP_DIR, VOLUME_NAME), "volume.cfg")
        os.remove(cfg)
        cmd.backup_volume_rm(address, VOLUME_NAME, backup_target)
        url = get_backup_volume_url(backup_target, VOLUME_NAME)
        with pytest.raises(subprocess.CalledProcessError):
            cmd.backup_inspect_volume(address, url)
        cmd.sync_agent_server_reset(address)
        cleanup_controller(grpc_controller)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)


def test_backup_volume_deletion(grpc_replica1, grpc_replica2,  # NOQA
                                grpc_controller, backup_targets):  # NOQA
    offset = 0
    length = 128
    address = grpc_controller.address

    for backup_target in backup_targets:
        dev = get_dev(grpc_replica1, grpc_replica2,
                      grpc_controller)
        snap_data = random_string(length)
        verify_data(dev, offset, snap_data)
        snap = cmd.snapshot_create(address)

        backup_info = create_backup(address, snap, backup_target)
        assert backup_info["VolumeName"] == VOLUME_NAME
        assert backup_info["Size"] == BLOCK_SIZE_STR
        assert snap in backup_info["SnapshotName"]

        cmd.backup_volume_rm(address, VOLUME_NAME, backup_target)
        url = get_backup_volume_url(backup_target, VOLUME_NAME)
        with pytest.raises(subprocess.CalledProcessError):
            cmd.backup_inspect_volume(address, url)

        cmd.sync_agent_server_reset(address)
        cleanup_controller(grpc_controller)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)


def test_backup_volume_list(grpc_replica_client, grpc_controller_client,  # NOQA
                            grpc_replica1, grpc_replica2,  # NOQA
                            grpc_controller, backup_targets):  # NOQA
    """
    Test backup volume list

    Context:

    We want to make sure that an error when listing a single backup volume
    does not stop us from listing all the other backup volumes. Otherwise a
    single faulty backup can block the retrieval of all known backup volumes.

    Steps:

    1.  Create a volume(1,2) and attach to the current node
    2.  write some data to volume(1,2)
    3.  Create a backup(1) of volume(1,2)
    4.  request a backup list
    5.  verify backup list contains no error messages for volume(1,2)
    6.  verify backup list contains backup(1) for volume(1,2)
    7.  place a file named "backup_1234@failure.cfg"
        into the backups folder of volume(1)
    8.  request a backup list
    9.  verify backup list contains no error messages for volume(1,2)
    10. verify backup list contains backup(1) for volume(1,2)
    11. delete backup volumes(1 & 2)
    12. cleanup
    """

    # create a second volume
    grpc2_replica1 = grpc_replica_client(REPLICA_2_NAME + "-1")
    grpc2_replica2 = grpc_replica_client(REPLICA_2_NAME + "-2")
    grpc2_controller = grpc_controller_client(ENGINE2_NAME, VOLUME2_NAME)

    offset = 0
    length = 128
    address = grpc_controller.address
    address2 = grpc2_controller.address

    for backup_target in backup_targets:
        dev = get_dev(grpc_replica1, grpc_replica2,
                      grpc_controller)
        dev2 = get_dev(grpc2_replica1, grpc2_replica2,
                       grpc2_controller)

        # create a regular backup
        snap_data = random_string(length)
        verify_data(dev, offset, snap_data)
        snap = cmd.snapshot_create(address)
        backup_info = create_backup(
            address, snap, backup_target, backup_name="backup-z")
        assert backup_info["VolumeName"] == VOLUME_NAME
        assert backup_info["Size"] == BLOCK_SIZE_STR
        assert snap in backup_info["SnapshotName"]

        # create a regular backup on volume 2
        verify_data(dev2, offset, random_string(length))
        snap = cmd.snapshot_create(address2)
        backup_info = create_backup(
            address2, snap, backup_target, backup_name="backup-y")
        assert backup_info["VolumeName"] == VOLUME2_NAME
        assert backup_info["Size"] == BLOCK_SIZE_STR
        assert snap in backup_info["SnapshotName"]

        # request a volume list
        info = cmd.backup_volume_list(address, "", backup_target,
                                      include_backup_details=True)
        assert info[VOLUME_NAME] is not None
        assert len(info[VOLUME_NAME]["Backups"]) == 1
        assert MESSAGE_TYPE_ERROR not in info[VOLUME_NAME]["Messages"]
        assert info[VOLUME2_NAME] is not None
        assert len(info[VOLUME2_NAME]["Backups"]) == 1
        assert MESSAGE_TYPE_ERROR not in info[VOLUME2_NAME]["Messages"]

        # place badly named backup.cfg file
        # we want the list call to return all valid files correctly
        backup_dir = os.path.join(finddir(BACKUP_DIR, VOLUME_NAME), "backups")
        cfg = open(os.path.join(backup_dir, "backup_1234@failure.cfg"), "w")
        cfg.close()
        info = cmd.backup_volume_list(address, "", backup_target,
                                      include_backup_details=True)
        assert info[VOLUME_NAME] is not None
        assert len(info[VOLUME_NAME]["Backups"]) == 1
        assert MESSAGE_TYPE_ERROR not in info[VOLUME_NAME]["Messages"]
        assert info[VOLUME2_NAME] is not None
        assert len(info[VOLUME2_NAME]["Backups"]) == 1
        assert MESSAGE_TYPE_ERROR not in info[VOLUME2_NAME]["Messages"]

        # remove the volume with the badly named backup.cfg
        cmd.backup_volume_rm(address, VOLUME_NAME, backup_target)
        info = cmd.backup_volume_list(address, VOLUME_NAME, backup_target,
                                      include_backup_details=True)
        url = get_backup_volume_url(backup_target, VOLUME_NAME)
        with pytest.raises(subprocess.CalledProcessError):
            cmd.backup_inspect_volume(address, url)

        # remove volume 2 backups
        cmd.backup_volume_rm(address, VOLUME2_NAME, backup_target)
        info = cmd.backup_volume_list(address, VOLUME2_NAME, backup_target,
                                      include_backup_details=True)
        url = get_backup_volume_url(backup_target, VOLUME2_NAME)
        with pytest.raises(subprocess.CalledProcessError):
            cmd.backup_inspect_volume(address, url)

        # clean up volume 1
        cmd.sync_agent_server_reset(address)
        cleanup_controller(grpc_controller)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)

        # clean up volume 2
        cmd.sync_agent_server_reset(address2)
        cleanup_controller(grpc2_controller)
        cleanup_replica(grpc2_replica1)
        cleanup_replica(grpc2_replica2)


def test_backup_lock(grpc_replica1, grpc_replica2,  # NOQA
                            grpc_controller, backup_targets):  # NOQA
    """
    Test backup locks

    Context:

    The idea is to implement a locking mechanism that utilizes the backupstore,
    to prevent the following dangerous cases of concurrent operations.
    - prevent backup deletion during backup restoration
    - prevent backup deletion while a backup is in progress
    - prevent backup creation during backup deletion
    - prevent backup restoration during backup deletion

    Steps:

    1.  Create a volume(1) and attach to the current node
    2.  create a backup(1) of volume(1)
    3.  verify backup(1) creation completed
    4.  write some data to volume(1)
    5.  create an active lock of type Delete
    6.  create a backup(2) of volume(1)
    7.  verify backup(2) creation timed out
    8.  delete active lock of type Delete
    9.  create an active lock of type Delete
    10. restore backup(1)
    11. verify backup(1) restore timed out
    12. delete active lock of type Delete
    13. restore backup(1)
    14. verify backup(1) restore completed
    15. create an active lock of type Restore
    16. delete backup(1)
    17. verify backup(1) deletion timed out
    18. delete active lock of type Restore
    19. delete backup(1)
    20. verify backup(1) deletion completed
    21. cleanup
    """
    for backup_target in backup_targets:
        dev = get_dev(grpc_replica1, grpc_replica2,
                      grpc_controller)

        # create a regular backup
        address = grpc_controller.address
        offset = 0
        length = 128

        snap1_data = random_string(length)
        verify_data(dev, offset, snap1_data)
        snap1_checksum = checksum_dev(dev)
        snap1 = cmd.snapshot_create(address)

        # create a backup to create the volume
        info = create_backup(address, snap1, backup_target)
        assert info["VolumeName"] == VOLUME_NAME
        assert info["Size"] == BLOCK_SIZE_STR
        assert snap1 in info["SnapshotName"]

        # backup should error out with timeout
        # because of delete lock
        create_delete_lock(True)
        with pytest.raises(subprocess.CalledProcessError):
            create_backup(address, snap1, backup_target)
        remove_lock_file(DELETE_LOCK)

        # restore should error out with timeout
        # because of delete lock
        create_delete_lock(True)
        with pytest.raises(subprocess.CalledProcessError):
            restore_with_frontend(address, ENGINE_NAME, info["URL"])
        remove_lock_file(DELETE_LOCK)

        # restore should succeed now, that there is no active delete lock
        restore_with_frontend(address, ENGINE_NAME, info["URL"])
        readed = read_dev(dev, offset, length)
        assert readed == snap1_data
        c = checksum_dev(dev)
        assert c == snap1_checksum

        # delete should error out with timeout
        # because of restore lock
        create_restore_lock(True)
        with pytest.raises(subprocess.CalledProcessError):
            rm_backups(address, ENGINE_NAME, [info["URL"]])
        remove_lock_file(RESTORE_LOCK)

        # delete should succeed now, that there is no active restore lock
        rm_backups(address, ENGINE_NAME, [info["URL"]])

        # clean up volume 1
        cmd.sync_agent_server_reset(address)
        cleanup_controller(grpc_controller)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)


def remove_lock_file(name):
    locks_dir = os.path.join(finddir(BACKUP_DIR, VOLUME_NAME), "locks")
    lock = os.path.join(locks_dir, name + ".lck")
    os.remove(lock)


def create_restore_lock(acquired):
    data = json.dumps({"Name": RESTORE_LOCK, "Type": 1, "Acquired": acquired})
    create_lock_file(RESTORE_LOCK, data)


def create_backup_lock(acquired):
    data = json.dumps({"Name": BACKUP_LOCK, "Type": 1, "Acquired": acquired})
    create_lock_file(BACKUP_LOCK, data)


def create_delete_lock(acquired):
    data = json.dumps({"Name": DELETE_LOCK, "Type": 2, "Acquired": acquired})
    create_lock_file(DELETE_LOCK, data)


def create_lock_file(name, data):
    locks_dir = os.path.join(finddir(BACKUP_DIR, VOLUME_NAME), "locks")
    tmp = os.path.join(locks_dir, name + ".lck" + ".tmp")
    os.makedirs(locks_dir, exist_ok=True)
    cfg = open(tmp, "w")
    cfg.write(data)
    cfg.close()
    os.rename(tmp, os.path.join(locks_dir, name + ".lck"))


def test_backup_type(grpc_replica1, grpc_replica2,      # NOQA
                     grpc_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        address = grpc_controller.address
        block_size = 2 * 1024 * 1024

        dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

        zero_string = b'\x00'.decode('utf-8')

        # backup0: 256 random data in 1st block
        length0 = 256
        snap0_data = random_string(length0)
        verify_data(dev, 0, snap0_data)
        verify_data(dev, block_size, snap0_data)
        snap0 = cmd.snapshot_create(address)
        backup0 = create_backup(address, snap0, backup_target)
        backup0_url = backup0["URL"]
        assert backup0['IsIncremental'] is False

        # backup1: 32 random data + 32 zero data + 192 random data in 1st block
        length1 = 32
        offset1 = 32
        snap1_data = zero_string * length1
        verify_data(dev, offset1, snap1_data)
        snap1 = cmd.snapshot_create(address)
        backup1 = create_backup(address, snap1, backup_target)
        backup1_url = backup1["URL"]
        assert backup1['IsIncremental'] is True

        # backup2: 32 random data + 256 random data in 1st block,
        #          256 random data in 2nd block
        length2 = 256
        offset2 = 32
        snap2_data = random_string(length2)
        verify_data(dev, offset2, snap2_data)
        verify_data(dev, block_size, snap2_data)
        snap2 = cmd.snapshot_create(address)
        backup2 = create_backup(address, snap2, backup_target)
        backup2_url = backup2["URL"]
        assert backup2['IsIncremental'] is True

        rm_backups(address, ENGINE_NAME, [backup2_url])

        # backup3: 64 zero data + 192 random data in 1st block
        length3 = 64
        offset3 = 0
        verify_data(dev, offset3, zero_string * length3)
        verify_data(dev, length2, zero_string * offset2)
        verify_data(dev, block_size, zero_string * length2)
        snap3 = cmd.snapshot_create(address)
        backup3 = create_backup(address, snap3, backup_target)
        backup3_url = backup3["URL"]
        assert backup3['IsIncremental'] is True

        # full backup: backup the same snapshot twice
        backup3 = create_backup(address, snap3, backup_target)
        backup3_url = backup3["URL"]
        assert backup3['IsIncremental'] is False

        # backup4: 256 random data in 1st block
        length4 = 256
        offset4 = 0
        snap4_data = random_string(length4)
        verify_data(dev, offset4, snap4_data)
        snap4 = cmd.snapshot_create(address)
        backup4 = create_backup(address, snap4, backup_target)
        backup4_url = backup4["URL"]
        assert backup4['IsIncremental'] is True

        rm_backups(address, ENGINE_NAME, [backup0_url, backup1_url,
                                          backup3_url, backup4_url])

        cmd.sync_agent_server_reset(address)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)
        cleanup_controller(grpc_controller)
