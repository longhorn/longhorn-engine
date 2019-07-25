import cmd
from common import (  # NOQA
    backup_targets,  # NOQA
    grpc_controller, grpc_backing_controller,  # NOQA
    grpc_replica1, grpc_replica2,  # NOQA
    grpc_backing_replica1, grpc_backing_replica2,  # NOQA
    cleanup_controller, cleanup_replica,
    get_dev, get_backing_dev, read_dev,
    read_from_backing_file,
    random_string, verify_data, checksum_dev,
    create_backup, rm_backups,
    restore_with_no_frontend,
)
from snapshot_tree import (
    snapshot_tree_build, snapshot_tree_verify_backup_node
)
from setting import (
    VOLUME_NAME, VOLUME_BACKING_NAME, BLOCK_SIZE_STR,
    ENGINE_NAME, ENGINE_BACKING_NAME,
)


def backup_test(dev, address,  # NOQA
                volume_name, engine_name, backup_target):
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

    restore_with_no_frontend(address, engine_name,
                             backup3_info["URL"])

    readed = read_dev(dev, offset, length)
    assert readed == snap3_data
    c = checksum_dev(dev)
    assert c == snap3_checksum

    rm_backups(address, engine_name, [backup3_info["URL"]])

    restore_with_no_frontend(address, engine_name,
                             backup1_info["URL"])
    readed = read_dev(dev, offset, length)
    assert readed == snap1_data
    c = checksum_dev(dev)
    assert c == snap1_checksum

    rm_backups(address, engine_name, [backup1_info["URL"]])

    restore_with_no_frontend(address, engine_name,
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

    dev = get_backing_dev(grpc_backing_replica1,
                          grpc_backing_replica2,
                          grpc_backing_controller)

    offset = 0
    length = 256

    snap0 = cmd.snapshot_create(address)
    before = read_dev(dev, offset, length)
    assert before != ""
    snap0_checksum = checksum_dev(dev)

    exists = read_from_backing_file(offset, length)
    assert before == exists

    backup0_info = create_backup(address, snap0, backup_target)
    assert backup0_info["VolumeName"] == VOLUME_BACKING_NAME

    backup_test(dev, address, VOLUME_BACKING_NAME,
                ENGINE_BACKING_NAME, backup_target)

    restore_with_no_frontend(address, ENGINE_BACKING_NAME,
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

    dev = get_backing_dev(grpc_backing_replica1,
                          grpc_backing_replica2,
                          grpc_backing_controller)

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
    backup1_info = create_backup(address, snap1, backup_target)

    snap2_data = random_string(length2)
    verify_data(dev, offset2, snap2_data)
    snap2_checksum = checksum_dev(dev)
    snap2 = cmd.snapshot_create(address)

    boundary_data_backup2 = read_dev(dev, boundary_offset, boundary_length)
    hole_data_backup2 = read_dev(dev, hole_offset, hole_length)
    backup2_info = create_backup(address, snap2, backup_target)

    restore_with_no_frontend(address, ENGINE_BACKING_NAME,
                             backup1_info["URL"])
    readed = read_dev(dev, boundary_offset, boundary_length)
    assert readed == boundary_data_backup1
    readed = read_dev(dev, hole_offset, hole_length)
    assert readed == hole_data_backup1
    c = checksum_dev(dev)
    assert c == snap1_checksum

    restore_with_no_frontend(address, ENGINE_BACKING_NAME,
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

    snapshot_tree_verify_backup_node(dev, address, engine_name,
                                     offset, length, backup, data, "0b")
    snapshot_tree_verify_backup_node(dev, address, engine_name,
                                     offset, length, backup, data, "0c")
    snapshot_tree_verify_backup_node(dev, address, engine_name,
                                     offset, length, backup, data, "1c")
    snapshot_tree_verify_backup_node(dev, address, engine_name,
                                     offset, length, backup, data, "2b")
    snapshot_tree_verify_backup_node(dev, address, engine_name,
                                     offset, length, backup, data, "2c")
    snapshot_tree_verify_backup_node(dev, address, engine_name,
                                     offset, length, backup, data, "3c")


def test_backup(grpc_replica1, grpc_replica2,  # NOQA
                grpc_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        dev = get_dev(grpc_replica1, grpc_replica2,
                      grpc_controller)
        backup_test(dev, grpc_controller.address,
                    VOLUME_NAME, ENGINE_NAME, backup_target)
        cmd.sync_agent_server_reset(grpc_controller.address)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)
        cleanup_controller(grpc_controller)


def test_snapshot_tree_backup(grpc_replica1, grpc_replica2,  # NOQA
                              grpc_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        snapshot_tree_backup_test(backup_target, ENGINE_NAME,
                                  grpc_controller,
                                  grpc_replica1, grpc_replica2)
        cmd.sync_agent_server_reset(grpc_controller.address)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)
        cleanup_controller(grpc_controller)


def test_backup_with_backing_file(grpc_backing_replica1, grpc_backing_replica2,  # NOQA
                                  grpc_backing_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        backup_with_backing_file_test(backup_target,
                                      grpc_backing_controller,
                                      grpc_backing_replica1,
                                      grpc_backing_replica2)
        cmd.sync_agent_server_reset(grpc_backing_controller.address)
        cleanup_replica(grpc_backing_replica1)
        cleanup_replica(grpc_backing_replica2)
        cleanup_controller(grpc_backing_controller)


def test_backup_hole_with_backing_file(grpc_backing_replica1, grpc_backing_replica2,  # NOQA
                                       grpc_backing_controller, backup_targets):  # NOQA
    for backup_target in backup_targets:
        backup_hole_with_backing_file_test(backup_target,
                                           grpc_backing_controller,
                                           grpc_backing_replica1,
                                           grpc_backing_replica2)
        cmd.sync_agent_server_reset(grpc_backing_controller.address)
        cleanup_replica(grpc_backing_replica1)
        cleanup_replica(grpc_backing_replica2)
        cleanup_controller(grpc_backing_controller)


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
        info = cmd.backup_volume_list(address, VOLUME_NAME, backup_target)
        assert "cannot find" in info[VOLUME_NAME]["Messages"]["error"]

        cmd.sync_agent_server_reset(address)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)
        cleanup_controller(grpc_controller)
