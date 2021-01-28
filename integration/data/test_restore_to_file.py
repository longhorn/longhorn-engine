import os
import pyqcow

import common.cmd as cmd
from common.core import (  # NOQA
    cleanup_controller, cleanup_replica,
    get_dev, read_dev, checksum_dev,
    random_string, verify_data,
    read_file, read_from_backing_file,
    create_backup, rm_backups, rm_snaps,
    snapshot_revert_with_frontend,
)
from common.util import (
    file, checksum_data,
)
from common.constants import (
    SIZE, BACKING_FILE_QCOW2,
    ENGINE_NAME, ENGINE_BACKING_NAME,
)


OUTPUT_FILE_RAW = 'test-restore_to_file-output.raw'
OUTPUT_FILE_QCOW2 = 'test-restore_to_file-output.qcow2'

IMAGE_FORMAT_RAW = 'raw'
IMAGE_FORMAT_QCOW2 = 'qcow2'


# lib pyqcow doesn't support for reading a qcow2 file
# with backing file set
def read_qcow2_file_without_backing_file(file, offset, length):
    assert os.path.exists(file)
    qcow_file = pyqcow.file()
    qcow_file.open(file)
    qcow_file.seek(offset)
    data = qcow_file.read(length)
    qcow_file.close()
    return data


# guarantee qcow2 backing file unchanged
def check_backing(offset=0, length=4*1024):
    backing_data_raw = read_from_backing_file(offset, length)
    backing_data_qcow2 = read_qcow2_file_without_backing_file(
        file(BACKING_FILE_QCOW2), offset, length)
    assert backing_data_raw == backing_data_qcow2.decode('utf-8')


# check whether volume (with base image) is empty
def check_empty_volume(dev, offset=0, length=4*1024):
    backing_data = read_from_backing_file(offset, length)
    empty_volume_data = read_dev(dev, offset, length)
    assert backing_data == empty_volume_data


def restore_to_file_with_backing_file_test(backup_target,  # NOQA
                                           grpc_backing_controller,  # NOQA
                                           grpc_backing_replica1,  # NOQA
                                           grpc_backing_replica2):  # NOQA
    address = grpc_backing_controller.address

    backing_dev = get_dev(grpc_backing_replica1, grpc_backing_replica2,
                          grpc_backing_controller)

    length0 = 4 * 1024
    length1 = 256
    length2 = 128
    offset0 = 0
    offset1 = length1 + offset0
    offset2 = length2 + offset0

    output_raw_path = file(OUTPUT_FILE_RAW)
    output_qcow2_path = file(OUTPUT_FILE_QCOW2)

    # create 1 empty snapshot.
    # data in output image == data in backing
    check_backing()
    check_empty_volume(backing_dev)
    snap0 = cmd.snapshot_create(address)
    backup = create_backup(address, snap0, backup_target)["URL"]

    volume_data = read_dev(backing_dev, offset0, length0)
    backing_data = read_from_backing_file(offset0, length0)
    dev_checksum = checksum_dev(backing_dev)
    assert volume_data != ""
    assert volume_data == backing_data

    cmd.restore_to_file(address, backup, file(BACKING_FILE_QCOW2),
                        output_raw_path, IMAGE_FORMAT_RAW)
    output0_raw = read_file(output_raw_path, offset0, length0)
    output0_checksum = checksum_data(
        read_file(output_raw_path, 0, SIZE).encode('utf-8'))
    assert output0_raw == backing_data
    assert output0_checksum == dev_checksum
    os.remove(output_raw_path)
    assert not os.path.exists(output_raw_path)

    cmd.restore_to_file(address, backup, file(BACKING_FILE_QCOW2),
                        output_qcow2_path, IMAGE_FORMAT_QCOW2)
    output0_qcow2 = read_qcow2_file_without_backing_file(
        output_qcow2_path, offset0, length0)
    output0_checksum = checksum_data(
        read_qcow2_file_without_backing_file(output_qcow2_path, 0, SIZE))
    assert output0_qcow2.decode('utf-8') == backing_data
    assert output0_qcow2.decode('utf-8') == volume_data
    assert output0_checksum == dev_checksum
    os.remove(output_qcow2_path)
    assert not os.path.exists(output_qcow2_path)

    rm_backups(address, ENGINE_BACKING_NAME, [backup])

    # create 1 snapshot with 256B data.
    # output = snap1(offset0, length1) + backing(offset1, ...)
    snap1_data = random_string(length1)
    verify_data(backing_dev, offset0, snap1_data)
    snap1 = cmd.snapshot_create(address)
    backup = create_backup(address, snap1, backup_target)["URL"]

    volume_data = read_dev(backing_dev, offset0, length0)
    backing_data = read_from_backing_file(
        offset1, length0 - offset1)
    dev_checksum = checksum_dev(backing_dev)

    cmd.restore_to_file(address, backup, file(BACKING_FILE_QCOW2),
                        output_raw_path, IMAGE_FORMAT_RAW)
    output1_raw_snap1 = read_file(
        output_raw_path, offset0, length1)
    output1_raw_backing = read_file(
        output_raw_path, offset1, length0 - offset1)
    output1_checksum = checksum_data(
        read_file(output_raw_path, 0, SIZE).encode('utf-8'))
    assert output1_raw_snap1 == snap1_data
    assert output1_raw_backing == backing_data
    assert output1_raw_snap1 + output1_raw_backing == volume_data
    assert output1_checksum == dev_checksum
    os.remove(output_raw_path)
    assert not os.path.exists(output_raw_path)

    cmd.restore_to_file(address, backup, file(BACKING_FILE_QCOW2),
                        output_qcow2_path, IMAGE_FORMAT_QCOW2)
    output1_qcow2_snap1 = read_qcow2_file_without_backing_file(
        output_qcow2_path, offset0, length1)
    output1_qcow2_backing = read_qcow2_file_without_backing_file(
        output_qcow2_path, offset1, length0 - offset1)
    output1_checksum = checksum_data(
        read_qcow2_file_without_backing_file(output_qcow2_path, 0, SIZE))
    assert output1_qcow2_snap1.decode('utf-8') == snap1_data
    assert output1_qcow2_backing.decode('utf-8') == backing_data
    assert output1_qcow2_snap1.decode('utf-8') + \
        output1_qcow2_backing.decode('utf-8') == volume_data
    assert output1_checksum == dev_checksum
    os.remove(output_qcow2_path)
    assert not os.path.exists(output_qcow2_path)

    snapshot_revert_with_frontend(address, ENGINE_BACKING_NAME, snap0)
    rm_snaps(address, [snap1])
    rm_backups(address, ENGINE_BACKING_NAME, [backup])
    check_backing()
    check_empty_volume(backing_dev)

    # create 2 snapshots with 256B data and 128B data
    # output = snap2(offset0, length1 - length2) +
    #          snap1(offset2, length2) + backing(offset2, ...)
    snap1_data = random_string(length1)
    verify_data(backing_dev, offset0, snap1_data)
    snap1 = cmd.snapshot_create(address)
    snap2_data = random_string(length2)
    verify_data(backing_dev, offset0, snap2_data)
    snap2 = cmd.snapshot_create(address)
    backup = create_backup(address, snap2, backup_target)["URL"]

    volume_data = read_dev(backing_dev, offset0, length0)
    backing_data = read_from_backing_file(
        offset1, length0 - offset1)
    dev_checksum = checksum_dev(backing_dev)

    cmd.restore_to_file(address, backup, file(BACKING_FILE_QCOW2),
                        output_raw_path, IMAGE_FORMAT_RAW)
    output2_raw_snap2 = read_file(
        output_raw_path, offset0, length2)
    output2_raw_snap1 = read_file(
        output_raw_path, offset2, length1 - length2)
    output2_raw_backing = read_file(
        output_raw_path, offset1, length0 - offset1)
    output2_checksum = checksum_data(
        read_file(output_raw_path, 0, SIZE).encode('utf-8'))
    assert output2_raw_snap2 == snap2_data
    assert output2_raw_snap1 == snap1_data[offset2: length1]
    assert output2_raw_backing == backing_data
    assert \
        volume_data == \
        output2_raw_snap2 + output2_raw_snap1 + output2_raw_backing
    assert output2_checksum == dev_checksum
    os.remove(output_raw_path)
    assert not os.path.exists(output_raw_path)

    cmd.restore_to_file(address, backup, file(BACKING_FILE_QCOW2),
                        output_qcow2_path, IMAGE_FORMAT_QCOW2)
    output2_qcow2_snap2 = read_qcow2_file_without_backing_file(
        output_qcow2_path, offset0, length2)
    output2_qcow2_snap1 = read_qcow2_file_without_backing_file(
        output_qcow2_path, offset2, length1 - length2)
    output2_qcow2_backing = read_qcow2_file_without_backing_file(
        output_qcow2_path, offset1, length0 - offset1)
    output2_checksum = checksum_data(
        read_qcow2_file_without_backing_file(output_qcow2_path, 0, SIZE))
    assert output2_qcow2_snap2.decode('utf-8') == snap2_data
    assert output2_qcow2_snap1.decode('utf-8') == snap1_data[offset2: length1]
    assert output2_qcow2_backing.decode('utf-8') == backing_data
    assert \
        volume_data == \
        output2_qcow2_snap2.decode('utf-8') + \
        output2_qcow2_snap1.decode('utf-8') + \
        output1_qcow2_backing.decode('utf-8')
    assert output2_checksum == dev_checksum
    os.remove(output_qcow2_path)
    assert not os.path.exists(output_qcow2_path)

    snapshot_revert_with_frontend(address, ENGINE_BACKING_NAME, snap0)
    rm_snaps(address, [snap1, snap2])
    rm_backups(address, ENGINE_BACKING_NAME, [backup])
    check_backing()
    check_empty_volume(backing_dev)


def restore_to_file_without_backing_file_test(backup_target,  # NOQA
                                              grpc_controller,  # NOQA
                                              grpc_replica1,  # NOQA
                                              grpc_replica2):  # NOQA
    address = grpc_controller.address

    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    length0 = 256
    length1 = 128
    offset0 = 0
    offset1 = length1 + offset0

    output_raw_path = file(OUTPUT_FILE_RAW)
    output_qcow2_path = file(OUTPUT_FILE_QCOW2)

    # create 1 empty snapshot for converting to init state.
    snap0 = cmd.snapshot_create(address)

    # create 1 snapshot with 256B data.
    # output = snap2(offset0, length1)
    snap1_data = random_string(length0)
    verify_data(dev, offset0, snap1_data)
    snap1 = cmd.snapshot_create(address)
    backup = create_backup(address, snap1, backup_target)["URL"]

    cmd.restore_to_file(address, backup, "",
                        output_raw_path, IMAGE_FORMAT_RAW)
    output1_raw = read_file(output_raw_path, offset0, length0)
    assert output1_raw == snap1_data
    os.remove(output_raw_path)
    assert not os.path.exists(output_raw_path)

    cmd.restore_to_file(address, backup, "",
                        output_qcow2_path, IMAGE_FORMAT_QCOW2)
    output1_qcow2 = read_qcow2_file_without_backing_file(
        output_qcow2_path, offset0, length0)
    assert output1_qcow2.decode('utf-8') == snap1_data
    os.remove(output_qcow2_path)
    assert not os.path.exists(output_qcow2_path)

    snapshot_revert_with_frontend(address, ENGINE_NAME, snap0)
    rm_snaps(address, [snap1])
    rm_backups(address, ENGINE_NAME, [backup])

    # create 2 snapshots with 256B data and 128B data
    # output = snap2(offset0, length1 - length2) +
    #          snap1(offset2, length2)
    snap1_data = random_string(length0)
    verify_data(dev, offset0, snap1_data)
    snap1 = cmd.snapshot_create(address)
    snap2_data = random_string(length1)
    verify_data(dev, offset0, snap2_data)
    snap2 = cmd.snapshot_create(address)
    backup = create_backup(address, snap2, backup_target)["URL"]

    cmd.restore_to_file(address, backup, "",
                        output_raw_path, IMAGE_FORMAT_RAW)
    output2_raw_snap2 = read_file(
        output_raw_path, offset0, length1)
    output2_raw_snap1 = read_file(
        output_raw_path, offset1, length0 - length1)
    assert output2_raw_snap2 == snap2_data
    assert output2_raw_snap1 == snap1_data[offset1: length0]

    cmd.restore_to_file(address, backup, "",
                        output_qcow2_path, IMAGE_FORMAT_QCOW2)
    output2_qcow2_snap2 = read_qcow2_file_without_backing_file(
        output_qcow2_path, offset0, length1)
    output2_qcow2_snap1 = read_qcow2_file_without_backing_file(
        output_qcow2_path, offset1, length0 - length1)
    assert output2_qcow2_snap2.decode('utf-8') == snap2_data
    assert output2_qcow2_snap1.decode('utf-8') == snap1_data[offset1: length0]
    os.remove(output_qcow2_path)
    assert not os.path.exists(output_qcow2_path)

    snapshot_revert_with_frontend(address, ENGINE_NAME, snap0)
    rm_snaps(address, [snap1, snap2])
    rm_backups(address, ENGINE_NAME, [backup])


def test_restore_to_file_with_backing_file(backup_targets,  # NOQA
                                           grpc_backing_controller,  # NOQA
                                           grpc_backing_qcow2_replica1,  # NOQA
                                           grpc_backing_qcow2_replica2):  # NOQA
    for backup_target in backup_targets:
        restore_to_file_with_backing_file_test(backup_target,
                                               grpc_backing_controller,
                                               grpc_backing_qcow2_replica1,
                                               grpc_backing_qcow2_replica2,)
        cmd.sync_agent_server_reset(grpc_backing_controller.address)
        cleanup_controller(grpc_backing_controller)
        cleanup_replica(grpc_backing_qcow2_replica1)
        cleanup_replica(grpc_backing_qcow2_replica2)


def test_restore_to_file_without_backing_file(backup_targets,  # NOQA
                                              grpc_controller,  # NOQA
                                              grpc_replica1,  # NOQA
                                              grpc_replica2):  # NOQA
    for backup_target in backup_targets:
        restore_to_file_without_backing_file_test(backup_target,
                                                  grpc_controller,
                                                  grpc_replica1,
                                                  grpc_replica2)
        cmd.sync_agent_server_reset(grpc_controller.address)
        cleanup_controller(grpc_controller)
        cleanup_replica(grpc_replica1)
        cleanup_replica(grpc_replica2)
