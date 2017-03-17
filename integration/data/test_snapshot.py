import cmd
import common
from common import dev, backing_dev  # NOQA
from common import read_dev, read_from_backing_file


def test_snapshot_revert(dev):  # NOQA
    offset = 0
    length = 128

    snap1_data = common.random_string(length)
    common.verify_data(dev, offset, snap1_data)
    snap1 = cmd.snapshot_create()

    snap2_data = common.random_string(length)
    common.verify_data(dev, offset, snap2_data)
    snap2 = cmd.snapshot_create()

    snap3_data = common.random_string(length)
    common.verify_data(dev, offset, snap3_data)
    snap3 = cmd.snapshot_create()

    snapList = cmd.snapshot_ls()
    assert snap1 in snapList
    assert snap2 in snapList
    assert snap3 in snapList

    cmd.snapshot_revert(snap2)
    readed = read_dev(dev, offset, length)
    assert readed == snap2_data

    cmd.snapshot_revert(snap1)
    readed = read_dev(dev, offset, length)
    assert readed == snap1_data


# BUG: https://github.com/rancher/longhorn/issues/108
def test_snapshot_rm_basic(dev):  # NOQA
    offset = 0
    length = 128

    snap1_data = common.random_string(length)
    common.verify_data(dev, offset, snap1_data)
    snap1 = cmd.snapshot_create()

    snap2_data = common.random_string(length)
    common.verify_data(dev, offset, snap2_data)
    snap2 = cmd.snapshot_create()

    snap3_data = common.random_string(length)
    common.verify_data(dev, offset, snap3_data)
    snap3 = cmd.snapshot_create()

    info = cmd.snapshot_info()
    assert len(info) == 4
    assert snap1 in info
    assert snap2 in info
    assert snap3 in info

    cmd.snapshot_rm(snap2)
    cmd.snapshot_purge()

    info = cmd.snapshot_info()
    assert len(info) == 3
    assert snap1 in info
    assert snap3 in info

    readed = read_dev(dev, offset, length)
    assert readed == snap3_data

    cmd.snapshot_revert(snap1)
    readed = read_dev(dev, offset, length)
    assert readed == snap1_data


def test_snapshot_revert_with_backing_file(backing_dev):  # NOQA
    dev = backing_dev  # NOQA

    offset = 0
    length = 256

    snap0 = cmd.snapshot_create()
    before = read_dev(dev, offset, length)
    assert before != ""

    exists = read_from_backing_file(offset, length)
    assert before == exists

    test_snapshot_revert(dev)

    cmd.snapshot_revert(snap0)
    after = read_dev(dev, offset, length)
    assert before == after


def test_snapshot_rm_rolling(dev):  # NOQA
    offset = 0
    length = 128

    snap1_data = common.random_string(length)
    common.verify_data(dev, offset, snap1_data)
    snap1 = cmd.snapshot_create()

    snapList = cmd.snapshot_ls()
    assert snap1 in snapList

    cmd.snapshot_rm(snap1)
    # cannot do anything because it's the parent of volume head
    cmd.snapshot_purge()

    snap2_data = common.random_string(length)
    common.verify_data(dev, offset, snap2_data)
    snap2 = cmd.snapshot_create()

    info = cmd.snapshot_info()
    assert len(info) == 3
    assert snap1 in info
    assert info[snap1]["removed"] is True
    assert snap2 in info
    assert info[snap2]["removed"] is False

    cmd.snapshot_rm(snap2)
    # this should trigger the deletion of snap1
    cmd.snapshot_purge()

    snap3_data = common.random_string(length)
    common.verify_data(dev, offset, snap3_data)
    snap3 = cmd.snapshot_create()

    snap4_data = common.random_string(length)
    common.verify_data(dev, offset, snap4_data)
    snap4 = cmd.snapshot_create()

    snap5_data = common.random_string(length)
    common.verify_data(dev, offset, snap5_data)
    snap5 = cmd.snapshot_create()

    snapList = cmd.snapshot_ls()
    assert snap1 not in snapList
    assert snap2 not in snapList
    assert snap3 in snapList
    assert snap4 in snapList
    assert snap5 in snapList

    info = cmd.snapshot_info()
    assert len(info) == 5
    assert snap1 not in info
    assert snap2 in info
    assert info[snap2]["removed"] is True
    assert snap3 in info
    assert info[snap3]["size"] == "4096"
    assert snap4 in info
    assert info[snap4]["size"] == "4096"
    assert snap5 in info
    assert info[snap5]["size"] == "4096"

    cmd.snapshot_rm(snap3)
    cmd.snapshot_rm(snap4)
    cmd.snapshot_rm(snap5)
    # this should trigger the deletion of snap2 - snap4
    # and snap5 marked as removed
    cmd.snapshot_purge()

    info = cmd.snapshot_info()
    assert len(info) == 2
    assert snap1 not in info
    assert snap2 not in info
    assert snap3 not in info
    assert snap4 not in info
    assert snap5 in info
    assert info[snap5]["removed"] is True
    assert info[snap5]["size"] == "4096"

    readed = read_dev(dev, offset, length)
    assert readed == snap5_data
