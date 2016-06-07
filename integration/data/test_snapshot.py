import pytest

import cmd
import common
from common import dev, SIZE, read_dev, write_dev


def test_snapshot_revert(dev):
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


# TODO BUG: https://github.com/rancher/longhorn/issues/108
@pytest.mark.xfail(strict=True)
def test_snapshot_rm(dev):
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

    cmd.snapshot_rm(snap2)

    readed = read_dev(dev, offset, length)
    assert readed == snap3_data

    cmd.snapshot_revert(snap1)
    readed = read_dev(dev, offset, length)
    assert readed == snap1_data
