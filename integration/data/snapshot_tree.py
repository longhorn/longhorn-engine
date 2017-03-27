import cmd
import common
from common import read_dev, VOLUME_HEAD


def snapshot_tree_build(dev, offset, length, strict=True):
    # snap["0a"] -> snap["0b"] -> snap["0c"]
    #                 |-> snap["1a"] -> snap["1b"] -> snap["1c"]
    #                 \-> snap["2a"] -> snap["2b"] -> snap["2c"]
    #                       \-> snap["3a"] -> snap["3b"] -> snap["3c"] -> head

    snap = {}
    snap_data = {}

    snap_data["0a"] = common.random_string(length)
    common.verify_data(dev, offset, snap_data["0a"])
    snap["0a"] = cmd.snapshot_create()

    snap_data["0b"] = common.random_string(length)
    common.verify_data(dev, offset, snap_data["0b"])
    snap["0b"] = cmd.snapshot_create()

    snap_data["0c"] = common.random_string(length)
    common.verify_data(dev, offset, snap_data["0c"])
    snap["0c"] = cmd.snapshot_create()

    cmd.snapshot_revert(snap["0b"])

    snap_data["1a"] = common.random_string(length)
    common.verify_data(dev, offset, snap_data["1a"])
    snap["1a"] = cmd.snapshot_create()

    snap_data["1b"] = common.random_string(length)
    common.verify_data(dev, offset, snap_data["1b"])
    snap["1b"] = cmd.snapshot_create()

    snap_data["1c"] = common.random_string(length)
    common.verify_data(dev, offset, snap_data["1c"])
    snap["1c"] = cmd.snapshot_create()

    cmd.snapshot_revert(snap["0b"])

    snap_data["2a"] = common.random_string(length)
    common.verify_data(dev, offset, snap_data["2a"])
    snap["2a"] = cmd.snapshot_create()

    snap_data["2b"] = common.random_string(length)
    common.verify_data(dev, offset, snap_data["2b"])
    snap["2b"] = cmd.snapshot_create()

    snap_data["2c"] = common.random_string(length)
    common.verify_data(dev, offset, snap_data["2c"])
    snap["2c"] = cmd.snapshot_create()

    cmd.snapshot_revert(snap["2a"])

    snap_data["3a"] = common.random_string(length)
    common.verify_data(dev, offset, snap_data["3a"])
    snap["3a"] = cmd.snapshot_create()

    snap_data["3b"] = common.random_string(length)
    common.verify_data(dev, offset, snap_data["3b"])
    snap["3b"] = cmd.snapshot_create()

    snap_data["3c"] = common.random_string(length)
    common.verify_data(dev, offset, snap_data["3c"])
    snap["3c"] = cmd.snapshot_create()

    snapshot_tree_verify(dev, offset, length, snap, snap_data, strict)
    return snap, snap_data


# snapshot_tree_verify_relationship won't check head or initial snapshot if
# "strict" is False
def snapshot_tree_verify_relationship(snap, strict):
    info = cmd.snapshot_info()

    assert snap["0a"] in info
    assert info[snap["0a"]]["children"] == [snap["0b"]]

    assert snap["0b"] in info
    assert info[snap["0b"]]["parent"] == snap["0a"]
    assert len(info[snap["0b"]]["children"]) == 3
    assert snap["0c"] in info[snap["0b"]]["children"]
    assert snap["1a"] in info[snap["0b"]]["children"]
    assert snap["2a"] in info[snap["0b"]]["children"]

    assert snap["0c"] in info
    assert info[snap["0c"]]["parent"] == snap["0b"]
    assert info[snap["0c"]]["children"] == []

    assert snap["1a"] in info
    assert info[snap["1a"]]["parent"] == snap["0b"]
    assert info[snap["1a"]]["children"] == [snap["1b"]]

    assert snap["1b"] in info
    assert info[snap["1b"]]["parent"] == snap["1a"]
    assert info[snap["1b"]]["children"] == [snap["1c"]]

    assert snap["1c"] in info
    assert info[snap["1c"]]["parent"] == snap["1b"]
    assert info[snap["1c"]]["children"] == []

    assert snap["2a"] in info
    assert info[snap["2a"]]["parent"] == snap["0b"]
    assert len(info[snap["2a"]]["children"]) == 2
    assert snap["2b"] in info[snap["2a"]]["children"]
    assert snap["3a"] in info[snap["2a"]]["children"]

    assert snap["2b"] in info
    assert info[snap["2b"]]["parent"] == snap["2a"]
    assert info[snap["2b"]]["children"] == [snap["2c"]]

    assert snap["2c"] in info
    assert info[snap["2c"]]["parent"] == snap["2b"]
    assert info[snap["2c"]]["children"] == []

    assert snap["3a"] in info
    assert info[snap["3a"]]["parent"] == snap["2a"]
    assert info[snap["3a"]]["children"] == [snap["3b"]]

    assert snap["3b"] in info
    assert info[snap["3b"]]["parent"] == snap["3a"]
    assert info[snap["3b"]]["children"] == [snap["3c"]]

    assert snap["3c"] in info
    assert info[snap["3c"]]["parent"] == snap["3b"]

    if strict:
        assert len(info) == 13
        assert info[snap["0a"]]["parent"] == ""
        assert info[snap["3c"]]["children"] == [VOLUME_HEAD]
        assert VOLUME_HEAD in info
        assert info[VOLUME_HEAD]["parent"] == snap["3c"]
        assert info[VOLUME_HEAD]["children"] == []

        output = cmd.snapshot_ls()
        assert output == '''ID
{}
{}
{}
{}
{}
{}
'''.format(snap["3c"], snap["3b"], snap["3a"],
           snap["2a"], snap["0b"], snap["0a"])


def snapshot_tree_verify_data(dev, offset, length, snap, snap_data):
    cmd.snapshot_revert(snap["0a"])
    readed = read_dev(dev, offset, length)
    assert readed == snap_data["0a"]

    cmd.snapshot_revert(snap["0b"])
    readed = read_dev(dev, offset, length)
    assert readed == snap_data["0b"]

    cmd.snapshot_revert(snap["0c"])
    readed = read_dev(dev, offset, length)
    assert readed == snap_data["0c"]

    cmd.snapshot_revert(snap["1a"])
    readed = read_dev(dev, offset, length)
    assert readed == snap_data["1a"]

    cmd.snapshot_revert(snap["1b"])
    readed = read_dev(dev, offset, length)
    assert readed == snap_data["1b"]

    cmd.snapshot_revert(snap["1c"])
    readed = read_dev(dev, offset, length)
    assert readed == snap_data["1c"]

    cmd.snapshot_revert(snap["2a"])
    readed = read_dev(dev, offset, length)
    assert readed == snap_data["2a"]

    cmd.snapshot_revert(snap["2b"])
    readed = read_dev(dev, offset, length)
    assert readed == snap_data["2b"]

    cmd.snapshot_revert(snap["2c"])
    readed = read_dev(dev, offset, length)
    assert readed == snap_data["2c"]

    cmd.snapshot_revert(snap["3a"])
    readed = read_dev(dev, offset, length)
    assert readed == snap_data["3a"]

    cmd.snapshot_revert(snap["3b"])
    readed = read_dev(dev, offset, length)
    assert readed == snap_data["3b"]

    cmd.snapshot_revert(snap["3c"])
    readed = read_dev(dev, offset, length)
    assert readed == snap_data["3c"]


def snapshot_tree_verify(dev, offset, length, snap, snap_data, strict=False):
    snapshot_tree_verify_relationship(snap, strict)
    snapshot_tree_verify_data(dev, offset, length, snap, snap_data)


def snapshot_tree_verify_backup_node(dev, offset, length, backup, data, name):
    cmd.backup_restore(backup[name])
    readed = read_dev(dev, offset, length)
    assert readed == data[name]
