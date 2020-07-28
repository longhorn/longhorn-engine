import common.cmd as cmd
from common.core import (  # NOQA
    read_dev, random_string, verify_data,
    snapshot_revert_with_frontend,
    restore_with_frontend,
    reset_volume, get_blockdev,
)
from common.constants import VOLUME_HEAD


def snapshot_tree_build(dev, address, engine_name,
                        offset, length, strict=True):
    # snap["0a"] -> snap["0b"] -> snap["0c"]
    #                 |-> snap["1a"] -> snap["1b"] -> snap["1c"]
    #                 \-> snap["2a"] -> snap["2b"] -> snap["2c"]
    #                       \-> snap["3a"] -> snap["3b"] -> snap["3c"] -> head

    snap = {}
    data = {}

    snapshot_tree_create_node(dev, address, offset, length, snap, data, "0a")
    snapshot_tree_create_node(dev, address, offset, length, snap, data, "0b")
    snapshot_tree_create_node(dev, address, offset, length, snap, data, "0c")

    snapshot_revert_with_frontend(address, engine_name, snap["0b"])

    snapshot_tree_create_node(dev, address, offset, length, snap, data, "1a")
    snapshot_tree_create_node(dev, address, offset, length, snap, data, "1b")
    snapshot_tree_create_node(dev, address, offset, length, snap, data, "1c")

    snapshot_revert_with_frontend(address, engine_name, snap["0b"])

    snapshot_tree_create_node(dev, address, offset, length, snap, data, "2a")
    snapshot_tree_create_node(dev, address, offset, length, snap, data, "2b")
    snapshot_tree_create_node(dev, address, offset, length, snap, data, "2c")

    snapshot_revert_with_frontend(address, engine_name, snap["2a"])

    snapshot_tree_create_node(dev, address, offset, length, snap, data, "3a")
    snapshot_tree_create_node(dev, address, offset, length, snap, data, "3b")
    snapshot_tree_create_node(dev, address, offset, length, snap, data, "3c")

    snapshot_tree_verify(dev, address, engine_name,
                         offset, length, snap, data, strict)
    return snap, data


def snapshot_tree_create_node(dev, address, offset, length, snap, data, name):
    data[name] = random_string(length)
    verify_data(dev, offset, data[name])
    snap[name] = cmd.snapshot_create(address)


def snapshot_tree_verify(dev, address, engine_name,
                         offset, length, snap, data, strict=False):
    snapshot_tree_verify_relationship(address, snap, strict)
    snapshot_tree_verify_data(dev, address, engine_name,
                              offset, length, snap, data)


# snapshot_tree_verify_relationship won't check head or initial snapshot if
# "strict" is False
def snapshot_tree_verify_relationship(address, snap, strict):
    info = cmd.snapshot_info(address)

    assert snap["0a"] in info
    assert snap["0b"] in info[snap["0a"]]["children"]

    assert snap["0b"] in info
    assert info[snap["0b"]]["parent"] == snap["0a"]
    assert len(info[snap["0b"]]["children"]) == 3
    assert snap["0c"] in info[snap["0b"]]["children"]
    assert snap["1a"] in info[snap["0b"]]["children"]
    assert snap["2a"] in info[snap["0b"]]["children"]

    assert snap["0c"] in info
    assert info[snap["0c"]]["parent"] == snap["0b"]
    assert not info[snap["0c"]]["children"]

    assert snap["1a"] in info
    assert info[snap["1a"]]["parent"] == snap["0b"]
    assert snap["1b"] in info[snap["1a"]]["children"]

    assert snap["1b"] in info
    assert info[snap["1b"]]["parent"] == snap["1a"]
    assert snap["1c"] in info[snap["1b"]]["children"]

    assert snap["1c"] in info
    assert info[snap["1c"]]["parent"] == snap["1b"]
    assert not info[snap["1c"]]["children"]

    assert snap["2a"] in info
    assert info[snap["2a"]]["parent"] == snap["0b"]
    assert len(info[snap["2a"]]["children"]) == 2
    assert snap["2b"] in info[snap["2a"]]["children"]
    assert snap["3a"] in info[snap["2a"]]["children"]

    assert snap["2b"] in info
    assert info[snap["2b"]]["parent"] == snap["2a"]
    assert snap["2c"] in info[snap["2b"]]["children"]

    assert snap["2c"] in info
    assert info[snap["2c"]]["parent"] == snap["2b"]
    assert not info[snap["2c"]]["children"]

    assert snap["3a"] in info
    assert info[snap["3a"]]["parent"] == snap["2a"]
    assert snap["3b"] in info[snap["3a"]]["children"]

    assert snap["3b"] in info
    assert info[snap["3b"]]["parent"] == snap["3a"]
    assert snap["3c"] in info[snap["3b"]]["children"]

    assert snap["3c"] in info
    assert info[snap["3c"]]["parent"] == snap["3b"]

    if strict:
        assert len(info) == 13
        assert info[snap["0a"]]["parent"] == ""
        assert VOLUME_HEAD in info[snap["3c"]]["children"]
        assert VOLUME_HEAD in info
        assert info[VOLUME_HEAD]["parent"] == snap["3c"]
        assert not info[VOLUME_HEAD]["children"]

        output = cmd.snapshot_ls(address)
        assert output == '''ID
{}
{}
{}
{}
{}
{}
'''.format(snap["3c"], snap["3b"], snap["3a"],
           snap["2a"], snap["0b"], snap["0a"])


def snapshot_tree_verify_data(dev, address, engine_name,
                              offset, length, snap, data):
    snapshot_tree_verify_node(dev, address, engine_name,
                              offset, length, snap, data, "0a")
    snapshot_tree_verify_node(dev, address, engine_name,
                              offset, length, snap, data, "0b")
    snapshot_tree_verify_node(dev, address, engine_name,
                              offset, length, snap, data, "0c")
    snapshot_tree_verify_node(dev, address, engine_name,
                              offset, length, snap, data, "1a")
    snapshot_tree_verify_node(dev, address, engine_name,
                              offset, length, snap, data, "1b")
    snapshot_tree_verify_node(dev, address, engine_name,
                              offset, length, snap, data, "1c")
    snapshot_tree_verify_node(dev, address, engine_name,
                              offset, length, snap, data, "2a")
    snapshot_tree_verify_node(dev, address, engine_name,
                              offset, length, snap, data, "2b")
    snapshot_tree_verify_node(dev, address, engine_name,
                              offset, length, snap, data, "2c")
    snapshot_tree_verify_node(dev, address, engine_name,
                              offset, length, snap, data, "3a")
    snapshot_tree_verify_node(dev, address, engine_name,
                              offset, length, snap, data, "3b")
    snapshot_tree_verify_node(dev, address, engine_name,
                              offset, length, snap, data, "3c")


def snapshot_tree_verify_node(dev, address, engine_name,
                              offset, length, snap, data, name):
    snapshot_revert_with_frontend(address, engine_name, snap[name])
    readed = read_dev(dev, offset, length)
    assert readed == data[name]


def snapshot_tree_verify_backup_node(
        grpc_controller, grpc_replica1, grpc_replica2,
        address, engine_name, offset, length, backup, data, name):  # NOQA
    reset_volume(grpc_controller,
                 grpc_replica1, grpc_replica2)
    dev = get_blockdev(grpc_controller.volume_get().name)
    restore_with_frontend(address, engine_name, backup[name])
    readed = read_dev(dev, offset, length)
    assert readed == data[name]
