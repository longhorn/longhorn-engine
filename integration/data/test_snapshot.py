import cmd
from common import (  # NOQA
    grpc_controller, grpc_replica1, grpc_replica2,  # NOQA
    grpc_backing_controller, grpc_backing_replica1, grpc_backing_replica2,  # NOQA
    get_dev, get_backing_dev, read_dev,
    generate_random_data, read_from_backing_file,
    Snapshot, snapshot_revert_with_frontend,
)
from setting import (
    VOLUME_HEAD, ENGINE_NAME, ENGINE_BACKING_NAME,
)
from snapshot_tree import snapshot_tree_build, snapshot_tree_verify_node


def snapshot_revert_test(dev, address, engine_name):  # NOQA
    existings = {}

    snap1 = Snapshot(dev, generate_random_data(existings),
                     address)
    snap2 = Snapshot(dev, generate_random_data(existings),
                     address)
    snap3 = Snapshot(dev, generate_random_data(existings),
                     address)

    snapList = cmd.snapshot_ls(address)
    assert snap1.name in snapList
    assert snap2.name in snapList
    assert snap3.name in snapList

    snapshot_revert_with_frontend(address, engine_name, snap2.name)
    snap3.refute_data()
    snap2.verify_checksum()
    snap1.verify_data()

    snapshot_revert_with_frontend(address, engine_name, snap1.name)
    snap3.refute_data()
    snap2.refute_data()
    snap1.verify_checksum()


def test_snapshot_revert(grpc_controller,  # NOQA
                         grpc_replica1, grpc_replica2):  # NOQA
    address = grpc_controller.address
    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)
    snapshot_revert_test(dev, address, ENGINE_NAME)


# BUG: https://github.com/rancher/longhorn/issues/108
def test_snapshot_rm_basic(grpc_controller,  # NOQA
                           grpc_replica1, grpc_replica2):  # NOQA
    address = grpc_controller.address

    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    existings = {}

    snap1 = Snapshot(dev, generate_random_data(existings),
                     address)
    snap2 = Snapshot(dev, generate_random_data(existings),
                     address)
    snap3 = Snapshot(dev, generate_random_data(existings),
                     address)

    info = cmd.snapshot_info(address)
    assert len(info) == 4
    assert VOLUME_HEAD in info
    assert snap1.name in info
    assert snap2.name in info
    assert snap3.name in info

    cmd.snapshot_rm(address, snap2.name)
    cmd.snapshot_purge(address)

    info = cmd.snapshot_info(address)
    assert len(info) == 3
    assert snap1.name in info
    assert snap3.name in info

    snap3.verify_checksum()
    snap2.verify_data()
    snap1.verify_data()

    snapshot_revert_with_frontend(address, ENGINE_NAME, snap1.name)
    snap3.refute_data()
    snap2.refute_data()
    snap1.verify_checksum()


def test_snapshot_revert_with_backing_file(grpc_backing_controller,  # NOQA
                                           grpc_backing_replica1,  # NOQA
                                           grpc_backing_replica2):  # NOQA
    address = grpc_backing_controller.address

    dev = get_backing_dev(grpc_backing_replica1, grpc_backing_replica2,
                          grpc_backing_controller)

    offset = 0
    length = 256

    snap0 = cmd.snapshot_create(address)
    before = read_dev(dev, offset, length)
    assert before != ""

    info = cmd.snapshot_info(address)
    assert len(info) == 2
    assert VOLUME_HEAD in info
    assert snap0 in info

    exists = read_from_backing_file(offset, length)
    assert before == exists

    snapshot_revert_test(dev, address, ENGINE_BACKING_NAME)

    snapshot_revert_with_frontend(address, ENGINE_BACKING_NAME, snap0)
    after = read_dev(dev, offset, length)
    assert before == after


def test_snapshot_rm_rolling(grpc_controller,  # NOQA
                             grpc_replica1, grpc_replica2):  # NOQA
    address = grpc_controller.address

    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    existings = {}

    snap1 = Snapshot(dev, generate_random_data(existings),
                     address)

    snapList = cmd.snapshot_ls(address)
    assert snap1.name in snapList

    cmd.snapshot_rm(address, snap1.name)
    # cannot do anything because it's the parent of volume head
    cmd.snapshot_purge(address)

    snap2 = Snapshot(dev, generate_random_data(existings),
                     address)

    info = cmd.snapshot_info(address)
    assert len(info) == 3
    assert snap1.name in info
    assert snap2.name in info
    assert info[snap1.name]["removed"] is True
    assert info[snap2.name]["removed"] is False

    cmd.snapshot_rm(address, snap2.name)
    # this should trigger the deletion of snap1
    cmd.snapshot_purge(address)

    snap2.verify_checksum()
    snap1.verify_data()

    snap3 = Snapshot(dev, generate_random_data(existings),
                     address)
    snap4 = Snapshot(dev, generate_random_data(existings),
                     address)
    snap5 = Snapshot(dev, generate_random_data(existings),
                     address)

    snapList = cmd.snapshot_ls(address)
    assert snap1.name not in snapList
    assert snap2.name not in snapList
    assert snap3.name in snapList
    assert snap4.name in snapList
    assert snap5.name in snapList

    info = cmd.snapshot_info(address)
    assert len(info) == 5
    assert snap1.name not in info
    assert snap2.name in info
    assert snap3.name in info
    assert snap4.name in info
    assert snap5.name in info
    assert info[snap2.name]["removed"] is True

    cmd.snapshot_rm(address, snap3.name)
    cmd.snapshot_rm(address, snap4.name)
    cmd.snapshot_rm(address, snap5.name)
    # this should trigger the deletion of snap2 - snap4
    # and snap5 marked as removed
    cmd.snapshot_purge(address)

    info = cmd.snapshot_info(address)
    assert len(info) == 2
    assert snap1.name not in info
    assert snap2.name not in info
    assert snap3.name not in info
    assert snap4.name not in info
    assert snap5.name in info
    assert info[snap5.name]["removed"] is True

    snap5.verify_checksum()
    snap4.verify_data()
    snap3.verify_data()
    snap2.verify_data()
    snap1.verify_data()


def test_snapshot_tree_basic(grpc_controller,  # NOQA
                             grpc_replica1, grpc_replica2):  # NOQA
    address = grpc_controller.address

    dev = get_dev(grpc_replica1, grpc_replica2, grpc_controller)

    offset = 0
    length = 128

    snap, data = snapshot_tree_build(dev, address, ENGINE_NAME,
                                     offset, length)

    snapshot_revert_with_frontend(address, ENGINE_NAME, snap["1b"])
    cmd.snapshot_rm(address, snap["0a"])
    cmd.snapshot_rm(address, snap["0b"])
    cmd.snapshot_rm(address, snap["1c"])
    cmd.snapshot_rm(address, snap["2a"])
    cmd.snapshot_rm(address, snap["2b"])
    cmd.snapshot_rm(address, snap["2c"])
    cmd.snapshot_rm(address, snap["3a"])
    cmd.snapshot_rm(address, snap["3b"])
    cmd.snapshot_rm(address, snap["3c"])
    cmd.snapshot_purge(address)

    # the result should looks like this
    # snap["0b"](r) -> snap["0c"]
    #   \-> snap["1a"] -> snap["1b"] -> head
    info = cmd.snapshot_info(address)
    assert len(info) == 5

    assert snap["0b"] in info
    assert info[snap["0b"]]["parent"] == ""
    assert len(info[snap["0b"]]["children"]) == 2
    assert snap["0c"] in info[snap["0b"]]["children"]
    assert snap["1a"] in info[snap["0b"]]["children"]
    assert info[snap["0b"]]["removed"] is True

    assert snap["0c"] in info
    assert info[snap["0c"]]["parent"] == snap["0b"]
    assert not info[snap["0c"]]["children"]

    assert snap["1a"] in info
    assert info[snap["1a"]]["parent"] == snap["0b"]
    assert snap["1b"] in info[snap["1a"]]["children"]

    assert snap["1b"] in info
    assert info[snap["1b"]]["parent"] == snap["1a"]
    assert VOLUME_HEAD in info[snap["1b"]]["children"]

    assert VOLUME_HEAD in info
    assert info[VOLUME_HEAD]["parent"] == snap["1b"]

    snapshot_tree_verify_node(dev, address, ENGINE_NAME,
                              offset, length, snap, data, "0b")
    snapshot_tree_verify_node(dev, address, ENGINE_NAME,
                              offset, length, snap, data, "0c")
    snapshot_tree_verify_node(dev, address, ENGINE_NAME,
                              offset, length, snap, data, "1a")
    snapshot_tree_verify_node(dev, address, ENGINE_NAME,
                              offset, length, snap, data, "1b")
