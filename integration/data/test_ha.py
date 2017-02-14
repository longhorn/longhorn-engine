import threading

import cmd
import common
from common import controller, replica1, replica2 # NOQA
from common import open_replica, get_blockdev, cleanup_replica
from common import verify_read, verify_data, data_verifier

def test_ha_single_replica_failure(controller, replica1, replica2):  # NOQA
    open_replica(replica1)
    open_replica(replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = controller.list_volume()[0]
    v = v.start(replicas=[
        common.REPLICA1,
        common.REPLICA2
    ])
    assert v.replicaCount == 2

    replicas = controller.list_replica()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev()

    data = common.random_string(128)
    data_offset = 0
    verify_data(dev, data_offset, data)

    cleanup_replica(replica2)

    thread = threading.Thread(target=data_verifier, args=(dev, 10, 1024, 128))
    thread.start()

    common.verify_replica_state(controller, 1, "ERR")

    thread.join()

    verify_read(dev, data_offset, data)

def test_ha_single_replica_rebuild(controller, replica1, replica2):  # NOQA
    open_replica(replica1)
    open_replica(replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = controller.list_volume()[0]
    v = v.start(replicas=[
        common.REPLICA1,
        common.REPLICA2
    ])
    assert v.replicaCount == 2

    replicas = controller.list_replica()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev()

    data = common.random_string(128)
    data_offset = 0
    verify_data(dev, data_offset, data)

    # Cleanup replica2
    cleanup_replica(replica2)

    thread = threading.Thread(target=data_verifier, args=(dev, 10, 1024, 128))
    thread.start()

    common.verify_replica_state(controller, 1, "ERR")

    thread.join()

    verify_read(dev, data_offset, data)

    controller.delete(replicas[1])

    # Rebuild replica2
    common.open_replica(replica2)
    cmd.add_replica(common.REPLICA2)

    thread = threading.Thread(target=data_verifier, args=(dev, 10, 1024, 128))
    thread.start()

    common.verify_replica_state(controller, 1, "RW")

    thread.join()

    verify_read(dev, data_offset, data)

def test_ha_double_replica_rebuild(controller, replica1, replica2):  # NOQA
    open_replica(replica1)
    open_replica(replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = controller.list_volume()[0]
    v = v.start(replicas=[
        common.REPLICA1,
        common.REPLICA2
    ])
    assert v.replicaCount == 2

    replicas = controller.list_replica()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev()

    data1 = common.random_string(128)
    data1_offset = 0
    verify_data(dev, data1_offset, data1)

    # Close replica2
    r2 = replica2.list_replica()[0]
    assert r2.revisioncounter == 1
    r2.close()

    thread = threading.Thread(target=data_verifier, args=(dev, 10, 1024, 128))
    thread.start()

    common.verify_replica_state(controller, 1, "ERR")

    verify_read(dev, data1_offset, data1)

    thread.join()

    data2 = common.random_string(128)
    data2_offset = 512
    verify_data(dev, data2_offset, data2)

    # Close replica1
    r1 = replica1.list_replica()[0]
    assert r1.revisioncounter == 12  # 1 + 10 + 1
    r1.close()

    # Restart volume
    common.cleanup_controller(controller)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = controller.list_volume()[0]
    # NOTE the order is reversed here
    v = v.start(replicas=[
        common.REPLICA2,
        common.REPLICA1
    ])
    assert v.replicaCount == 2

    # replica2 is out because of lower revision counter
    replicas = controller.list_replica()
    assert len(replicas) == 2
    assert replicas[0].mode == "ERR"
    assert replicas[1].mode == "RW"

    verify_read(dev, data1_offset, data1)
    verify_read(dev, data2_offset, data2)

    # Rebuild replica2
    r2 = replica2.list_replica()[0]
    assert r2.revisioncounter == 1
    r2.close()

    controller.delete(replicas[0])

    cmd.add_replica(common.REPLICA2)

    thread = threading.Thread(target=data_verifier, args=(dev, 10, 1024, 128))
    thread.start()

    common.verify_replica_state(controller, 1, "RW")

    thread.join()

    verify_read(dev, data1_offset, data1)
    verify_read(dev, data2_offset, data2)

    r1 = replica1.list_replica()[0]
    r2 = replica2.list_replica()[0]
    assert r1.revisioncounter == 22  # 1 + 10 + 1 + 10
    assert r2.revisioncounter == 22  # must be in sync with r1


def test_ha_revision_counter_consistency(controller, replica1, replica2):  # NOQA
    open_replica(replica1)
    open_replica(replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = controller.list_volume()[0]
    v = v.start(replicas=[
        common.REPLICA1,
        common.REPLICA2
    ])
    assert v.replicaCount == 2

    replicas = controller.list_replica()
    assert len(replicas) == 2
    assert replicas[0].mode == "RW"
    assert replicas[1].mode == "RW"

    dev = get_blockdev()

    common.verify_async(dev, 10, 128, 100)

    r1 = replica1.list_replica()[0]
    r2 = replica2.list_replica()[0]
    # kernel can merge requests so backend may not receive 1000 writes
    assert r1.revisioncounter > 0
    assert r1.revisioncounter == r2.revisioncounter
