import time
import sys
import os
import grpc
import pytest

from common import (  # NOQA
    grpc_controller_client as grpc_client,  # NOQA
    create_backend_file, cleanup_backend_file,
)

# include directory intergration/rpc for module import
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.split(__file__)[0], "../rpc")
    )
)
from controller.controller_client import ControllerClient  # NOQA


def test_replica_list(grpc_client):  # NOQA
    replicas = grpc_client.replica_list()
    assert len(replicas) == 0


def test_replica_create(grpc_client):  # NOQA
    f1 = create_backend_file()
    r1 = 'file://' + f1
    replica = grpc_client.replica_create(address=r1)
    assert replica.address == r1

    grpc_client.replica_create(address=r1)
    grpc_client.replica_create(address=r1)

    rs = grpc_client.replica_list()
    assert len(rs) == 1
    assert rs[0].address == r1
    assert rs[0].mode == 'WO'

    f2 = create_backend_file()
    r2 = 'file://' + f2
    with pytest.raises(grpc.RpcError) as e:
        grpc_client.replica_create(address=r2)
    assert 'Can only have one WO replica at a time' in str(e.value)

    r = grpc_client.replica_update(rs[0].address, mode='RW')
    assert r.mode == 'RW'

    replica2 = grpc_client.replica_create(address=r2)
    assert replica2.address == r2

    rs = grpc_client.replica_list()
    assert len(rs) == 2

    cleanup_backend_file([f1, f2])


def test_replica_delete(grpc_client):  # NOQA
    f1 = create_backend_file()
    f2 = create_backend_file()
    f3 = create_backend_file()
    r1 = grpc_client.replica_create(address='file://' + f1)
    grpc_client.replica_update(r1.address, mode='RW')
    r2 = grpc_client.replica_create(address='file://' + f2)
    grpc_client.replica_update(r2.address, mode='RW')
    r3 = grpc_client.replica_create(address='file://' + f3)
    grpc_client.replica_update(r3.address, mode='RW')

    rs = grpc_client.replica_list()
    assert len(rs) == 3

    grpc_client.replica_delete(r1.address)
    rs = grpc_client.replica_list()
    assert len(rs) == 2

    grpc_client.replica_delete(r1.address)
    rs = grpc_client.replica_list()
    assert len(rs) == 2

    grpc_client.replica_delete(r2.address)
    rs = grpc_client.replica_list()
    assert len(rs) == 1

    grpc_client.replica_delete(r3.address)
    rs = grpc_client.replica_list()
    assert len(rs) == 0

    cleanup_backend_file([f1, f2, f3])


def test_replica_change(grpc_client):  # NOQA
    f = create_backend_file()
    r1 = grpc_client.replica_create(address='file://' + f)
    assert r1.mode == 'WO'

    r1 = grpc_client.replica_update(r1.address, mode='RW')
    assert r1.mode == 'RW'

    r1 = grpc_client.replica_get(r1.address)
    assert r1.mode == 'RW'

    cleanup_backend_file([f])


def test_start(grpc_client):  # NOQA
    v = grpc_client.volume_get()
    assert v.replicaCount == 0

    f1 = create_backend_file()
    f2 = create_backend_file()
    addresses = ['file://' + f1, 'file://' + f2]
    v = grpc_client.volume_start(replicas=addresses)

    rs = grpc_client.replica_list()
    assert len(rs) == 2
    assert v.replicaCount == 2

    found_addresses = [r.address for r in rs]
    assert set(found_addresses) == set(addresses)

    cleanup_backend_file([f1, f2])


def test_shutdown(grpc_client):  # NOQA
    v = grpc_client.volume_get()
    assert v.replicaCount == 0

    f1 = create_backend_file()
    f2 = create_backend_file()
    addresses = ['file://' + f1, 'file://' + f2]
    v = grpc_client.volume_start(replicas=addresses)
    assert v.replicaCount == 2

    v = grpc_client.volume_shutdown()
    assert v.replicaCount == 0

    rs = grpc_client.replica_list()
    assert len(rs) == 0

    cleanup_backend_file([f1, f2])


def test_metric(grpc_client):  # NOQA
    replies = grpc_client.metric_get()

    cnt = 0
    while cnt < 5:
        try:
            metric = next(replies).metric
            assert metric.readBandwidth == 0
            assert metric.writeBandwidth == 0
            assert metric.readLatency == 0
            assert metric.writeLatency == 0
            assert metric.iOPS == 0
            cnt = cnt + 1
        except StopIteration:
            time.sleep(1)
