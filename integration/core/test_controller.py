import time
import sys
import os
import grpc
import pytest

from common import grpc_controller_client as grpc_client  # NOQA
from common import random_str

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
    f = 'file://' + random_str()
    replica = grpc_client.replica_create(address=f)
    assert replica.address == f

    grpc_client.replica_create(address=f)
    grpc_client.replica_create(address=f)

    rs = grpc_client.replica_list()
    assert len(rs) == 1
    assert rs[0].address == f
    assert rs[0].mode == 'WO'

    f2 = 'file://' + random_str()
    with pytest.raises(grpc.RpcError) as e:
        grpc_client.replica_create(address=f2)
    assert 'Can only have one WO replica at a time' in str(e.value)

    r = grpc_client.replica_update(rs[0].address, mode='RW')
    assert r.mode == 'RW'

    replica2 = grpc_client.replica_create(address=f2)
    assert replica2.address == f2

    rs = grpc_client.replica_list()
    assert len(rs) == 2


def test_replica_delete(grpc_client):  # NOQA
    f = 'file://' + random_str()
    r1 = grpc_client.replica_create(address=f+'1')
    grpc_client.replica_update(r1.address, mode='RW')
    r2 = grpc_client.replica_create(address=f+'2')
    grpc_client.replica_update(r2.address, mode='RW')
    r3 = grpc_client.replica_create(address=f+'3')
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


def test_replica_change(grpc_client):  # NOQA
    f = 'file://' + random_str()
    r1 = grpc_client.replica_create(address=f)
    assert r1.mode == 'WO'

    r1 = grpc_client.replica_update(r1.address, mode='RW')
    assert r1.mode == 'RW'

    r1 = grpc_client.replica_get(r1.address)
    assert r1.mode == 'RW'


def test_start(grpc_client):  # NOQA
    v = grpc_client.volume_get()
    assert v.replicaCount == 0

    addresses = ['file://' + random_str(), 'file://' + random_str()]
    v = grpc_client.volume_start(replicas=addresses)

    rs = grpc_client.replica_list()
    assert len(rs) == 2
    assert v.replicaCount == 2

    found_addresses = [r.address for r in rs]
    assert set(found_addresses) == set(addresses)


def test_shutdown(grpc_client):  # NOQA
    v = grpc_client.volume_get()
    assert v.replicaCount == 0

    addresses = ['file://' + random_str(), 'file://' + random_str()]
    v = grpc_client.volume_start(replicas=addresses)
    assert v.replicaCount == 2

    v = grpc_client.volume_shutdown()
    assert v.replicaCount == 0

    rs = grpc_client.replica_list()
    assert len(rs) == 0


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
