import time
import random
import sys
import os
import grpc

import pytest

# include directory intergration/rpc for module import
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.split(__file__)[0], "../rpc")
    )
)
from controller.controller_client import ControllerClient  # NOQA


GRPC_CONTROLLER = "localhost:9501"


@pytest.fixture
def grpc_client(request):
    c = ControllerClient(GRPC_CONTROLLER)
    request.addfinalizer(lambda: cleanup(c))
    return cleanup(c)


def cleanup(grpc_client):
    try:
        v = grpc_client.volume_get()
    except grpc.RpcError as grpc_err:
        if "Socket closed" not in grpc_err.details():
            raise grpc_err
        return grpc_client

    if v.replicaCount != 0:
        grpc_client.volume_shutdown()
    for r in grpc_client.replica_list():
        grpc_client.replica_delete(r.address)
    return grpc_client


@pytest.fixture
def random_str():
    return 'random-{0}-{1}'.format(random_num(), int(time.time()))


def random_num():
    return random.randint(0, 1000000)


def test_replica_list(grpc_client):
    replicas = grpc_client.replica_list()
    assert len(replicas) == 0


def test_replica_create(grpc_client):
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


def test_replica_delete(grpc_client):
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


def test_replica_change(grpc_client):
    f = 'file://' + random_str()
    r1 = grpc_client.replica_create(address=f)
    assert r1.mode == 'WO'

    r1 = grpc_client.replica_update(r1.address, mode='RW')
    assert r1.mode == 'RW'

    r1 = grpc_client.replica_get(r1.address)
    assert r1.mode == 'RW'


def test_start(grpc_client):
    v = grpc_client.volume_get()
    assert v.replicaCount == 0

    addresses = ['file://' + random_str(), 'file://' + random_str()]
    v = grpc_client.volume_start(replicas=addresses)

    rs = grpc_client.replica_list()
    assert len(rs) == 2
    assert v.replicaCount == 2

    found_addresses = [r.address for r in rs]
    assert set(found_addresses) == set(addresses)


def test_shutdown(grpc_client):
    v = grpc_client.volume_get()
    assert v.replicaCount == 0

    addresses = ['file://' + random_str(), 'file://' + random_str()]
    v = grpc_client.volume_start(replicas=addresses)
    assert v.replicaCount == 2

    v = grpc_client.volume_shutdown()
    assert v.replicaCount == 0

    rs = grpc_client.replica_list()
    assert len(rs) == 0


def test_metric(grpc_client):
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


def test_port_update():
    grpc_client = ControllerClient(GRPC_CONTROLLER)
    v = grpc_client.volume_get()
    assert v.replicaCount == 0

    addresses = ['file://' + random_str(), 'file://' + random_str()]
    v = grpc_client.volume_start(replicas=addresses)
    assert v.replicaCount == 2

    new_url = "localhost:9505"
    new_port = 9505
    grpc_client.port_update(new_port)

    new_grpc_client = ControllerClient(new_url)
    v = new_grpc_client .volume_get()
    assert v.replicaCount == 2

    cleanup(new_grpc_client)
    old_port = 9501
    new_grpc_client.port_update(old_port)
