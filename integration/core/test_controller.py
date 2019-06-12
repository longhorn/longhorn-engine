import time
import random
import sys
import os
import grpc

import pytest
import cattle

# include directory intergration/rpc for module import
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.split(__file__)[0], "../rpc")
    )
)
from controller.controller_client import ControllerClient  # NOQA


GRPC_CONTROLLER = "localhost:9505"


@pytest.fixture
def client(request):
    url = 'http://localhost:9501/v1/schemas'
    c = cattle.from_env(url=url)
    grpc_c = ControllerClient(GRPC_CONTROLLER)
    request.addfinalizer(lambda: cleanup(c, grpc_c))
    return cleanup(c, grpc_c)


@pytest.fixture
def grpc_client():
    return ControllerClient(GRPC_CONTROLLER)


def cleanup(client, grpc_client):
    v = client.list_volume()[0]
    if v.replicaCount != 0:
        grpc_client.volume_shutdown()
    for r in client.list_replica():
        client.delete(r)
    return client


@pytest.fixture
def random_str():
    return 'random-{0}-{1}'.format(random_num(), int(time.time()))


def random_num():
    return random.randint(0, 1000000)


def test_replica_list(client):
    replicas = client.list_replica()
    assert len(replicas) == 0


def test_replica_create(client, grpc_client):
    f = 'file://' + random_str()
    replica = grpc_client.replica_create(address=f)
    assert replica.address == f

    grpc_client.replica_create(address=f)
    grpc_client.replica_create(address=f)

    r = client.list_replica()
    assert len(r) == 1
    assert r[0].address == f
    assert r[0].mode == 'WO'

    f2 = 'file://' + random_str()
    with pytest.raises(grpc.RpcError) as e:
        grpc_client.replica_create(address=f2)
    assert 'Can only have one WO replica at a time' in str(e.value)

    r = grpc_client.replica_update(r[0].address, mode='RW')
    assert r.mode == 'RW'

    replica2 = grpc_client.replica_create(address=f2)
    assert replica2.address == f2

    r = client.list_replica()
    assert len(r) == 2


def test_replica_delete(client, grpc_client):
    f = 'file://' + random_str()
    r1 = grpc_client.replica_create(address=f+'1')
    grpc_client.replica_update(r1.address, mode='RW')
    r2 = grpc_client.replica_create(address=f+'2')
    grpc_client.replica_update(r2.address, mode='RW')
    r3 = grpc_client.replica_create(address=f+'3')
    grpc_client.replica_update(r3.address, mode='RW')

    r = client.list_replica()
    assert len(r) == 3

    client.delete(r1)
    r = client.list_replica()
    assert len(r) == 2

    client.delete(r1)
    r = client.list_replica()
    assert len(r) == 2

    client.delete(r2)
    r = client.list_replica()
    assert len(r) == 1

    client.delete(r3)
    r = client.list_replica()
    assert len(r) == 0


def test_replica_change(client, grpc_client):
    f = 'file://' + random_str()
    r1 = grpc_client.replica_create(address=f)
    assert r1.mode == 'WO'

    r1 = grpc_client.replica_update(r1.address, mode='RW')
    assert r1.mode == 'RW'

    r1 = client.reload(r1)
    assert r1.mode == 'RW'


def test_start(client, grpc_client):
    vs = client.list_volume()
    assert len(vs) == 1

    v = vs[0]
    assert v.replicaCount == 0

    addresses = ['file://' + random_str(), 'file://' + random_str()]
    v = grpc_client.volume_start(replicas=addresses)

    rs = client.list_replica()
    assert len(rs) == 2
    assert v.replicaCount == 2

    found_addresses = [r.address for r in rs]
    assert set(found_addresses) == set(addresses)


def test_shutdown(client, grpc_client):
    vs = client.list_volume()
    assert len(vs) == 1
    v = vs[0]
    assert v.replicaCount == 0

    addresses = ['file://' + random_str(), 'file://' + random_str()]
    v = grpc_client.volume_start(replicas=addresses)
    assert v.replicaCount == 2

    v = grpc_client.volume_shutdown()
    assert v.replicaCount == 0

    r = client.list_replica()
    assert len(r) == 0
