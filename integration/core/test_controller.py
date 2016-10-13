import time
import random

import pytest
import cattle
from cattle import ApiError


@pytest.fixture
def client(request):
    url = 'http://localhost:9501/v1/schemas'
    c = cattle.from_env(url=url)
    request.addfinalizer(lambda: cleanup(c))
    return cleanup(c)


def cleanup(client):
    v = client.list_volume()[0]
    if v.replicaCount != 0:
        v = v.shutdown()
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


def test_replica_create(client):
    f = 'file://' + random_str()
    replica = client.create_replica(address=f)
    assert replica.address == f

    client.create_replica(address=f)
    client.create_replica(address=f)

    r = client.list_replica()
    assert len(r) == 1
    assert r[0].address == f
    assert r[0].mode == 'WO'

    f2 = 'file://' + random_str()
    with pytest.raises(ApiError) as e:
        client.create_replica(address=f2)
    assert e.value.error.status == 500
    assert e.value.error.message == 'Can only have one WO replica at a time'

    r = client.update(r[0], mode='RW')
    assert r.mode == 'RW'

    replica2 = client.create_replica(address=f2)
    assert replica2.address == f2

    r = client.list_replica()
    assert len(r) == 2


def test_replica_delete(client):
    f = 'file://' + random_str()
    r1 = client.create_replica(address=f+'1')
    client.update(r1, mode='RW')
    r2 = client.create_replica(address=f+'2')
    client.update(r2, mode='RW')
    r3 = client.create_replica(address=f+'3')
    client.update(r3, mode='RW')

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


def test_replica_change(client):
    f = 'file://' + random_str()
    r1 = client.create_replica(address=f)
    assert r1.mode == 'WO'

    r1 = client.update(r1, mode='RW')
    assert r1.mode == 'RW'

    r1 = client.reload(r1)
    assert r1.mode == 'RW'


def test_start(client):
    vs = client.list_volume()
    assert len(vs) == 1

    v = vs[0]
    assert v.replicaCount == 0

    addresses = ['file://' + random_str(), 'file://' + random_str()]
    v = v.start(replicas=addresses)

    rs = client.list_replica()
    assert len(rs) == 2
    assert v.replicaCount == 2

    found_addresses = [r.address for r in rs]
    assert set(found_addresses) == set(addresses)


def test_shutdown(client):
    vs = client.list_volume()
    assert len(vs) == 1
    v = vs[0]
    assert v.replicaCount == 0

    addresses = ['file://' + random_str(), 'file://' + random_str()]
    v = v.start(replicas=addresses)
    assert v.replicaCount == 2

    v = v.shutdown()
    assert v.replicaCount == 0

    r = client.list_replica()
    assert len(r) == 0
