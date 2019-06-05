import time
import random
import datetime
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
from replica.replica_client import ReplicaClient  # NOQA

GRPC_URL = 'localhost:9502'
SIZE_STR = str(1024*4096)


@pytest.fixture
def grpc_client(request):
    grpc_c = ReplicaClient(GRPC_URL)
    request.addfinalizer(lambda: cleanup(grpc_c))
    return cleanup(grpc_c)


def cleanup(grpc_client):
    r = grpc_client.replica_get()
    if r.state == 'initial':
        return grpc_client
    if r.state == 'closed':
        grpc_client.replica_open()
    grpc_client.replica_delete()
    r = grpc_client.replica_reload()
    assert r.state == 'initial'
    return grpc_client


@pytest.fixture
def random_str():
    return 'random-{0}-{1}'.format(random_num(), int(time.time()))


def random_num():
    return random.randint(0, 1000000)


def test_create(grpc_client):
    r = grpc_client.replica_get()
    assert r.state == 'initial'
    assert r.size == '0'
    assert r.sectorSize == 0
    assert r.parent == ''
    assert r.head == ''

    r = grpc_client.replica_create(size=SIZE_STR)

    assert r.state == 'closed'
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'


def test_open(grpc_client):
    r = grpc_client.replica_get()
    assert r.state == 'initial'
    assert r.size == '0'
    assert r.sectorSize == 0
    assert r.parent == ''
    assert r.head == ''

    r = grpc_client.replica_create(size=SIZE_STR)

    assert r.state == 'closed'
    assert not r.dirty
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    r = grpc_client.replica_open()

    assert r.state == 'open'
    assert not r.dirty
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'


def test_close(grpc_client):
    grpc_client.replica_create(size=SIZE_STR)

    r = grpc_client.replica_open()

    assert r.state == 'open'
    assert not r.dirty
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    r = grpc_client.replica_close()

    assert r.state == 'closed'
    assert not r.dirty
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'


def test_snapshot(grpc_client):
    grpc_client.replica_create(size=SIZE_STR)

    r = grpc_client.replica_open()

    assert r.state == 'open'
    assert not r.dirty
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    r = grpc_client.replica_snapshot(
        name='000', created=datetime.datetime.utcnow().isoformat(),
        labels={"name": "000", "key": "value"})
    assert r.state == 'dirty'
    assert r.dirty
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.disks["volume-snap-000.img"].labels["name"] == "000"
    assert r.disks["volume-snap-000.img"].labels["key"] == "value"

    r = grpc_client.replica_snapshot(
        name='001', created=datetime.datetime.utcnow().isoformat())

    assert r.state == 'dirty'
    assert r.dirty
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']


def test_remove_disk(grpc_client):
    grpc_client.replica_create(size=SIZE_STR)
    grpc_client.replica_open()

    grpc_client.replica_snapshot(
        name='000', created=datetime.datetime.utcnow().isoformat())
    r = grpc_client.replica_snapshot(
        name='001', created=datetime.datetime.utcnow().isoformat())
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']

    # idempotent
    grpc_client.disk_mark_as_removed(name='003')
    grpc_client.disk_prepare_remove(name='003')

    with pytest.raises(grpc.RpcError) as e:
        grpc_client.disk_mark_as_removed(name='volume-head-002.img')
    assert "Can not mark the active" in str(e.value)

    with pytest.raises(grpc.RpcError) as e:
        grpc_client.disk_prepare_remove(name='volume-head-002.img')
    assert "Can not delete the active" in str(e.value)

    grpc_client.disk_mark_as_removed(name='001')
    ops = grpc_client.disk_prepare_remove(name='001').operations
    assert len(ops) == 0

    r = grpc_client.disk_remove(name='volume-snap-001.img')
    assert r.state == 'dirty'
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-000.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-000.img']


def test_remove_last_disk(grpc_client):
    grpc_client.replica_create(size=SIZE_STR)
    grpc_client.replica_open()

    grpc_client.replica_snapshot(
        name='000', created=datetime.datetime.utcnow().isoformat())
    r = grpc_client.replica_snapshot(
        name='001', created=datetime.datetime.utcnow().isoformat())
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']

    grpc_client.disk_mark_as_removed(name='volume-snap-000.img')

    ops = grpc_client.disk_prepare_remove(
        name='volume-snap-000.img').operations
    assert len(ops) == 2
    assert ops[0].action == "coalesce"
    assert ops[0].source == "volume-snap-000.img"
    assert ops[0].target == "volume-snap-001.img"
    assert ops[1].action == "replace"
    assert ops[1].source == "volume-snap-000.img"
    assert ops[1].target == "volume-snap-001.img"

    r = grpc_client.disk_remove(name='volume-snap-000.img')
    assert r.state == 'dirty'
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img']


def test_reload(grpc_client):
    grpc_client.replica_create(size=SIZE_STR)
    grpc_client.replica_open()

    r = grpc_client.replica_get()
    assert r.chain == ['volume-head-000.img']

    r = grpc_client.replica_snapshot(
        name='000', created=datetime.datetime.utcnow().isoformat())
    assert r.chain == ['volume-head-001.img', 'volume-snap-000.img']
    r = grpc_client.replica_snapshot(
        name='001', created=datetime.datetime.utcnow().isoformat())
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']

    r = grpc_client.disk_remove(name='volume-snap-000.img')
    assert r.state == 'dirty'
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img']

    r = grpc_client.replica_reload()
    assert r.state == 'dirty'
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img']
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'

    grpc_client.replica_close()
    r = grpc_client.replica_open()
    assert r.state == 'open'
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img']
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'


def test_reload_simple(grpc_client):
    grpc_client.replica_create(size=SIZE_STR)

    r = grpc_client.replica_open()
    assert r.state == 'open'
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    r = grpc_client.replica_reload()
    assert r.state == 'open'
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'


def test_rebuilding(grpc_client):
    grpc_client.replica_create(size=SIZE_STR)
    grpc_client.replica_open()

    r = grpc_client.replica_snapshot(
        name='001', created=datetime.datetime.utcnow().isoformat())
    assert r.state == 'dirty'
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']

    r = grpc_client.rebuilding_set(rebuilding=True)
    assert r.state == 'rebuilding'
    assert r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']

    grpc_client.replica_close()
    r = grpc_client.replica_open()
    assert r.state == 'rebuilding'
    assert r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']

    r = grpc_client.replica_reload()
    assert r.state == 'rebuilding'
    assert r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']


def test_not_rebuilding(grpc_client):
    grpc_client.replica_create(size=SIZE_STR)
    grpc_client.replica_open()

    r = grpc_client.replica_snapshot(
        name='001', created=datetime.datetime.utcnow().isoformat())
    assert r.state == 'dirty'
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']

    r = grpc_client.rebuilding_set(rebuilding=True)
    assert r.state == 'rebuilding'
    assert r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']

    r = grpc_client.rebuilding_set(rebuilding=False)
    assert r.state == 'dirty'
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']
