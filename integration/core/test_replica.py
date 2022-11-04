import time
import random
import datetime
import grpc
import pytest

from common.constants import SIZE_STR


@pytest.fixture
def random_str():
    return 'random-{0}-{1}'.format(random_num(), int(time.time()))


def random_num():
    return random.randint(0, 1000000)


def test_create(grpc_replica_client):  # NOQA
    r = grpc_replica_client.replica_get()
    assert r.state == 'initial'
    assert r.size == '0'
    assert r.sector_size == 0
    assert r.parent == ''
    assert r.head == ''

    r = grpc_replica_client.replica_create(size=SIZE_STR)

    assert r.state == 'closed'
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'


def test_open(grpc_replica_client):  # NOQA
    r = grpc_replica_client.replica_get()
    assert r.state == 'initial'
    assert r.size == '0'
    assert r.sector_size == 0
    assert r.parent == ''
    assert r.head == ''

    r = grpc_replica_client.replica_create(size=SIZE_STR)

    assert r.state == 'closed'
    assert not r.dirty
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    r = grpc_replica_client.replica_open()

    assert r.state == 'open'
    assert not r.dirty
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'


def test_close(grpc_replica_client):  # NOQA
    grpc_replica_client.replica_create(size=SIZE_STR)

    r = grpc_replica_client.replica_open()

    assert r.state == 'open'
    assert not r.dirty
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    r = grpc_replica_client.replica_close()

    assert r.state == 'closed'
    assert not r.dirty
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'


def test_snapshot(grpc_replica_client):  # NOQA
    grpc_replica_client.replica_create(size=SIZE_STR)

    r = grpc_replica_client.replica_open()

    assert r.state == 'open'
    assert not r.dirty
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    r = grpc_replica_client.replica_snapshot(
        name='000', created=datetime.datetime.utcnow().isoformat(),
        labels={"name": "000", "key": "value"})
    assert r.state == 'dirty'
    assert r.dirty
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.disks["volume-snap-000.img"].labels["name"] == "000"
    assert r.disks["volume-snap-000.img"].labels["key"] == "value"

    r = grpc_replica_client.replica_snapshot(
        name='001', created=datetime.datetime.utcnow().isoformat())

    assert r.state == 'dirty'
    assert r.dirty
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']


def test_remove_disk(grpc_replica_client):  # NOQA
    grpc_replica_client.replica_create(size=SIZE_STR)
    grpc_replica_client.replica_open()

    grpc_replica_client.replica_snapshot(
        name='000', created=datetime.datetime.utcnow().isoformat())
    r = grpc_replica_client.replica_snapshot(
        name='001', created=datetime.datetime.utcnow().isoformat())
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']

    # idempotent
    grpc_replica_client.disk_mark_as_removed(name='003')
    grpc_replica_client.disk_prepare_remove(name='003')

    with pytest.raises(grpc.RpcError) as e:
        grpc_replica_client.disk_mark_as_removed(name='volume-head-002.img')
    assert "cannot mark the active" in str(e.value)

    with pytest.raises(grpc.RpcError) as e:
        grpc_replica_client.disk_prepare_remove(name='volume-head-002.img')
    assert "cannot delete the active" in str(e.value)

    grpc_replica_client.disk_mark_as_removed(name='001')
    ops = grpc_replica_client.disk_prepare_remove(name='001').operations
    assert len(ops) == 1

    r = grpc_replica_client.disk_remove(name='volume-snap-001.img')
    assert r.state == 'dirty'
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-000.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-000.img']


def test_remove_last_disk(grpc_replica_client):  # NOQA
    grpc_replica_client.replica_create(size=SIZE_STR)
    grpc_replica_client.replica_open()

    grpc_replica_client.replica_snapshot(
        name='000', created=datetime.datetime.utcnow().isoformat())
    r = grpc_replica_client.replica_snapshot(
        name='001', created=datetime.datetime.utcnow().isoformat())
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']

    grpc_replica_client.disk_mark_as_removed(name='volume-snap-000.img')

    ops = grpc_replica_client.disk_prepare_remove(
        name='volume-snap-000.img').operations
    assert len(ops) == 2
    assert ops[0].action == "coalesce"
    assert ops[0].source == "volume-snap-000.img"
    assert ops[0].target == "volume-snap-001.img"
    assert ops[1].action == "replace"
    assert ops[1].source == "volume-snap-000.img"
    assert ops[1].target == "volume-snap-001.img"

    r = grpc_replica_client.disk_remove(name='volume-snap-000.img')
    assert r.state == 'dirty'
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img']


def test_reload(grpc_replica_client):  # NOQA
    grpc_replica_client.replica_create(size=SIZE_STR)
    grpc_replica_client.replica_open()

    r = grpc_replica_client.replica_get()
    assert r.chain == ['volume-head-000.img']

    r = grpc_replica_client.replica_snapshot(
        name='000', created=datetime.datetime.utcnow().isoformat())
    assert r.chain == ['volume-head-001.img', 'volume-snap-000.img']
    r = grpc_replica_client.replica_snapshot(
        name='001', created=datetime.datetime.utcnow().isoformat())
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']

    r = grpc_replica_client.disk_remove(name='volume-snap-000.img')
    assert r.state == 'dirty'
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img']

    r = grpc_replica_client.replica_reload()
    assert r.state == 'dirty'
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img']
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'

    grpc_replica_client.replica_close()
    r = grpc_replica_client.replica_open()
    assert r.state == 'open'
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img']
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'


def test_reload_simple(grpc_replica_client):  # NOQA
    grpc_replica_client.replica_create(size=SIZE_STR)

    r = grpc_replica_client.replica_open()
    assert r.state == 'open'
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    r = grpc_replica_client.replica_reload()
    assert r.state == 'open'
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'


def test_rebuilding(grpc_replica_client):  # NOQA
    grpc_replica_client.replica_create(size=SIZE_STR)
    grpc_replica_client.replica_open()

    r = grpc_replica_client.replica_snapshot(
        name='001', created=datetime.datetime.utcnow().isoformat())
    assert r.state == 'dirty'
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']

    r = grpc_replica_client.rebuilding_set(rebuilding=True)
    assert r.state == 'rebuilding'
    assert r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']

    grpc_replica_client.replica_close()
    r = grpc_replica_client.replica_open()
    assert r.state == 'rebuilding'
    assert r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']

    r = grpc_replica_client.replica_reload()
    assert r.state == 'rebuilding'
    assert r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']


def test_not_rebuilding(grpc_replica_client):  # NOQA
    grpc_replica_client.replica_create(size=SIZE_STR)
    grpc_replica_client.replica_open()

    r = grpc_replica_client.replica_snapshot(
        name='001', created=datetime.datetime.utcnow().isoformat())
    assert r.state == 'dirty'
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']

    r = grpc_replica_client.rebuilding_set(rebuilding=True)
    assert r.state == 'rebuilding'
    assert r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']

    r = grpc_replica_client.rebuilding_set(rebuilding=False)
    assert r.state == 'dirty'
    assert not r.rebuilding
    assert r.size == SIZE_STR
    assert r.sector_size == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']
