import time
import random
import datetime

import pytest
import cattle


@pytest.fixture
def client(request):
    url = 'http://localhost:9502/v1/schemas'
    c = cattle.from_env(url=url)
    request.addfinalizer(lambda: cleanup(c))
    return cleanup(c)


def cleanup(client):
    r = client.list_replica()[0]
    if r.state == 'initial':
        return client
    if 'open' in r:
        r = r.open()
    client.delete(r)
    r = client.reload(r)
    assert r.state == 'initial'
    return client


@pytest.fixture
def random_str():
    return 'random-{0}-{1}'.format(random_num(), int(time.time()))


def random_num():
    return random.randint(0, 1000000)


def test_create(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    assert r.state == 'initial'
    assert r.size == '0'
    assert r.sectorSize == 0
    assert r.parent == ''
    assert r.head == ''

    r = r.create(size=str(1024*4096))

    assert r.state == 'closed'
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'


def test_open(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    assert r.state == 'initial'
    assert r.size == '0'
    assert r.sectorSize == 0
    assert r.parent == ''
    assert r.head == ''

    r = r.create(size=str(1024*4096))

    assert r.state == 'closed'
    assert not r.dirty
    assert not r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    r = r.open()

    assert r.state == 'open'
    assert not r.dirty
    assert not r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'


def test_close(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    r = r.create(size=str(1024*4096))
    r = r.open()

    assert r.state == 'open'
    assert not r.dirty
    assert not r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    r = r.close()

    assert r.state == 'closed'
    assert not r.dirty
    assert not r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'


def test_snapshot(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    r = r.create(size=str(1024*4096))
    r = r.open()

    assert r.state == 'open'
    assert not r.dirty
    assert not r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    r = r.snapshot(name='000', created=datetime.datetime.utcnow().isoformat(),
                   labels={"name": "000", "key": "value"})
    assert r.state == 'dirty'
    assert r.dirty
    assert not r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.disks["volume-snap-000.img"].labels["name"] == "000"
    assert r.disks["volume-snap-000.img"].labels["key"] == "value"

    r = r.snapshot(name='001', created=datetime.datetime.utcnow().isoformat())

    assert r.state == 'dirty'
    assert r.dirty
    assert not r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']


def test_remove_disk(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    r = r.create(size=str(1024*4096))
    r = r.open()
    r = r.snapshot(name='000', created=datetime.datetime.utcnow().isoformat())
    r = r.snapshot(name='001', created=datetime.datetime.utcnow().isoformat())
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']

    with pytest.raises(cattle.ApiError) as e:
        r.markdiskasremoved(name='003')
    assert "Can not find snapshot" in str(e.value)

    with pytest.raises(cattle.ApiError) as e:
        r.prepareremovedisk(name='003')
    assert "Can not find snapshot" in str(e.value)

    with pytest.raises(cattle.ApiError) as e:
        r.markdiskasremoved(name='volume-head-002.img')
    assert "Can not mark the active" in str(e.value)

    with pytest.raises(cattle.ApiError) as e:
        r.prepareremovedisk(name='volume-head-002.img')
    assert "Can not delete the active" in str(e.value)

    r.markdiskasremoved(name='001')
    ops = r.prepareremovedisk(name='001')["operations"]
    assert len(ops) == 0

    r = r.removedisk(name='volume-snap-001.img')
    assert r.state == 'dirty'
    assert not r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-000.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-000.img']


def test_remove_last_disk(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    r = r.create(size=str(1024*4096))
    r = r.open()
    r = r.snapshot(name='000', created=datetime.datetime.utcnow().isoformat())
    r = r.snapshot(name='001', created=datetime.datetime.utcnow().isoformat())
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']

    r.markdiskasremoved(name='volume-snap-000.img')
    ops = r.prepareremovedisk(name='volume-snap-000.img')["operations"]
    assert len(ops) == 2
    assert ops[0].action == "coalesce"
    assert ops[0].source == "volume-snap-000.img"
    assert ops[0].target == "volume-snap-001.img"
    assert ops[1].action == "replace"
    assert ops[1].source == "volume-snap-000.img"
    assert ops[1].target == "volume-snap-001.img"

    r = r.removedisk(name='volume-snap-000.img')
    assert r.state == 'dirty'
    assert not r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img']


def test_reload(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    r = r.create(size=str(1024*4096))
    r = r.open()
    assert r.chain == ['volume-head-000.img']
    r = r.snapshot(name='000', created=datetime.datetime.utcnow().isoformat())
    assert r.chain == ['volume-head-001.img', 'volume-snap-000.img']
    r = r.snapshot(name='001', created=datetime.datetime.utcnow().isoformat())
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']

    r = r.removedisk(name='volume-snap-000.img')
    assert r.state == 'dirty'
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img']

    r = r.reload()
    assert r.state == 'dirty'
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img']
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'

    r = r.close().open()
    assert r.state == 'open'
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img']
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'


def test_reload_simple(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    r = r.create(size=str(1024*4096))
    r = r.open()
    assert r.state == 'open'
    assert not r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    r = r.reload()
    assert r.state == 'open'
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'


def test_rebuilding(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    r = r.create(size=str(1024*4096))
    r = r.open()
    r = r.snapshot(name='001', created=datetime.datetime.utcnow().isoformat())
    assert r.state == 'dirty'
    assert not r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']

    r = r.setrebuilding(rebuilding=True)
    assert r.state == 'rebuilding'
    assert r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']

    r = r.close().open()
    assert r.state == 'rebuilding'
    assert r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']

    r = r.reload()
    assert r.state == 'rebuilding'
    assert r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']


def test_not_rebuilding(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    r = r.create(size=str(1024*4096))
    r = r.open()
    r = r.snapshot(name='001', created=datetime.datetime.utcnow().isoformat())
    assert r.state == 'dirty'
    assert not r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']

    r = r.setrebuilding(rebuilding=True)
    assert r.state == 'rebuilding'
    assert r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']

    r = r.setrebuilding(rebuilding=False)
    assert r.state == 'dirty'
    assert not r.rebuilding
    assert r.size == str(1024*4096)
    assert r.sectorSize == 512
    assert r.parent == 'volume-snap-001.img'
    assert r.head == 'volume-head-001.img'
    assert r.chain == ['volume-head-001.img', 'volume-snap-001.img']
