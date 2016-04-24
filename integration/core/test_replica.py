import time
import random

import pytest
import cattle


@pytest.fixture
def client(request):
    url = 'http://localhost:9502/v1/schemas'
    c = cattle.from_env(url=url)
    request.addfinalizer(lambda: cleanup(c))
    return cleanup(c)


def cleanup(client):
    for r in client.list_replica():
        if 'close' in r:
            client.delete(r)
        else:
            client.delete(r.open(size='4096'))
    return client


@pytest.fixture
def random_str():
    return 'random-{0}-{1}'.format(random_num(), int(time.time()))


def random_num():
    return random.randint(0, 1000000)


def test_open(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    assert r.state == 'closed'
    assert r.size == ''
    assert r.sectorSize == 0
    assert r.parent == ''
    assert r.head == ''

    r = r.open(size=str(1024*4096))

    assert r.state == 'open'
    assert r.size == str(1024*4096)
    assert r.sectorSize == 4096
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'


def test_close(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    assert r.state == 'closed'
    assert r.size == ''
    assert r.sectorSize == 0
    assert r.parent == ''
    assert r.head == ''

    r = r.open(size=str(1024*4096))

    assert r.state == 'open'
    assert r.size == str(1024*4096)
    assert r.sectorSize == 4096
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    r = r.close()

    assert r.state == 'closed'
    assert r.size == ''
    assert r.sectorSize == 0
    assert r.parent == ''
    assert r.head == ''


def test_snapshot(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    assert r.state == 'closed'
    assert r.size == ''
    assert r.sectorSize == 0
    assert r.parent == ''
    assert r.head == ''

    r = r.open(size=str(1024*4096))

    assert r.state == 'open'
    assert r.size == str(1024*4096)
    assert r.sectorSize == 4096
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    r = r.snapshot(name='000')
    r = r.snapshot(name='001')

    assert r.state == 'open'
    assert r.size == str(1024*4096)
    assert r.sectorSize == 4096
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']


def test_remove_disk(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    r = r.open(size=str(1024*4096))
    r = r.snapshot(name='000')
    r = r.snapshot(name='001')
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']

    r = r.removedisk(name='volume-snap-001.img')
    assert r.state == 'open'
    assert r.size == str(1024*4096)
    assert r.sectorSize == 4096
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-000.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-000.img']


def test_remove_last_disk(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    r = r.open(size=str(1024*4096))
    r = r.snapshot(name='000')
    r = r.snapshot(name='001')
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']

    r = r.removedisk(name='volume-snap-000.img')
    assert r.state == 'open'
    assert r.size == str(1024*4096)
    assert r.sectorSize == 4096
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img']


def test_reload(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    r = r.open(size=str(1024*4096))
    r = r.snapshot(name='000')
    r = r.snapshot(name='001')
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img',
                       'volume-snap-000.img']

    r = r.removedisk(name='volume-snap-000.img')
    assert r.state == 'open'
    assert r.size == str(1024*4096)
    assert r.sectorSize == 4096
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img']

    r = r.reload()
    assert r.state == 'open'
    assert r.size == str(1024*4096)
    assert r.sectorSize == 4096
    assert r.chain == ['volume-head-002.img', 'volume-snap-001.img']
    assert r.head == 'volume-head-002.img'
    assert r.parent == 'volume-snap-001.img'


def test_reload_simple(client):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    assert r.state == 'closed'
    assert r.size == ''
    assert r.sectorSize == 0
    assert r.parent == ''
    assert r.head == ''

    r = r.open(size=str(1024*4096))
    assert r.state == 'open'
    assert r.size == str(1024*4096)
    assert r.sectorSize == 4096
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    r = r.reload()
    assert r.state == 'open'
    assert r.size == str(1024*4096)
    assert r.sectorSize == 4096
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'
