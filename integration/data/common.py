import random
import string
import subprocess
import hashlib

import os
from os import path

import pytest
import cattle

from frontend import restdev


REPLICA1 = 'tcp://localhost:9502'
REPLICA1_SCHEMA = 'http://localhost:9502/v1/schemas'
REPLICA2 = 'tcp://localhost:9505'
REPLICA2_SCHEMA = 'http://localhost:9505/v1/schemas'

BACKED_REPLICA1 = 'tcp://localhost:9602'
BACKED_REPLICA1_SCHEMA = 'http://localhost:9602/v1/schemas'
BACKED_REPLICA2 = 'tcp://localhost:9605'
BACKED_REPLICA2_SCHEMA = 'http://localhost:9605/v1/schemas'

SIZE = 4 * 1024 * 1024

BACKUP_DIR = '/tmp/longhorn-backup'
BACKUP_DEST = 'vfs://' + BACKUP_DIR

BACKING_FILE = 'backing_file.raw'


def _file(f):
    return path.join(_base(), '../../{}'.format(f))


def _base():
    return path.dirname(__file__)


@pytest.fixture()
def dev(request):
    prepare_backup_dir(BACKUP_DIR)
    controller = controller_client(request)
    replica = replica_client(request, REPLICA1_SCHEMA)
    replica2 = replica_client(request, REPLICA2_SCHEMA)

    open_replica(replica)
    open_replica(replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = controller.list_volume()[0]
    v = v.start(replicas=[
        REPLICA1,
        REPLICA2
    ])
    assert v.replicaCount == 2
    d = get_restdev()

    return d


@pytest.fixture()
def backing_dev(request):
    prepare_backup_dir(BACKUP_DIR)
    controller = controller_client(request)
    replica = replica_client(request, BACKED_REPLICA1_SCHEMA)
    replica2 = replica_client(request, BACKED_REPLICA2_SCHEMA)

    open_replica(replica)
    open_replica(replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = controller.list_volume()[0]
    v = v.start(replicas=[
        BACKED_REPLICA1,
        BACKED_REPLICA2
    ])
    assert v.replicaCount == 2
    d = get_restdev()

    return d


def controller_client(request):
    url = 'http://localhost:9501/v1/schemas'
    c = cattle.from_env(url=url)
    request.addfinalizer(lambda: cleanup_controller(c))
    c = cleanup_controller(c)
    assert c.list_volume()[0].replicaCount == 0
    return c


def cleanup_controller(client):
    for r in client.list_replica():
        client.delete(r)
    return client


def replica_client(request, url):
    c = cattle.from_env(url=url)
    request.addfinalizer(lambda: cleanup_replica(c))
    return cleanup_replica(c)


def cleanup_replica(client):
    r = client.list_replica()[0]
    if r.state == 'initial':
        return client
    if 'open' in r:
        r = r.open()
    client.delete(r)
    r = client.reload(r)
    assert r.state == 'initial'
    return client


def open_replica(client, backing_file=None):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    assert r.state == 'initial'
    assert r.size == '0'
    assert r.sectorSize == 0
    assert r.parent == ''
    assert r.head == ''

    r = r.create(size=str(1024 * 4096))

    assert r.state == 'closed'
    assert r.size == str(1024 * 4096)
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    return r


def get_restdev():
    return restdev("test-volume")


def write_dev(dev, offset, data):
    return dev.writeat(offset, data)


def read_dev(dev, offset, length):
    return dev.readat(offset, length)


def random_string(length):
    return ''.join(random.choice(string.lowercase) for x in range(length))


def verify_data(dev, offset, data):
    write_dev(dev, offset, data)
    readed = read_dev(dev, offset, len(data))
    assert data == readed


def prepare_backup_dir(backup_dir):
    if os.path.exists(backup_dir):
        subprocess.check_call(["rm", "-rf", backup_dir])

    os.makedirs(backup_dir)
    assert os.path.exists(backup_dir)


def read_from_backing_file(offset, length):
    p = _file(BACKING_FILE)
    assert path.exists(p)
    f = open(p, 'r')
    f.seek(offset)
    data = f.read(length)
    f.close()
    return data


def checksum_dev(dev):
    return hashlib.sha512(dev.readat(0, SIZE)).hexdigest()
