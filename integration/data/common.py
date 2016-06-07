import random
import string
import base64
import subprocess

import os
from os import path

import pytest
import cattle

REPLICA = 'tcp://localhost:9502'
REPLICA2 = 'tcp://localhost:9505'

SIZE = 1024 * 4096

BACKUP_DEST = '/tmp/longhorn-backup'


@pytest.fixture()
def dev(request):
    controller = controller_client(request)
    replica = replica_client(request)
    replica2 = replica2_client(request)

    open_replica(replica)
    open_replica(replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = controller.list_volume()[0]
    v = v.start(replicas=[
        REPLICA,
        REPLICA2
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


def replica_client(request):
    url = 'http://localhost:9502/v1/schemas'
    c = cattle.from_env(url=url)
    request.addfinalizer(lambda: cleanup_replica(c))
    return cleanup_replica(c)


def replica2_client(request):
    url = 'http://localhost:9505/v1/schemas'
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


def open_replica(client, backing_file = None):
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
    url = 'http://localhost:9414/v1/schemas'
    c = cattle.from_env(url=url)
    dev = c.list_volume()[0]
    assert dev.name == "test-volume"
    return dev


def write_dev(dev, offset, data):
    l = len(data)
    encoded_data = base64.encodestring(data)
    dev.write(offset=offset, length=l, data=encoded_data)


def read_dev(dev, offset, length):
    data = dev.read(offset=offset, length=length)["data"]
    return base64.decodestring(data)


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
