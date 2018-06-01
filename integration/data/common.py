import random
import string
import subprocess
import hashlib
import time
import threading

import os
from os import path

import pytest
import cattle

from frontend import restdev, blockdev
from frontend import PAGE_SIZE, LONGHORN_DEV_DIR, get_socket_path  # NOQA


REPLICA1 = 'tcp://localhost:9502'
REPLICA1_SCHEMA = 'http://localhost:9502/v1/schemas'
REPLICA2 = 'tcp://localhost:9505'
REPLICA2_SCHEMA = 'http://localhost:9505/v1/schemas'

BACKED_REPLICA1 = 'tcp://localhost:9602'
BACKED_REPLICA1_SCHEMA = 'http://localhost:9602/v1/schemas'
BACKED_REPLICA2 = 'tcp://localhost:9605'
BACKED_REPLICA2_SCHEMA = 'http://localhost:9605/v1/schemas'

UPGRADE_REPLICA1 = 'tcp://localhost:9512'
UPGRADE_REPLICA1_SCHEMA = 'http://localhost:9512/v1/schemas'
UPGRADE_REPLICA2 = 'tcp://localhost:9515'
UPGRADE_REPLICA2_SCHEMA = 'http://localhost:9515/v1/schemas'

LONGHORN_BINARY = './bin/longhorn'
LONGHORN_UPGRADE_BINARY = '/opt/longhorn'

SIZE = 4 * 1024 * 1024

BACKUP_DIR = '/data/backupbucket'
BACKUP_DEST = 'vfs://' + BACKUP_DIR

BACKING_FILE = 'backing_file.raw'

VOLUME_NAME = 'test-volume_1.0'
VOLUME_HEAD = 'volume-head'

thread_failed = False


def _file(f):
    return path.join(_base(), '../../{}'.format(f))


def _base():
    return path.dirname(__file__)


@pytest.fixture()
def dev(request):
    replica = replica_client(request, REPLICA1_SCHEMA)
    replica2 = replica_client(request, REPLICA2_SCHEMA)
    controller = controller_client(request)

    return get_dev(replica, replica2, controller)


@pytest.fixture()
def backing_dev(request):
    replica = replica_client(request, BACKED_REPLICA1_SCHEMA)
    replica2 = replica_client(request, BACKED_REPLICA2_SCHEMA)
    controller = controller_client(request)

    return get_backing_dev(replica, replica2, controller)


@pytest.fixture()
def controller(request):
    return controller_client(request)


def controller_client(request):
    url = 'http://localhost:9501/v1/schemas'
    c = cattle.from_env(url=url)
    request.addfinalizer(lambda: cleanup_controller(c))
    c = cleanup_controller(c)
    assert c.list_volume()[0].replicaCount == 0
    return c


def cleanup_controller(client):
    v = client.list_volume()[0]
    if v.replicaCount != 0:
        v = v.shutdown()
    for r in client.list_replica():
        client.delete(r)
    return client


@pytest.fixture()
def replica1(request):
    return replica_client(request, REPLICA1_SCHEMA)


@pytest.fixture()
def replica2(request):
    return replica_client(request, REPLICA2_SCHEMA)


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
    return restdev(VOLUME_NAME)


def get_blockdev():
    dev = blockdev(VOLUME_NAME)
    for i in range(10):
        if not dev.ready():
            time.sleep(1)
    assert dev.ready()
    return dev


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


def data_verifier(dev, times, offset, length):
    try:
        verify_loop(dev, times, offset, length)
    except Exception as ex:
        global thread_failed
        thread_failed = True
        raise ex


def verify_loop(dev, times, offset, length):
    for i in range(times):
        data = random_string(length)
        verify_data(dev, offset, data)


def verify_replica_state(c, index, state):
    for i in range(10):
        replicas = c.list_replica()
        assert len(replicas) == 2

        if replicas[index].mode == state:
            break

        time.sleep(0.2)

    assert replicas[index].mode == state


def verify_read(dev, offset, data):
    for i in range(10):
        readed = read_dev(dev, offset, len(data))
        assert data == readed


def verify_async(dev, times, length, count):
    assert length * count < SIZE

    threads = []
    for i in range(count):
        t = threading.Thread(target=data_verifier,
                             args=(dev, times, i * PAGE_SIZE, length))
        t.start()
        threads.append(t)

    for i in range(count):
        threads[i].join()

    global thread_failed
    if thread_failed:
        thread_failed = False
        raise Exception("data_verifier thread failed")


@pytest.fixture()
def backing_replica1(request):
    return replica_client(request, BACKED_REPLICA1_SCHEMA)


@pytest.fixture()
def backing_replica2(request):
    return replica_client(request, BACKED_REPLICA2_SCHEMA)


def get_dev(replica1, replica2, controller):
    prepare_backup_dir(BACKUP_DIR)
    open_replica(replica1)
    open_replica(replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = controller.list_volume()[0]
    v = v.start(replicas=[
        REPLICA1,
        REPLICA2
    ])
    assert v.replicaCount == 2
    d = get_blockdev()

    return d


def get_backing_dev(backing_replica1, backing_replica2,
                    controller):
    prepare_backup_dir(BACKUP_DIR)
    open_replica(backing_replica1)
    open_replica(backing_replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = controller.list_volume()[0]
    v = v.start(replicas=[
        BACKED_REPLICA1,
        BACKED_REPLICA2
    ])
    assert v.replicaCount == 2
    d = get_blockdev()

    return d


@pytest.fixture()
def backup_targets():
    env = dict(os.environ)
    assert env["BACKUPTARGETS"] != ""
    return env["BACKUPTARGETS"].split(",")
