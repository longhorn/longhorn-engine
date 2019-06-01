import random
import string
import subprocess
import time
import threading
import sys

import os
from os import path

import pytest
import cattle

from cmd import snapshot_create
from utils import read_file, checksum_data, SIZE
from frontend import restdev, blockdev
from frontend import PAGE_SIZE, LONGHORN_DEV_DIR, get_socket_path  # NOQA

# include directory intergration/rpc for module import
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.split(__file__)[0], "../rpc")
    )
)
from replica.replica_client import ReplicaClient  # NOQA


REPLICA1 = 'tcp://localhost:9502'
REPLICA1_SCHEMA = 'http://localhost:9502/v1/schemas'
GRPC_REPLICA1 = 'localhost:9505'
REPLICA2 = 'tcp://localhost:9512'
REPLICA2_SCHEMA = 'http://localhost:9512/v1/schemas'
GRPC_REPLICA2 = 'localhost:9515'

BACKED_REPLICA1 = 'tcp://localhost:9602'
BACKED_REPLICA1_SCHEMA = 'http://localhost:9602/v1/schemas'
GRPC_BACKED_REPLICA1 = 'localhost:9605'
BACKED_REPLICA2 = 'tcp://localhost:9612'
BACKED_REPLICA2_SCHEMA = 'http://localhost:9612/v1/schemas'
GRPC_BACKED_REPLICA2 = 'localhost:9615'

UPGRADE_REPLICA1 = 'tcp://localhost:9522'
UPGRADE_REPLICA1_SCHEMA = 'http://localhost:9522/v1/schemas'
GRPC_UPGRADE_REPLICA1 = 'localhost:9525'
UPGRADE_REPLICA2 = 'tcp://localhost:9532'
UPGRADE_REPLICA2_SCHEMA = 'http://localhost:9532/v1/schemas'
GRPC_UPGRADE_REPLICA2 = 'localhost:9535'

STANDBY_REPLICA1 = 'tcp://localhost:9542'
STANDBY_REPLICA1_SCHEMA = 'http://localhost:9542/v1/schemas'
GRPC_STANDBY_REPLICA1 = 'localhost:9545'
STANDBY_REPLICA2 = 'tcp://localhost:9552'
STANDBY_REPLICA2_SCHEMA = 'http://localhost:9552/v1/schemas'
GRPC_STANDBY_REPLICA2 = 'localhost:9555'

STANDBY_REPLICA1_PATH = '/tmp/standby_vol_replica_1/'
STANDBY_REPLICA2_PATH = '/tmp/standby_vol_replica_2/'

CONTROLLER_SCHEMA = "http://localhost:9501/v1/schemas"
CONTROLLER_NO_FRONTEND_SCHEMA = "http://localhost:9801/v1/schemas"

LONGHORN_BINARY = './bin/longhorn'
LONGHORN_UPGRADE_BINARY = '/opt/longhorn'

BACKUP_DIR = '/data/backupbucket'
BACKUP_DEST = 'vfs://' + BACKUP_DIR

BACKING_FILE = 'backing_file.raw'

VOLUME_NAME = 'test-volume_1.0'
VOLUME2_NAME = 'test-volume_2.0'
VOLUME_HEAD = 'volume-head'

RETRY_COUNTS = 100

thread_failed = False


def _file(f):
    return path.join(_base(), '../../{}'.format(f))


def _base():
    return path.dirname(__file__)


@pytest.fixture()
def dev(request):
    replica = replica_client(request, REPLICA1_SCHEMA)
    replica2 = replica_client(request, REPLICA2_SCHEMA)
    grpc_replica1 = grpc_replica_client(GRPC_REPLICA1)
    grpc_replica2 = grpc_replica_client(GRPC_REPLICA2)
    controller = controller_client(request)

    return get_dev(replica, replica2,
                   grpc_replica1, grpc_replica2, controller)


@pytest.fixture()
def backing_dev(request):
    replica = replica_client(request, BACKED_REPLICA1_SCHEMA)
    replica2 = replica_client(request, BACKED_REPLICA2_SCHEMA)
    grpc_replica1 = grpc_replica_client(GRPC_BACKED_REPLICA1)
    grpc_replica2 = grpc_replica_client(GRPC_BACKED_REPLICA2)
    controller = controller_client(request)

    return get_backing_dev(replica, replica2,
                           grpc_replica1, grpc_replica2,
                           controller)


@pytest.fixture()
def controller(request):
    return controller_client(request)


def controller_client(request):
    url = CONTROLLER_SCHEMA
    c = cattle.from_env(url=url)
    request.addfinalizer(lambda: cleanup_controller(c))
    c = cleanup_controller(c)
    assert c.list_volume()[0].replicaCount == 0
    return c


@pytest.fixture()
def controller_no_frontend(request):
    return controller_no_frontend_client(request)


def controller_no_frontend_client(request):
    url = CONTROLLER_NO_FRONTEND_SCHEMA
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


@pytest.fixture()
def standby_replica1(request):
    return replica_client(request, STANDBY_REPLICA1_SCHEMA)


@pytest.fixture()
def standby_replica2(request):
    return replica_client(request, STANDBY_REPLICA2_SCHEMA)


@pytest.fixture()
def grpc_replica1():
    return grpc_replica_client(GRPC_REPLICA1)


@pytest.fixture()
def grpc_replica2():
    return grpc_replica_client(GRPC_REPLICA2)


@pytest.fixture()
def grpc_backing_replica1():
    return grpc_replica_client(GRPC_BACKED_REPLICA1)


@pytest.fixture()
def grpc_backing_replica2():
    return grpc_replica_client(GRPC_BACKED_REPLICA2)


@pytest.fixture()
def grpc_standby_replica1():
    return grpc_replica_client(GRPC_STANDBY_REPLICA1)


@pytest.fixture()
def grpc_standby_replica2():
    return grpc_replica_client(GRPC_STANDBY_REPLICA2)


def replica_client(request, url):
    c = cattle.from_env(url=url)
    request.addfinalizer(lambda: cleanup_replica(c))
    return cleanup_replica(c)


def grpc_replica_client(url):
    return ReplicaClient(url)


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


def open_replica(client, grpc_client, backing_file=None):
    replicas = client.list_replica()
    assert len(replicas) == 1

    r = replicas[0]
    assert r.state == 'initial'
    assert r.size == '0'
    assert r.sectorSize == '0'
    assert r.parent == ''
    assert r.head == ''

    r = grpc_client.replica_create(size=str(1024 * 4096))

    assert r.state == 'closed'
    assert r.size == str(1024 * 4096)
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    return r


def get_restdev(volume=VOLUME_NAME):
    return restdev(volume)


def get_blockdev(volume=VOLUME_NAME):
    dev = blockdev(volume)
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
    return read_file(p, offset, length)


def checksum_dev(dev):
    return checksum_data(dev.readat(0, SIZE))


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


def get_dev(replica1, replica2,
            grpc_replica1, grpc_replica2, controller):
    prepare_backup_dir(BACKUP_DIR)
    open_replica(replica1, grpc_replica1)
    open_replica(replica2, grpc_replica2)

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
                    grpc_backing_replica1, grpc_backing_replica2,
                    controller):
    prepare_backup_dir(BACKUP_DIR)
    open_replica(backing_replica1, grpc_backing_replica1)
    open_replica(backing_replica2, grpc_backing_replica2)

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


def random_offset(size, existings={}):
    assert size < PAGE_SIZE
    for i in range(RETRY_COUNTS):
        offset = 0
        if int(SIZE) != size:
            offset = random.randrange(0, int(SIZE)-size, PAGE_SIZE)
        collided = False
        # it's [start, end) vs [pos, pos + size)
        for start, end in existings.items():
            if offset + size <= start or offset >= end:
                continue
            collided = True
            break
        if not collided:
            break
    assert not collided
    existings[offset] = offset + size
    return offset


def random_length(length_limit):
    return random.randint(1, length_limit)


class Data:
    def __init__(self, offset, length, content):
        self.offset = offset
        self.length = length
        self.content = content

    def write_and_verify_data(self, dev):
        verify_data(dev, self.offset, self.content)

    def read_and_verify_data(self, dev):
        assert read_dev(dev, self.offset, self.length) == self.content

    def read_and_refute_data(self, dev):
        assert read_dev(dev, self.offset, self.length) != self.content


class Snapshot:
    def __init__(self, dev, data):
        self.dev = dev
        self.data = data
        self.data.write_and_verify_data(self.dev)
        self.checksum = checksum_dev(self.dev)
        self.name = snapshot_create()

    # verify the whole disk is at the state when snapshot was taken
    def verify_checksum(self):
        assert checksum_dev(self.dev) == self.checksum

    def verify_data(self):
        self.data.read_and_verify_data(self.dev)

    def refute_data(self):
        self.data.read_and_refute_data(self.dev)


def generate_random_data(dev, existings={}, length_limit=PAGE_SIZE):
    length = random_length(length_limit)
    return Data(random_offset(length, existings),
                length,
                random_string(length))
