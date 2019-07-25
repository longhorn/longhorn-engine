import random
import string
import subprocess
import time
import threading
import sys
import grpc
import tempfile
import os

import pytest

import cmd
from util import read_file, checksum_data
from frontend import blockdev
from setting import (
    INSTANCE_MANAGER, LONGHORN_BINARY,
    VOLUME_NAME, VOLUME_BACKING_NAME, VOLUME_NO_FRONTEND_NAME,
    ENGINE_NAME, ENGINE_BACKING_NAME, ENGINE_NO_FRONTEND_NAME,
    REPLICA_NAME, SIZE, PAGE_SIZE, SIZE_STR,
    BACKUP_DIR, BACKING_FILE, FRONTEND_TGT_BLOCKDEV,
    FIXED_REPLICA_PATH1, FIXED_REPLICA_PATH2,
    BACKING_FILE_PATH1, BACKING_FILE_PATH2,
    RETRY_COUNTS, RETRY_INTERVAL,
    RETRY_COUNTS_SHORT, RETRY_INTERVAL_SHORT,
    INSTANCE_MANAGER_TYPE_REPLICA, INSTANCE_MANAGER_TYPE_ENGINE,
    PROC_STATE_STARTING, PROC_STATE_RUNNING,
)

# include directory intergration/rpc for module import
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.split(__file__)[0], "../rpc")
    )
)
from instance_manager.engine_manager_client import EngineManagerClient  # NOQA
from instance_manager.process_manager_client import ProcessManagerClient  # NOQA
from replica.replica_client import ReplicaClient  # NOQA
from controller.controller_client import ControllerClient  # NOQA

thread_failed = False


def _file(f):
    return os.path.join(_base(), '../../{}'.format(f))


def _base():
    return os.path.dirname(__file__)


def process_manager_client(request, address=INSTANCE_MANAGER):
    c = ProcessManagerClient(address)
    request.addfinalizer(lambda: cleanup_process(c))
    return c


def cleanup_process(pm_client):
    cleanup_engine_process(EngineManagerClient(pm_client.address))
    for name in pm_client.process_list():
        try:
            pm_client.process_delete(name)
        except grpc.RpcError as e:
            if 'cannot find process' not in e.details():
                raise e
    for i in range(RETRY_COUNTS_SHORT):
        ps = pm_client.process_list()
        if len(ps) == 0:
            break
        time.sleep(RETRY_INTERVAL_SHORT)

    ps = pm_client.process_list()
    assert len(ps) == 0
    return pm_client


def engine_manager_client(request, address=INSTANCE_MANAGER):
    c = EngineManagerClient(address)
    request.addfinalizer(lambda: cleanup_engine_process(c))
    return c


def cleanup_engine_process(em_client):
    for _, engine in em_client.engine_list().iteritems():
        try:
            em_client.engine_delete(engine.spec.name)
        except grpc.RpcError as e:
            if 'cannot find engine' not in e.details():
                raise e
    for i in range(RETRY_COUNTS_SHORT):
        es = em_client.engine_list()
        if len(es) == 0:
            break
        time.sleep(RETRY_INTERVAL_SHORT)

    es = em_client.engine_list()
    assert len(es) == 0
    return em_client


def wait_for_process_running(client, name, type):
    healthy = False
    for i in range(RETRY_COUNTS_SHORT):
        if type == INSTANCE_MANAGER_TYPE_ENGINE:
            e = client.engine_get(name)
            state = e.status.process_status.state
        elif type == INSTANCE_MANAGER_TYPE_REPLICA:
            state = client.process_get(name).status.state
        else:
            # invalid type
            assert False
        if state == PROC_STATE_RUNNING:
            healthy = True
            break
        elif state != PROC_STATE_STARTING:
            # invalid state
            assert False
        time.sleep(RETRY_INTERVAL_SHORT)
    assert healthy


def create_replica_process(client, name, replica_dir="",
                           args=[], binary=LONGHORN_BINARY,
                           size=SIZE, port_count=15,
                           port_args=["--listen,localhost:"]):
    if not replica_dir:
        replica_dir = tempfile.mkdtemp()
    if not args:
        args = ["replica", replica_dir, "--size", str(size)]

    client.process_create(
        name=name, binary=binary, args=args,
        port_count=port_count, port_args=port_args)
    wait_for_process_running(client, name,
                             INSTANCE_MANAGER_TYPE_REPLICA)

    return client.process_get(name)


def create_engine_process(client, name,
                          volume_name=VOLUME_NAME,
                          binary=LONGHORN_BINARY,
                          listen="", listen_ip="localhost",
                          size=SIZE, frontend=FRONTEND_TGT_BLOCKDEV,
                          replicas=[], backends=["file"]):
    client.engine_create(
        name=name, volume_name=volume_name,
        binary=binary, listen=listen, listen_ip=listen_ip,
        size=size, frontend=frontend, replicas=replicas,
        backends=backends)
    wait_for_process_running(client, name,
                             INSTANCE_MANAGER_TYPE_ENGINE)

    return client.engine_get(name)


def get_replica_address(r):
    return "localhost:" + str(r.status.port_start)


def get_backend_replica_url(address):
    return "tcp://" + address


@pytest.fixture()
def grpc_engine_manager(request):
    return engine_manager_client(request)


@pytest.fixture()
def grpc_controller(request):
    return grpc_controller_client(request, ENGINE_NAME, VOLUME_NAME)


@pytest.fixture()
def grpc_controller_no_frontend(request):
    return grpc_controller_client(request, ENGINE_NO_FRONTEND_NAME,
                                  VOLUME_NO_FRONTEND_NAME,
                                  frontend="")


@pytest.fixture()
def grpc_backing_controller(request):
    return grpc_controller_client(request, ENGINE_BACKING_NAME,
                                  VOLUME_BACKING_NAME)


def grpc_controller_client(request, engine_name, volume_name,
                           frontend=FRONTEND_TGT_BLOCKDEV):
    em_client = engine_manager_client(request)
    e = create_engine_process(em_client,
                              name=engine_name,
                              volume_name=volume_name,
                              frontend=frontend)

    return ControllerClient(e.spec.listen)


def cleanup_controller(grpc_client):
    try:
        v = grpc_client.volume_get()
    except grpc.RpcError as grpc_err:
        if "Socket closed" not in grpc_err.details() and \
                "failed to connect to all addresses" not in grpc_err.details():
            raise grpc_err
        return grpc_client

    if v.replicaCount != 0:
        grpc_client.volume_shutdown()
    for r in grpc_client.replica_list():
        grpc_client.replica_delete(r.address)
    return grpc_client


def start_engine_frontend(engine_name, frontend=FRONTEND_TGT_BLOCKDEV,
                          em_address=INSTANCE_MANAGER):
    em_client = EngineManagerClient(em_address)
    em_client.frontend_start(engine_name, frontend)
    e = em_client.engine_get(engine_name)
    assert e.spec.frontend == frontend
    assert e.status.endpoint != ""


def shutdown_engine_frontend(engine_name,
                             em_address=INSTANCE_MANAGER):
    em_client = EngineManagerClient(em_address)
    em_client.frontend_shutdown(engine_name)
    e = em_client.engine_get(engine_name)
    assert e.spec.frontend == ""
    assert e.status.endpoint == ""


def wait_for_restore_completion(url):
    completed = 0
    rs = {}
    for x in range(RETRY_COUNTS):
        completed = 0
        rs = cmd.restore_status(url)
        for status in rs.values():
            assert 'state' in status.keys()
            if status['state'] == "complete":
                assert 'progress' in status.keys()
                assert status['progress'] == 100
                completed += 1
            elif status['state'] == "error":
                assert 'error' in status.keys()
                assert status['error'] == ""
            else:
                assert status['state'] == "incomplete"
        if completed == len(rs):
            break
        time.sleep(RETRY_INTERVAL)
    assert completed == len(rs)


def restore_with_frontend(url, engine_name, backup):
    shutdown_engine_frontend(engine_name)
    cmd.backup_restore(url, backup)
    wait_for_restore_completion(url)
    start_engine_frontend(engine_name)
    return


def restore_incrementally(url, backup_url, last_restored):
    cmd.restore_inc(url, backup_url, last_restored)
    wait_for_restore_completion(url)
    return


def create_backup(url, snap, backup_target):
    backup = cmd.backup_create(url, snap, backup_target)
    backup_info = cmd.backup_inspect(url, backup)
    assert backup_info["URL"] == backup
    assert backup_info["VolumeSize"] == SIZE_STR
    assert snap in backup_info["SnapshotName"]
    return backup_info


def rm_backups(url, engine_name, backups):
    for b in backups:
        cmd.backup_rm(url, b)
        with pytest.raises(subprocess.CalledProcessError):
            restore_with_frontend(url, engine_name, b)
        with pytest.raises(subprocess.CalledProcessError):
            cmd.backup_inspect(url, b)
    # Engine frontend is down, Start it up
    start_engine_frontend(engine_name)


def rm_snaps(url, snaps):
    for s in snaps:
        cmd.snapshot_rm(url, s)
        cmd.snapshot_purge(url)
    snap_info_list = cmd.snapshot_info(url)
    for s in snaps:
        assert s not in snap_info_list


def snapshot_revert_with_frontend(url, engine_name, name):
    shutdown_engine_frontend(engine_name)
    cmd.snapshot_revert(url, name)
    start_engine_frontend(engine_name)


@pytest.fixture()
def dev(request):
    grpc_replica1 = grpc_replica_client(request, REPLICA_NAME + "-1")
    grpc_replica2 = grpc_replica_client(request, REPLICA_NAME + "-2")
    grpc_controller = grpc_controller_client(
        request, ENGINE_NAME, VOLUME_NAME)

    return get_dev(grpc_replica1, grpc_replica2, grpc_controller)


@pytest.fixture()
def backing_dev(request):
    grpc_replica1 = grpc_replica_client(
        request, REPLICA_NAME + "-backing-1",
        args=["replica", tempfile.mkdtemp(), "--size", str(SIZE),
              "--backing-file", BACKING_FILE_PATH1])
    grpc_replica2 = grpc_replica_client(
        request, REPLICA_NAME + "-backing-2",
        args=["replica", tempfile.mkdtemp(), "--size", str(SIZE),
              "--backing-file", BACKING_FILE_PATH2])
    grpc_controller = grpc_controller_client(
        request, ENGINE_BACKING_NAME, VOLUME_BACKING_NAME)

    return get_backing_dev(grpc_replica1, grpc_replica2,
                           grpc_controller)


@pytest.fixture()
def grpc_replica1(request):
    return grpc_replica_client(request, REPLICA_NAME + "-1")


@pytest.fixture()
def grpc_replica2(request):
    return grpc_replica_client(request, REPLICA_NAME + "-2")


@pytest.fixture()
def grpc_backing_replica1(request):
    return grpc_replica_client(
        request, REPLICA_NAME + "-backing-1",
        args=["replica", tempfile.mkdtemp(),
              "--backing-file", BACKING_FILE_PATH1,
              "--size", str(SIZE)])


@pytest.fixture()
def grpc_backing_replica2(request):
    return grpc_replica_client(
        request, REPLICA_NAME + "-backing-2",
        args=["replica", tempfile.mkdtemp(),
              "--backing-file", BACKING_FILE_PATH2,
              "--size", str(SIZE)])


@pytest.fixture()
def grpc_fixed_dir_replica1(request):
    request.addfinalizer(lambda: cleanup_replica_dir(
        FIXED_REPLICA_PATH1))
    return grpc_replica_client(
        request, REPLICA_NAME + "-fixed-dir-1",
        args=["replica", FIXED_REPLICA_PATH1, "--size", str(SIZE)])


@pytest.fixture()
def grpc_fixed_dir_replica2(request):
    request.addfinalizer(lambda: cleanup_replica_dir(
        FIXED_REPLICA_PATH2))
    return grpc_replica_client(
        request, REPLICA_NAME + "-fixed-dir-2",
        args=["replica", FIXED_REPLICA_PATH2, "--size", str(SIZE)])


@pytest.fixture()
def grpc_extra_replica1(request):
    request.addfinalizer(lambda: cleanup_replica_dir(
        FIXED_REPLICA_PATH1))
    return grpc_replica_client(
        request, REPLICA_NAME + "-extra-1",
        args=["replica", FIXED_REPLICA_PATH1, "--size", str(SIZE)])


@pytest.fixture()
def grpc_extra_replica2(request):
    request.addfinalizer(lambda: cleanup_replica_dir(
        FIXED_REPLICA_PATH2))
    return grpc_replica_client(
        request, REPLICA_NAME + "-extra-2",
        args=["replica", FIXED_REPLICA_PATH2, "--size", str(SIZE)])


@pytest.fixture
def grpc_replica_client(request, replica_name, args=[]):
    pm_client = process_manager_client(request)
    r = create_replica_process(
        pm_client, replica_name, args=args)

    listen = get_replica_address(r)

    c = ReplicaClient(listen)
    return cleanup_replica(c)


def cleanup_replica(grpc_client):
    r = grpc_client.replica_get()
    if r.state == 'initial':
        return grpc_client
    if r.state == 'closed':
        grpc_client.replica_open()
    grpc_client.replica_delete()
    r = grpc_client.replica_reload()
    assert r.state == 'initial'
    return grpc_client


def cleanup_replica_dir(dir=""):
    if dir and os.path.exists(dir):
        try:
            cmd = ['rm', '-r', dir + "*"]
            subprocess.check_call(cmd)
        except Exception:
            pass


def open_replica(grpc_client, backing_file=None):
    r = grpc_client.replica_get()
    assert r.state == 'initial'
    assert r.size == '0'
    assert r.sectorSize == 0
    assert r.parent == ''
    assert r.head == ''

    r = grpc_client.replica_create(size=str(1024 * 4096))

    assert r.state == 'closed'
    assert r.size == str(1024 * 4096)
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    return r


def get_blockdev(volume):
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


def verify_replica_state(grpc_c, index, state):
    for i in range(10):
        replicas = grpc_c.replica_list()
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


def get_dev(grpc_replica1, grpc_replica2, grpc_controller):
    prepare_backup_dir(BACKUP_DIR)
    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    r1_url = get_backend_replica_url(grpc_replica1.address)
    r2_url = get_backend_replica_url(grpc_replica2.address)
    v = grpc_controller.volume_start(replicas=[r1_url, r2_url])
    assert v.replicaCount == 2
    d = get_blockdev(v.name)

    return d


def get_backing_dev(grpc_backing_replica1, grpc_backing_replica2,
                    grpc_backing_controller):
    prepare_backup_dir(BACKUP_DIR)
    open_replica(grpc_backing_replica1)
    open_replica(grpc_backing_replica2)

    replicas = grpc_backing_controller.replica_list()
    assert len(replicas) == 0

    r1_url = get_backend_replica_url(grpc_backing_replica1.address)
    r2_url = get_backend_replica_url(grpc_backing_replica2.address)
    v = grpc_backing_controller.volume_start(
        replicas=[r1_url, r2_url])
    assert v.name == VOLUME_BACKING_NAME
    assert v.replicaCount == 2
    d = get_blockdev(v.name)

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
            offset = random.randrange(0, int(SIZE) - size, PAGE_SIZE)
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
    return random.randint(1, length_limit - 1)


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
    def __init__(self, dev, data, controller_addr):
        self.dev = dev
        self.data = data
        self.controller_addr = controller_addr
        self.data.write_and_verify_data(self.dev)
        self.checksum = checksum_dev(self.dev)
        self.name = cmd.snapshot_create(controller_addr)

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
