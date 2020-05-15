import fcntl
import struct
import os
import grpc
import tempfile
import time
import random
import subprocess
import string
import threading

import pytest

from rpc.controller.controller_client import ControllerClient

import common.cmd as cmd
from common.util import read_file, checksum_data
from common.frontend import blockdev, get_block_device_path

from common.constants import (
    LONGHORN_BINARY, LONGHORN_UPGRADE_BINARY, LONGHORN_DEV_DIR,
    VOLUME_NAME, VOLUME_BACKING_NAME,
    SIZE, PAGE_SIZE, SIZE_STR,
    BACKUP_DIR, BACKING_FILE,
    FRONTEND_TGT_BLOCKDEV,
    RETRY_COUNTS, RETRY_INTERVAL, RETRY_COUNTS2,
    RETRY_COUNTS_SHORT, RETRY_INTERVAL_SHORT,
    PROC_STATE_STARTING, PROC_STATE_RUNNING,
    PROC_STATE_ERROR,
    ENGINE_NAME, EXPANDED_SIZE_STR,
)

thread_failed = False


def _file(f):
    return os.path.join(_base(), '../../{}'.format(f))


def _base():
    return os.path.dirname(__file__)


def cleanup_process(pm_client):
    for name in pm_client.process_list():
        try:
            pm_client.process_delete(name)
        except grpc.RpcError as e:
            if 'cannot find process' not in e.details():
                raise e
    for i in range(RETRY_COUNTS):
        ps = pm_client.process_list()
        if len(ps) == 0:
            break
        time.sleep(RETRY_INTERVAL)

    ps = pm_client.process_list()
    assert len(ps) == 0
    return pm_client


def wait_for_process_running(client, name):
    healthy = False
    for i in range(RETRY_COUNTS):
        state = client.process_get(name).status.state
        if state == PROC_STATE_RUNNING:
            healthy = True
            break
        elif state != PROC_STATE_STARTING:
            # invalid state
            assert False
        time.sleep(RETRY_INTERVAL)
    assert healthy


def wait_for_process_error(client, name):
    verified = False
    for i in range(RETRY_COUNTS):
        state = client.process_get(name).status.state
        if state == PROC_STATE_ERROR:
            verified = True
            break
        time.sleep(RETRY_INTERVAL)
    assert verified


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
    wait_for_process_running(client, name)

    return client.process_get(name)


def create_engine_process(client, name=ENGINE_NAME,
                          volume_name=VOLUME_NAME,
                          binary=LONGHORN_BINARY,
                          listen="", listen_ip="localhost",
                          size=SIZE, frontend=FRONTEND_TGT_BLOCKDEV,
                          replicas=[], backends=["file"]):
    args = ["controller", volume_name]
    if frontend != "":
        args += ["--frontend", frontend]
    for r in replicas:
        args += ["--replica", r]
    for b in backends:
        args += ["--enable-backend", b]
    client.process_create(
        name=name, binary=binary, args=args,
        port_count=1, port_args=["--listen,localhost:"])
    wait_for_process_running(client, name)

    return client.process_get(name)


def get_process_address(p):
    return "localhost:" + str(p.status.port_start)


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


def random_str():
    return 'random-{0}-{1}'.format(random_num(), int(time.time()))


def random_num():
    return random.randint(0, 1000000)


def create_backend_file():
    name = random_str()
    fo = open(name, "w+")
    fo.truncate(SIZE)
    fo.close()
    return os.path.abspath(name)


def cleanup_backend_file(paths):
    for path in paths:
        if os.path.exists(path):
            os.remove(path)


def get_dev_path(name):
    return os.path.join(LONGHORN_DEV_DIR, name)


def get_expansion_snapshot_name():
    return 'expand-{0}'.format(EXPANDED_SIZE_STR)


def get_replica_paths_from_snapshot_name(snap_name):
    replica_paths = []
    cmd = ["find", "/tmp", "-name",
           '*volume-snap-{0}.img'.format(snap_name)]
    snap_paths = subprocess.check_output(cmd).split()
    assert snap_paths
    for p in snap_paths:
        replica_paths.append(os.path.dirname(p.decode('utf-8')))
    return replica_paths


def get_snapshot_file_paths(replica_path, snap_name):
    return os.path.join(replica_path,
                        'volume-snap-{0}.img'.format(snap_name))


def get_replica_head_file_path(replica_dir):
    cmd = ["find", replica_dir, "-name",
           '*volume-head-*.img']
    return subprocess.check_output(cmd).strip()


def wait_for_rebuild_complete(url):
    completed = 0
    rebuild_status = {}
    for x in range(RETRY_COUNTS):
        completed = 0
        rebuild_status = cmd.replica_rebuild_status(url)
        for rebuild in rebuild_status.values():
            if rebuild['state'] == "complete":
                assert rebuild['progress'] == 100
                assert not rebuild['isRebuilding']
                completed += 1
            elif rebuild['state'] == "":
                assert not rebuild['isRebuilding']
                completed += 1
            # Right now add-replica/rebuild is a blocking call.
            # Hence the state won't become `in_progress` when
            # we check the rebuild status.
            elif rebuild['state'] == "in_progress":
                assert rebuild['state'] == "in_progress"
                assert rebuild['isRebuilding']
            else:
                assert rebuild['state'] == "error"
                assert rebuild['error'] != ""
                assert not rebuild['isRebuilding']
        if completed == len(rebuild_status):
            break
        time.sleep(RETRY_INTERVAL)
    return completed == len(rebuild_status)


def wait_for_purge_completion(url):
    completed = 0
    purge_status = {}
    for x in range(RETRY_COUNTS):
        completed = 0
        purge_status = cmd.snapshot_purge_status(url)
        for status in purge_status.values():
            assert status['progress'] <= 100
            assert 'isPurging' in status.keys()
            if not status['isPurging']:
                assert status['progress'] == 100
                completed += 1
            assert 'error' in status.keys()
            assert status['error'] == ''
        if completed == len(purge_status):
            break
        time.sleep(RETRY_INTERVAL)
    assert completed == len(purge_status)


def wait_for_restore_completion(url, backup_url):
    completed = 0
    rs = {}
    for x in range(RETRY_COUNTS):
        completed = 0
        rs = cmd.restore_status(url)
        for status in rs.values():
            assert 'state' in status.keys()
            if status['backupURL'] != backup_url:
                break
            if status['state'] == "complete":
                assert 'progress' in status.keys()
                assert status['progress'] == 100
                completed += 1
            elif status['state'] == "error":
                assert 'error' in status.keys()
                assert status['error'] == ""
            else:
                assert status['state'] == "in_progress"
        if completed == len(rs):
            break
        time.sleep(RETRY_INTERVAL)
    assert completed == len(rs)


def restore_with_frontend(url, engine_name, backup):
    client = ControllerClient(url)
    client.volume_frontend_shutdown()
    cmd.backup_restore(url, backup)
    wait_for_restore_completion(url, backup)
    client.volume_frontend_start(FRONTEND_TGT_BLOCKDEV)
    return


def restore_incrementally(url, backup_url, last_restored):
    cmd.restore_inc(url, backup_url, last_restored)
    wait_for_restore_completion(url, backup_url)
    return


def create_backup(url, snap, backup_target, volume_size=SIZE_STR):
    backup = cmd.backup_create(url, snap, backup_target)
    backup_info = cmd.backup_inspect(url, backup)
    assert backup_info["URL"] == backup
    assert backup_info["VolumeSize"] == volume_size
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
    client = ControllerClient(url)
    client.volume_frontend_start(FRONTEND_TGT_BLOCKDEV)


def rm_snaps(url, snaps):
    for s in snaps:
        cmd.snapshot_rm(url, s)
        cmd.snapshot_purge(url)
        wait_for_purge_completion(url)
    snap_info_list = cmd.snapshot_info(url)
    for s in snaps:
        assert s not in snap_info_list


def snapshot_revert_with_frontend(url, engine_name, name):
    client = ControllerClient(url)
    client.volume_frontend_shutdown()
    cmd.snapshot_revert(url, name)
    client.volume_frontend_start(FRONTEND_TGT_BLOCKDEV)


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
    return \
        ''.join(random.choice(string.ascii_lowercase) for x in range(length))


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
    return checksum_data(dev.readat(0, SIZE).encode('utf-8'))


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


def verify_replica_state(grpc_c, addr, state):
    if not addr.startswith("tcp://"):
        addr = "tcp://" + addr

    verified = False
    for i in range(RETRY_COUNTS_SHORT):
        replicas = grpc_c.replica_list()
        assert len(replicas) == 2
        for r in replicas:
            if r.address == addr and r.mode == state:
                verified = True
                break
        if verified:
            break

        time.sleep(RETRY_INTERVAL_SHORT)
    assert verified


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


def get_dev(grpc_replica1, grpc_replica2, grpc_controller,
            clean_backup_dir=True):
    if clean_backup_dir:
        prepare_backup_dir(BACKUP_DIR)
    open_replica(grpc_replica1)
    open_replica(grpc_replica2)

    replicas = grpc_controller.replica_list()
    assert len(replicas) == 0

    r1_url = grpc_replica1.url
    r2_url = grpc_replica2.url
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

    r1_url = grpc_backing_replica1.url
    r2_url = grpc_backing_replica2.url
    v = grpc_backing_controller.volume_start(
        replicas=[r1_url, r2_url])
    assert v.name == VOLUME_BACKING_NAME
    assert v.replicaCount == 2
    d = get_blockdev(v.name)

    return d


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


def expand_volume_with_frontend(grpc_controller_client, size):  # NOQA
    grpc_controller_client.volume_frontend_shutdown()
    grpc_controller_client.volume_expand(size)
    wait_for_volume_expansion(grpc_controller_client, size)
    grpc_controller_client.volume_frontend_start(FRONTEND_TGT_BLOCKDEV)


def wait_for_volume_expansion(grpc_controller_client, size):  # NOQA
    for i in range(RETRY_COUNTS):
        volume = grpc_controller_client.volume_get()
        if not volume.isExpanding and volume.size == size:
            break
        time.sleep(RETRY_INTERVAL)
    assert not volume.isExpanding
    assert volume.size == size
    return volume


def check_block_device_size(volume_name, size):
    device_path = get_block_device_path(volume_name)
    # BLKGETSIZE64, result is bytes as unsigned 64-bit integer (uint64)
    req = 0x80081272
    buf = ' ' * 8
    with open(device_path) as dev:
        buf = fcntl.ioctl(dev.fileno(), req, buf)
    device_size = struct.unpack('L', buf)[0]
    assert device_size == size


def wait_and_check_volume_expansion(grpc_controller_client, size):
    v = wait_for_volume_expansion(grpc_controller_client, size)
    check_block_device_size(v.name, size)


def delete_process(client, name):
    try:
        client.process_delete(name)
    except grpc.RpcError as e:
        if 'cannot find process' not in e.details():
            raise e


def wait_for_process_deletion(client, name):
    deleted = False
    for i in range(RETRY_COUNTS):
        rs = client.process_list()
        if name not in rs:
            deleted = True
            break
        time.sleep(RETRY_INTERVAL)
    assert deleted


def check_dev_existence(volume_name):
    found = False
    for i in range(RETRY_COUNTS):
        if os.path.exists(get_dev_path(volume_name)):
            found = True
            break
        time.sleep(RETRY_INTERVAL)
    assert found


def wait_for_dev_deletion(volume_name):
    found = True
    for i in range(RETRY_COUNTS):
        if not os.path.exists(get_dev_path(volume_name)):
            found = False
            break
        time.sleep(RETRY_INTERVAL)
    assert not found


def upgrade_engine(client, binary, engine_name, volume_name, replicas):
    args = ["controller", volume_name, "--frontend", FRONTEND_TGT_BLOCKDEV,
            "--upgrade"]
    for r in replicas:
        args += ["--replica", r]

    return client.process_replace(
        engine_name, binary, args,
    )
