import time
import random
import sys
from os import path
import os
import subprocess
import json
import datetime
import grpc

import pytest

from urlparse import urlparse

# include directory intergration/rpc for module import
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.split(__file__)[0], "../rpc")
    )
)
from replica.replica_client import ReplicaClient  # NOQA
from controller.controller_client import ControllerClient  # NOQA


GRPC_CONTROLLER = "localhost:9501"

REPLICA = 'tcp://localhost:9502'
REPLICA2 = 'tcp://localhost:9512'

GRPC_REPLICA = 'localhost:9502'
GRPC_REPLICA2 = 'localhost:9512'

BACKUP_DEST = '/data/backupbucket'

VOLUME_NAME = 'test-volume_1.0'
VOLUME_SIZE = str(4 * 1024 * 1024)  # 4M

VOLUME_HEAD = "volume-head"

VOLUME_CONFIG_FILE = "volume.cfg"
VOLUME_TMP_CONFIG_FILE = "volume.cfg.tmp"
MESSAGE_TYPE_ERROR = "error"

FRONTEND_TGT_BLOCKDEV = "tgt-blockdev"
LAUNCHER = "localhost:9510"

RETRY_COUNTS = 100


@pytest.fixture
def grpc_controller_client(request):
    c = ControllerClient(GRPC_CONTROLLER)
    request.addfinalizer(lambda: cleanup_controller(c))
    return cleanup_controller(c)


def cleanup_controller(grpc_client):
    try:
        v = grpc_client.volume_get()
    except grpc.RpcError as grpc_err:
        if "Socket closed" not in grpc_err.details():
            raise grpc_err
        return grpc_client

    if v.replicaCount != 0:
        grpc_client.volume_shutdown()
    for r in grpc_client.replica_list():
        grpc_client.replica_delete(r.address)
    return grpc_client


@pytest.fixture
def grpc_replica_client(request):
    c = ReplicaClient(GRPC_REPLICA)
    request.addfinalizer(lambda: cleanup_replica(c))
    return cleanup_replica(c)


@pytest.fixture
def grpc_replica_client2(request):
    c = ReplicaClient(GRPC_REPLICA2)
    request.addfinalizer(lambda: cleanup_replica(c))
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


@pytest.fixture()
def backup_targets():
    env = dict(os.environ)
    assert env["BACKUPTARGETS"] != ""
    return env["BACKUPTARGETS"].split(",")


@pytest.fixture
def random_str():
    return 'random-{0}-{1}'.format(random_num(), int(time.time()))


def launcher_bin():
    c = '/usr/local/bin/longhorn-engine-launcher'
    assert path.exists(c)
    return c


def launcher(url):
    return [launcher_bin(), "--url", url]


def start_engine_frontend(frontend, url=LAUNCHER):
    cmd = launcher(url) + ['engine-frontend-start', frontend]
    subprocess.check_output(cmd)


def shutdown_engine_frontend(url=LAUNCHER):
    cmd = launcher(url) + ['engine-frontend-shutdown']
    subprocess.check_output(cmd)


def random_num():
    return random.randint(0, 1000000)


def _file(f):
    return path.join(_base(), '../../{}'.format(f))


def _base():
    return path.dirname(__file__)


@pytest.fixture(scope='session')
def bin():
    c = _file('bin/longhorn')
    assert path.exists(c)
    return c


# find the path of the first file
def findfile(start, name):
    for relpath, dirs, files in os.walk(start):
        if name in files:
            full_path = os.path.join(start, relpath, name)
            return os.path.normpath(os.path.abspath(full_path))


# find the path of the first dir
def finddir(start, name):
    for relpath, dirs, files in os.walk(start):
        if name in dirs:
            full_path = os.path.join(start, relpath, name)
            return os.path.normpath(os.path.abspath(full_path))


def setup_module():
    if os.path.exists(BACKUP_DEST):
        subprocess.check_call(["rm", "-rf", BACKUP_DEST])

    os.makedirs(BACKUP_DEST)
    assert os.path.exists(BACKUP_DEST)


def open_replica(grpc_client):
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


def test_replica_add_start(bin, grpc_controller_client,
                           grpc_replica_client):
    open_replica(grpc_replica_client)

    cmd = [bin, '--debug', 'add-replica', REPLICA]
    subprocess.check_call(cmd)

    volume = grpc_controller_client.volume_get()
    assert volume.replicaCount == 1


def getNow():
    time.sleep(1)
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat()


def test_replica_add_rebuild(bin, grpc_controller_client,
                             grpc_replica_client, grpc_replica_client2):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    snap0 = "000"
    snap1 = "001"
    grpc_replica_client.replica_open()
    createtime0 = getNow()
    grpc_replica_client.replica_snapshot(
        name=snap0, created=createtime0,
        labels={"name": "snap0", "key": "value"})
    createtime1 = getNow()
    r = grpc_replica_client.replica_snapshot(
        name=snap1, user_created=True, created=createtime1)

    l = grpc_replica_client2.replica_get()

    assert r.chain == ['volume-head-002.img',
                       'volume-snap-001.img',
                       'volume-snap-000.img']

    assert l.chain != ['volume-head-002.img',
                       'volume-snap-001.img',
                       'volume-snap-000.img']

    grpc_replica_client.replica_close()
    cmd = [bin, '--debug', 'add-replica', REPLICA]
    subprocess.check_call(cmd)

    volume = grpc_controller_client.volume_get()
    assert volume.replicaCount == 1

    cmd = [bin, '--debug', 'add-replica', REPLICA2]
    subprocess.check_call(cmd)

    volume = grpc_controller_client.volume_get()
    assert volume.replicaCount == 2

    replicas = grpc_controller_client.replica_list()
    assert len(replicas) == 2

    for r in replicas:
        assert r.mode == 'RW'

    cmd = [bin, '--debug', 'snapshot', 'info']
    output = subprocess.check_output(cmd)
    info = json.loads(output)

    # two existing snapshots and one system snapshot due to rebuild
    # and volume-head
    volumehead = "volume-head"
    assert len(info) == 4
    for name in info:
        if name != snap0 and name != snap1 and name != volumehead:
            snapreb = name
            break

    head_info = info[volumehead]
    assert head_info["name"] == volumehead
    assert head_info["parent"] == snapreb
    assert not head_info["children"]
    assert head_info["removed"] is False
    assert head_info["usercreated"] is False
    assert head_info["size"] == "0"

    snapreb_info = info[snapreb]
    assert snapreb_info["name"] == snapreb
    assert snapreb_info["parent"] == snap1
    assert volumehead in snapreb_info["children"]
    assert snapreb_info["removed"] is False
    assert snapreb_info["usercreated"] is False
    assert snapreb_info["size"] == "0"

    snap1_info = info[snap1]
    assert snap1_info["name"] == snap1
    assert snap1_info["parent"] == snap0
    assert snapreb in snap1_info["children"]
    assert snap1_info["removed"] is False
    assert snap1_info["usercreated"] is True
    assert snap1_info["created"] == createtime1
    assert snap1_info["size"] == "0"

    snap0_info = info[snap0]
    assert snap0_info["name"] == snap0
    assert snap0_info["parent"] == ""
    assert snap1 in snap0_info["children"]
    assert snap0_info["removed"] is False
    assert snap0_info["usercreated"] is False
    assert snap0_info["created"] == createtime0
    assert snap0_info["size"] == "0"
    assert snap0_info["labels"]["name"] == "snap0"
    assert snap0_info["labels"]["key"] == "value"


def test_replica_add_after_rebuild_failed(bin, grpc_controller_client,
                                          grpc_replica_client,
                                          grpc_replica_client2):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    grpc_replica_client.replica_open()
    grpc_replica_client.replica_snapshot(
        name='000', created=datetime.datetime.utcnow().isoformat())
    grpc_replica_client.replica_close()

    cmd = [bin, '--debug', 'add-replica', REPLICA]
    subprocess.check_call(cmd)

    volume = grpc_controller_client.volume_get()
    assert volume.replicaCount == 1

    grpc_replica_client2.replica_open()
    grpc_replica_client2.rebuilding_set(rebuilding=True)
    grpc_replica_client2.replica_close()

    cmd = [bin, '--debug', 'add-replica', REPLICA2]
    subprocess.check_call(cmd)

    volume = grpc_controller_client.volume_get()
    assert volume.replicaCount == 2

    replicas = grpc_controller_client.replica_list()
    assert len(replicas) == 2

    for r in replicas:
        assert r.mode == 'RW'


def test_replica_failure_detection(grpc_controller_client,
                                   grpc_replica_client,
                                   grpc_replica_client2):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    v = grpc_controller_client.volume_start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    # wait for initial read/write period to pass
    time.sleep(2)

    cleanup_replica(grpc_replica_client)

    detected = False
    for i in range(10):
        replicas = grpc_controller_client.replica_list()
        assert len(replicas) == 2
        for r in replicas:
            if r.address == REPLICA and r.mode == 'ERR':
                detected = True
                break
        if detected:
            break
        time.sleep(1)
    assert detected


def test_revert(grpc_controller_client,
                grpc_replica_client, grpc_replica_client2):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    v = grpc_controller_client.volume_start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    snap = grpc_controller_client.volume_snapshot(name='foo1')
    assert snap == 'foo1'

    snap2 = grpc_controller_client.volume_snapshot(name='foo2')
    assert snap2 == 'foo2'

    r1 = grpc_replica_client.replica_get()
    r2 = grpc_replica_client2.replica_get()

    assert r1.chain == ['volume-head-002.img', 'volume-snap-foo2.img',
                        'volume-snap-foo1.img']
    assert r1.chain == r2.chain

    shutdown_engine_frontend()
    grpc_controller_client.volume_revert(name='foo1')
    start_engine_frontend(FRONTEND_TGT_BLOCKDEV)
    r1 = grpc_replica_client.replica_get()
    r2 = grpc_replica_client2.replica_get()
    assert r1.chain == ['volume-head-003.img', 'volume-snap-foo1.img']
    assert r1.chain == r2.chain


def test_snapshot(bin, grpc_controller_client,
                  grpc_replica_client, grpc_replica_client2):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    v = grpc_controller_client.volume_start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    snap = grpc_controller_client.volume_snapshot(name='foo1')
    assert snap == 'foo1'

    snap2 = grpc_controller_client.volume_snapshot(name='foo2')
    assert snap2 == 'foo2'

    cmd = [bin, '--debug', 'snapshot']
    output = subprocess.check_output(cmd)

    assert output == '''ID
{}
{}
'''.format(snap2, snap)


def test_snapshot_ls(bin, grpc_controller_client,
                     grpc_replica_client, grpc_replica_client2):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    v = grpc_controller_client.volume_start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    snap = grpc_controller_client.volume_snapshot()
    assert snap != ''

    snap2 = grpc_controller_client.volume_snapshot()
    assert snap2 != ''

    cmd = [bin, '--debug', 'snapshot', 'ls']
    output = subprocess.check_output(cmd)

    assert output == '''ID
{}
{}
'''.format(snap2, snap)


def test_snapshot_info(bin, grpc_controller_client,
                       grpc_replica_client, grpc_replica_client2):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    v = grpc_controller_client.volume_start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    snap = grpc_controller_client.volume_snapshot()
    assert snap != ''

    snap2 = grpc_controller_client.volume_snapshot(
        labels={"name": "snap", "key": "value"})
    assert snap2 != ''

    cmd = [bin, '--debug', 'snapshot', 'info']
    output = subprocess.check_output(cmd)
    info = json.loads(output)

    assert len(info) == 3

    volumehead = "volume-head"

    head_info = info[volumehead]
    assert head_info["name"] == volumehead
    assert head_info["parent"] == snap2
    assert not head_info["children"]
    assert head_info["removed"] is False
    assert head_info["usercreated"] is False
    assert head_info["created"] != ""
    assert len(head_info["labels"]) == 0

    snap2_info = info[snap2]
    assert snap2_info["name"] == snap2
    assert snap2_info["parent"] == snap
    assert volumehead in snap2_info["children"]
    assert snap2_info["removed"] is False
    assert snap2_info["usercreated"] is True
    assert snap2_info["created"] != ""
    assert snap2_info["labels"]["name"] == "snap"
    assert snap2_info["labels"]["key"] == "value"

    snap_info = info[snap]
    assert snap_info["name"] == snap
    assert snap_info["parent"] == ""
    assert snap2 in snap_info["children"]
    assert snap_info["removed"] is False
    assert snap_info["usercreated"] is True
    assert snap_info["created"] != ""
    assert len(snap_info["labels"]) == 0


def test_snapshot_create(bin, grpc_controller_client,
                         grpc_replica_client,
                         grpc_replica_client2):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    v = grpc_controller_client.volume_start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    cmd = [bin, 'snapshot', 'create']
    snap0 = subprocess.check_output(cmd).strip()
    expected = grpc_replica_client.replica_get().chain[1]
    assert expected == 'volume-snap-{}.img'.format(snap0)

    cmd = [bin, 'snapshot', 'create',
           '--label', 'name=snap1', '--label', 'key=value']
    snap1 = subprocess.check_output(cmd).strip()

    cmd = [bin, '--debug', 'snapshot', 'ls']
    ls_output = subprocess.check_output(cmd)

    assert ls_output == '''ID
{}
{}
'''.format(snap1, snap0)

    cmd = [bin, 'snapshot', 'info']
    output = subprocess.check_output(cmd)
    info = json.loads(output)

    assert len(info) == 3
    assert info[snap0]["parent"] == ""
    assert info[snap0]["removed"] is False
    assert len(info[snap0]["labels"]) == 0
    assert info[snap1]["parent"] == snap0
    assert info[snap1]["removed"] is False
    assert len(info[snap1]["labels"]) == 2
    assert info[snap1]["labels"]["name"] == "snap1"
    assert info[snap1]["labels"]["key"] == "value"
    assert info[VOLUME_HEAD]["parent"] == snap1
    assert len(info[VOLUME_HEAD]["labels"]) == 0


def test_snapshot_rm(bin, grpc_controller_client,
                     grpc_replica_client, grpc_replica_client2):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    v = grpc_controller_client.volume_start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    cmd = [bin, 'snapshot', 'create']
    subprocess.check_call(cmd)
    output = subprocess.check_output(cmd).strip()

    chain = grpc_replica_client.replica_get().chain
    assert len(chain) == 3
    assert chain[0] == 'volume-head-002.img'
    assert chain[1] == 'volume-snap-{}.img'.format(output)

    cmd = [bin, 'snapshot', 'rm', output]
    subprocess.check_call(cmd)

    new_chain = grpc_replica_client.replica_get().chain
    assert len(new_chain) == 2
    assert chain[0] == new_chain[0]
    assert chain[2] == new_chain[1]


def test_snapshot_rm_empty(bin, grpc_controller_client,
                           grpc_replica_client,
                           grpc_replica_client2):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    v = grpc_controller_client.volume_start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    cmd = [bin, 'snapshot', 'create']

    # first snapshot
    output1 = subprocess.check_output(cmd).strip()
    chain = grpc_replica_client.replica_get().chain
    assert len(chain) == 2
    assert chain[0] == 'volume-head-001.img'
    assert chain[1] == 'volume-snap-{}.img'.format(output1)

    # second snapshot
    output2 = subprocess.check_output(cmd).strip()
    chain = grpc_replica_client.replica_get().chain
    assert len(chain) == 3
    assert chain[0] == 'volume-head-002.img'
    assert chain[1] == 'volume-snap-{}.img'.format(output2)
    assert chain[2] == 'volume-snap-{}.img'.format(output1)

    # remove the first snapshot(empty), it will fold second snapshot(empty)
    # to the first snapshot(empty) and rename it to second snapshot
    cmd = [bin, 'snapshot', 'rm', output1]
    subprocess.check_call(cmd)
    new_chain = grpc_replica_client.replica_get().chain
    assert len(new_chain) == 2
    assert chain[0] == new_chain[0]
    assert chain[1] == new_chain[1]


def test_snapshot_last(bin, grpc_controller_client,
                       grpc_replica_client,
                       grpc_replica_client2):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    v = grpc_controller_client.volume_start(replicas=[
        REPLICA,
    ])
    assert v.replicaCount == 1

    cmd = [bin, 'add', REPLICA2]
    subprocess.check_output(cmd)
    output = subprocess.check_output([bin, 'snapshot', 'ls'])
    output = output.splitlines()[1]

    chain = grpc_replica_client.replica_get().chain
    assert len(chain) == 2
    assert chain[0] == 'volume-head-001.img'
    assert chain[1] == 'volume-snap-{}.img'.format(output)

    chain = grpc_replica_client2.replica_get().chain
    assert len(chain) == 2
    assert chain[0] == 'volume-head-001.img'
    assert chain[1] == 'volume-snap-{}.img'.format(output)

    # it will be marked as removed
    cmd = [bin, 'snapshot', 'rm', output]
    subprocess.check_call(cmd)


def get_backup_url(bin, backupID):
    assert backupID != ""
    rValue = ""
    cmd = [bin, 'backup', 'status', backupID]
    for x in range(RETRY_COUNTS):
        output = subprocess.check_output(cmd).strip()
        backup = json.loads(output)
        if backup['progress'] == 100 and 'backupURL' in backup.keys():
            rValue = backup['backupURL']
            break
        elif 'backupError' in backup.keys():
            rValue = backup['backupError']
            break
        time.sleep(1)
    return rValue


def restore_backup(bin, backupURL, env, grpc_c):
    shutdown_engine_frontend()
    v = grpc_c.volume_get()
    assert v.frontendState == "down"

    cmd = [bin, 'backup', 'restore', backupURL]
    subprocess.check_call(cmd, env=env)

    status_cmd = [bin, 'backup', 'restore-status']
    completed = 0
    rs = json.loads(subprocess.check_output(status_cmd).strip())
    for x in range(RETRY_COUNTS):
        time.sleep(3)
        completed = 0
        rs = json.loads(subprocess.check_output(status_cmd).strip())
        for status in rs.values():
            if 'progress' in status.keys() and status['progress'] == 100:
                completed += 1
                continue
            if 'error' in status.keys():
                assert status['error'] == ""
        if completed == len(rs):
            return
    assert completed == len(rs)
    start_engine_frontend(FRONTEND_TGT_BLOCKDEV)
    v = grpc_c.volume_get()
    assert v.frontendState == "up"


def backup_core(bin, grpc_controller_client,
                grpc_replica_client,
                grpc_replica_client2,
                backup_target):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    v = grpc_controller_client.volume_start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    env = dict(os.environ)
    backup_type = urlparse(backup_target).scheme
    cmd = [bin, 'snapshot', 'create']
    snapshot1 = subprocess.check_output(cmd).strip()
    output = grpc_replica_client.replica_get().chain[1]

    assert output == 'volume-snap-{}.img'.format(snapshot1)

    cmd = [bin, 'backup', 'create', snapshot1,
           '--dest', backup_target,
           '--label', 'name=backup1',
           '--label', 'type=' + backup_type]
    backupID = subprocess.check_output(cmd, env=env).strip()
    backup1 = get_backup_url(bin, backupID)

    cmd = [bin, 'snapshot', 'create']
    snapshot2 = subprocess.check_output(cmd).strip()
    output = grpc_replica_client.replica_get().chain[1]

    assert output == 'volume-snap-{}.img'.format(snapshot2)

    cmd = [bin, 'backup', 'create', snapshot2,
           '--dest', backup_target]
    backupID = subprocess.check_output(cmd, env=env).strip()
    backup2 = get_backup_url(bin, backupID)

    cmd = [bin, 'backup', 'inspect', backup1]
    data = subprocess.check_output(cmd, env=env)
    backup1_info = json.loads(data)
    assert backup1_info["URL"] == backup1
    assert backup1_info["VolumeName"] == VOLUME_NAME
    assert backup1_info["VolumeSize"] == VOLUME_SIZE
    assert backup1_info["SnapshotName"] == snapshot1
    assert len(backup1_info["Labels"]) == 2
    assert backup1_info["Labels"]["name"] == "backup1"
    assert backup1_info["Labels"]["type"] == backup_type

    cmd = [bin, 'backup', 'inspect', backup2]
    data = subprocess.check_output(cmd, env=env)
    backup2_info = json.loads(data)
    assert backup2_info["URL"] == backup2
    assert backup2_info["VolumeName"] == VOLUME_NAME
    assert backup2_info["VolumeSize"] == VOLUME_SIZE
    assert backup2_info["SnapshotName"] == snapshot2
    if backup2_info["Labels"] is not None:
        assert len(backup2_info["Labels"]) == 0

    cmd = [bin, 'backup', 'ls', backup_target]
    data = subprocess.check_output(cmd, env=env).strip()
    volume_info = json.loads(data)[VOLUME_NAME]
    assert volume_info["Name"] == VOLUME_NAME
    assert volume_info["Size"] == VOLUME_SIZE
    backup_list = volume_info["Backups"]
    assert backup_list[backup1]["URL"] == backup1_info["URL"]
    assert backup_list[backup1]["SnapshotName"] == backup1_info["SnapshotName"]
    assert backup_list[backup1]["Size"] == backup1_info["Size"]
    assert backup_list[backup1]["Created"] == backup1_info["Created"]
    assert backup_list[backup2]["URL"] == backup2_info["URL"]
    assert backup_list[backup2]["SnapshotName"] == backup2_info["SnapshotName"]
    assert backup_list[backup2]["Size"] == backup2_info["Size"]
    assert backup_list[backup2]["Created"] == backup2_info["Created"]

    # test backup volume list
    # https://github.com/rancher/longhorn/issues/399
    volume_dir = finddir(BACKUP_DEST, VOLUME_NAME)
    assert volume_dir
    assert path.exists(volume_dir)
    volume_cfg_path = findfile(volume_dir, VOLUME_CONFIG_FILE)
    assert path.exists(volume_cfg_path)
    volume_tmp_cfg_path = volume_cfg_path.replace(
        VOLUME_CONFIG_FILE, VOLUME_TMP_CONFIG_FILE)
    os.rename(volume_cfg_path, volume_tmp_cfg_path)
    assert path.exists(volume_tmp_cfg_path)

    cmd = [bin, 'backup', 'ls',
           '--volume-only', backup_target]
    data = subprocess.check_output(cmd, env=env)
    volume_info_dict = json.loads(data)

    assert volume_info_dict
    assert volume_info_dict.get(VOLUME_NAME, None)
    assert volume_info_dict[VOLUME_NAME].get("Messages", None)
    assert MESSAGE_TYPE_ERROR in volume_info_dict[VOLUME_NAME]["Messages"]

    os.rename(volume_tmp_cfg_path, volume_cfg_path)
    assert path.exists(volume_cfg_path)

    cmd = [bin, 'backup', 'ls',
           '--volume-only', backup_target]
    data = subprocess.check_output(cmd, env=env)
    volume_info_dict = json.loads(data)

    assert volume_info_dict
    assert volume_info_dict.get(VOLUME_NAME, None)
    assert volume_info_dict[VOLUME_NAME].get("Messages", None) is not None
    assert MESSAGE_TYPE_ERROR not in volume_info_dict[VOLUME_NAME]["Messages"]

    cmd = [bin, 'backup', 'inspect',
           backup_target + "?backup=backup-1234"
           + "&volume=test-volume_1.0"]
    # cannot find the backup
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_call(cmd, env=env)

    restore_backup(bin, backup1, env, grpc_controller_client)
    restore_backup(bin, backup2, env, grpc_controller_client)

    cmd = [bin, 'backup', 'rm', backup1]
    subprocess.check_call(cmd, env=env)
    cmd = [bin, 'backup', 'rm', backup2]
    subprocess.check_call(cmd, env=env)

    assert os.path.exists(BACKUP_DEST)

    cmd = [bin, 'backup', 'inspect',
           backup_target + "?backup=backup-1234"
           + "&volume=test-volume_1.0"]
    # cannot find the backup
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_call(cmd, env=env)

    cmd = [bin, 'backup', 'inspect', "xxx"]
    # cannot find the backup
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_call(cmd, env=env)
    start_engine_frontend(FRONTEND_TGT_BLOCKDEV)
    v = grpc_controller_client.volume_get()
    assert v.frontendState == "up"


def test_snapshot_purge_basic(bin, grpc_controller_client,
                              grpc_replica_client,
                              grpc_replica_client2):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    v = grpc_controller_client.volume_start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    cmd = [bin, 'snapshot', 'create']
    snap0 = subprocess.check_output(cmd).strip()
    snap1 = subprocess.check_output(cmd).strip()

    chain = grpc_replica_client.replica_get().chain
    assert len(chain) == 3
    assert chain[0] == 'volume-head-002.img'
    assert chain[1] == 'volume-snap-{}.img'.format(snap1)
    assert chain[2] == 'volume-snap-{}.img'.format(snap0)

    cmd = [bin, 'snapshot', 'rm', snap0]
    subprocess.check_call(cmd)

    new_chain = grpc_replica_client.replica_get().chain
    assert len(new_chain) == 2
    assert chain[0] == new_chain[0]
    assert chain[1] == new_chain[1]

    cmd = [bin, 'snapshot', 'info']
    output = subprocess.check_output(cmd)
    info = json.loads(output)

    assert len(info) == 3
    assert info[snap0]["parent"] == ""
    assert info[snap0]["removed"] is True
    assert info[snap1]["parent"] == snap0
    assert info[snap1]["removed"] is False
    assert info[VOLUME_HEAD]["parent"] == snap1

    cmd = [bin, 'snapshot', 'purge']
    subprocess.check_call(cmd)

    cmd = [bin, 'snapshot', 'info']
    output = subprocess.check_output(cmd)
    info = json.loads(output)

    assert len(info) == 2
    assert snap0 not in info
    assert info[snap1]["parent"] == ""


def test_snapshot_purge_head_parent(bin, grpc_controller_client,
                                    grpc_replica_client,
                                    grpc_replica_client2):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    v = grpc_controller_client.volume_start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    cmd = [bin, 'snapshot', 'create']
    snap0 = subprocess.check_output(cmd).strip()
    snap1 = subprocess.check_output(cmd).strip()

    chain = grpc_replica_client.replica_get().chain
    assert len(chain) == 3
    assert chain[0] == 'volume-head-002.img'
    assert chain[1] == 'volume-snap-{}.img'.format(snap1)
    assert chain[2] == 'volume-snap-{}.img'.format(snap0)

    cmd = [bin, 'snapshot', 'rm', snap1]
    subprocess.check_call(cmd)

    new_chain = grpc_replica_client.replica_get().chain
    assert len(new_chain) == 2
    assert chain[0] == new_chain[0]
    assert chain[2] == new_chain[1]

    cmd = [bin, 'snapshot', 'info']
    output = subprocess.check_output(cmd)
    info = json.loads(output)

    assert len(info) == 3
    assert info[snap0]["parent"] == ""
    assert info[snap0]["removed"] is False
    assert info[snap1]["parent"] == snap0
    assert info[snap1]["removed"] is True
    assert info[VOLUME_HEAD]["parent"] == snap1

    cmd = [bin, 'snapshot', 'purge']
    subprocess.check_call(cmd)

    cmd = [bin, 'snapshot', 'info']
    output = subprocess.check_output(cmd)
    info = json.loads(output)

    # Current we're unable to purge the head's parent
    assert len(info) == 3
    assert info[snap0]["parent"] == ""
    assert info[snap0]["removed"] is False
    assert info[snap1]["parent"] == snap0
    assert info[snap1]["removed"] is True
    assert info[VOLUME_HEAD]["parent"] == snap1


def test_backup_cli(bin, grpc_controller_client,
                    grpc_replica_client, grpc_replica_client2,
                    backup_targets):
    for backup_target in backup_targets:
        backup_core(bin, grpc_controller_client,
                    grpc_replica_client, grpc_replica_client2,
                    backup_target)
        cleanup_replica(grpc_replica_client)
        cleanup_replica(grpc_replica_client2)
        cleanup_controller(grpc_controller_client)
