import time
import random
from os import path
import os
import subprocess
import json
import datetime
import pytest

from urllib.parse import urlparse

from core.common import (  # NOQA
    cleanup_replica, cleanup_controller,
    get_expansion_snapshot_name,
    get_replica_paths_from_snapshot_name,
    get_snapshot_file_paths,
    get_replica_head_file_path,
    wait_for_volume_expansion,
    wait_for_rebuild_complete,

    VOLUME_NAME, ENGINE_NAME,
    RETRY_COUNTS2, FRONTEND_TGT_BLOCKDEV,
    RETRY_INTERVAL,
    SIZE, EXPANDED_SIZE,
    SIZE_STR, EXPANDED_SIZE_STR,
)

BACKUP_DEST = '/data/backupbucket'

VOLUME_HEAD = "volume-head"

VOLUME_CONFIG_FILE = "volume.cfg"
VOLUME_TMP_CONFIG_FILE = "volume.cfg.tmp"
MESSAGE_TYPE_ERROR = "error"


@pytest.fixture()
def backup_targets():
    env = dict(os.environ)
    assert env["BACKUPTARGETS"] != ""
    return env["BACKUPTARGETS"].split(",")


@pytest.fixture
def random_str():
    return 'random-{0}-{1}'.format(random_num(), int(time.time()))


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


def getNow():
    time.sleep(1)
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat()


def open_replica(grpc_client):
    r = grpc_client.replica_get()
    assert r.state == 'initial'
    assert r.size == '0'
    assert r.sectorSize == 0
    assert r.parent == ''
    assert r.head == ''

    r = grpc_client.replica_create(size=SIZE_STR)

    assert r.state == 'closed'
    assert r.size == SIZE_STR
    assert r.sectorSize == 512
    assert r.parent == ''
    assert r.head == 'volume-head-000.img'

    return r


def test_replica_add_start(bin, grpc_controller_client,  # NOQA
                           grpc_replica_client):  # NOQA
    open_replica(grpc_replica_client)

    cmd = [bin, '--debug', '--url', grpc_controller_client.address,
           'add-replica',
           grpc_replica_client.url]
    subprocess.check_call(cmd)
    wait_for_rebuild_complete(bin, grpc_controller_client.address)

    volume = grpc_controller_client.volume_get()
    assert volume.replicaCount == 1


def test_replica_add_rebuild(bin, grpc_controller_client,  # NOQA
                             grpc_replica_client,  # NOQA
                             grpc_replica_client2):  # NOQA
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

    r2 = grpc_replica_client2.replica_get()

    assert r.chain == ['volume-head-002.img',
                       'volume-snap-001.img',
                       'volume-snap-000.img']

    assert r2.chain != ['volume-head-002.img',
                        'volume-snap-001.img',
                        'volume-snap-000.img']

    grpc_replica_client.replica_close()
    cmd = [bin, '--debug', '--url', grpc_controller_client.address,
           'add-replica',
           grpc_replica_client.url]
    subprocess.check_call(cmd)
    wait_for_rebuild_complete(bin, grpc_controller_client.address)

    volume = grpc_controller_client.volume_get()
    assert volume.replicaCount == 1

    cmd = [bin, '--debug', '--url', grpc_controller_client.address,
           'add-replica',
           grpc_replica_client2.url]
    subprocess.check_call(cmd)
    wait_for_rebuild_complete(bin, grpc_controller_client.address)

    volume = grpc_controller_client.volume_get()
    assert volume.replicaCount == 2

    replicas = grpc_controller_client.replica_list()
    assert len(replicas) == 2

    for r in replicas:
        assert r.mode == 'RW'

    cmd = [bin, '--debug', '--url', grpc_controller_client.address,
           'snapshot', 'info']
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


def test_replica_add_after_rebuild_failed(bin, grpc_controller_client,  # NOQA
                                          grpc_replica_client,  # NOQA
                                          grpc_replica_client2):  # NOQA
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    grpc_replica_client.replica_open()
    grpc_replica_client.replica_snapshot(
        name='000', created=datetime.datetime.utcnow().isoformat())
    grpc_replica_client.replica_close()

    cmd = [bin, '--debug', '--url', grpc_controller_client.address,
           'add-replica',
           grpc_replica_client.url]
    subprocess.check_call(cmd)

    volume = grpc_controller_client.volume_get()
    assert volume.replicaCount == 1

    grpc_replica_client2.replica_open()
    grpc_replica_client2.rebuilding_set(rebuilding=True)
    grpc_replica_client2.replica_close()

    cmd = [bin, '--debug', '--url', grpc_controller_client.address,
           'add-replica',
           grpc_replica_client2.url]
    subprocess.check_call(cmd)

    volume = grpc_controller_client.volume_get()
    assert volume.replicaCount == 2

    replicas = grpc_controller_client.replica_list()
    assert len(replicas) == 2

    for r in replicas:
        assert r.mode == 'RW'


def test_replica_failure_detection(grpc_controller_client,  # NOQA
                                   grpc_replica_client,  # NOQA
                                   grpc_replica_client2):  # NOQA
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(replicas=[
        r1_url,
        r2_url,
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
            if r.address == r1_url and r.mode == 'ERR':
                detected = True
                break
        if detected:
            break
        time.sleep(1)
    assert detected


def test_revert(engine_manager_client,  # NOQA
                grpc_controller_client,  # NOQA
                grpc_replica_client,  # NOQA
                grpc_replica_client2):  # NOQA
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(replicas=[
        r1_url,
        r2_url,
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

    engine_manager_client.frontend_shutdown(ENGINE_NAME)
    grpc_controller_client.volume_revert(name='foo1')
    engine_manager_client.frontend_start(ENGINE_NAME,
                                         FRONTEND_TGT_BLOCKDEV)
    r1 = grpc_replica_client.replica_get()
    r2 = grpc_replica_client2.replica_get()
    assert r1.chain == ['volume-head-003.img', 'volume-snap-foo1.img']
    assert r1.chain == r2.chain


def test_snapshot(bin, grpc_controller_client,  # NOQA
                  grpc_replica_client, grpc_replica_client2):  # NOQA
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(replicas=[
        r1_url,
        r2_url,
    ])
    assert v.replicaCount == 2

    snap = grpc_controller_client.volume_snapshot(name='foo1')
    assert snap == 'foo1'

    snap2 = grpc_controller_client.volume_snapshot(name='foo2')
    assert snap2 == 'foo2'

    cmd = [bin, '--debug', '--url', grpc_controller_client.address,
           'snapshot']
    output = subprocess.check_output(cmd, encoding='utf-8')

    assert output == '''ID
{}
{}
'''.format(snap2, snap)


def test_snapshot_ls(bin, grpc_controller_client,  # NOQA
                     grpc_replica_client, grpc_replica_client2):  # NOQA
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(replicas=[
        r1_url,
        r2_url,
    ])
    assert v.replicaCount == 2

    snap = grpc_controller_client.volume_snapshot()
    assert snap != ''

    snap2 = grpc_controller_client.volume_snapshot()
    assert snap2 != ''

    cmd = [bin, '--debug', '--url', grpc_controller_client.address,
           'snapshot', 'ls']
    output = subprocess.check_output(cmd, encoding='utf-8')

    assert output == '''ID
{}
{}
'''.format(snap2, snap)


def test_snapshot_info(bin, grpc_controller_client,  # NOQA
                       grpc_replica_client, grpc_replica_client2):  # NOQA
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(replicas=[
        r1_url,
        r2_url,
    ])
    assert v.replicaCount == 2

    snap = grpc_controller_client.volume_snapshot()
    assert snap != ''

    snap2 = grpc_controller_client.volume_snapshot(
        labels={"name": "snap", "key": "value"})
    assert snap2 != ''

    cmd = [bin, '--debug', '--url', grpc_controller_client.address,
           'snapshot', 'info']
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


def test_snapshot_create(bin, grpc_controller_client,  # NOQA
                         grpc_replica_client,  # NOQA
                         grpc_replica_client2):  # NOQA
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(replicas=[
        r1_url,
        r2_url,
    ])
    assert v.replicaCount == 2

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create']
    snap0 = subprocess.check_output(cmd, encoding='utf-8').strip()
    expected = grpc_replica_client.replica_get().chain[1]
    assert expected == 'volume-snap-{}.img'.format(snap0)

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create',
           '--label', 'name=snap1', '--label', 'key=value']
    snap1 = subprocess.check_output(cmd, encoding='utf-8').strip()

    cmd = [bin, '--debug',
           '--url', grpc_controller_client.address,
           'snapshot', 'ls']
    ls_output = subprocess.check_output(cmd, encoding='utf-8')

    assert ls_output == '''ID
{}
{}
'''.format(snap1, snap0)

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'info']
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


def test_snapshot_rm(bin, grpc_controller_client,  # NOQA
                     grpc_replica_client, grpc_replica_client2):  # NOQA
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(replicas=[
        r1_url,
        r2_url,
    ])
    assert v.replicaCount == 2

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create']
    subprocess.check_call(cmd)
    output = subprocess.check_output(cmd, encoding='utf-8').strip()

    chain = grpc_replica_client.replica_get().chain
    assert len(chain) == 3
    assert chain[0] == 'volume-head-002.img'
    assert chain[1] == 'volume-snap-{}.img'.format(output)

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'rm', output]
    subprocess.check_call(cmd)

    new_chain = grpc_replica_client.replica_get().chain
    assert len(new_chain) == 2
    assert chain[0] == new_chain[0]
    assert chain[2] == new_chain[1]


def test_snapshot_rm_empty(bin, grpc_controller_client,  # NOQA
                           grpc_replica_client,  # NOQA
                           grpc_replica_client2):  # NOQA
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(replicas=[
        r1_url,
        r2_url,
    ])
    assert v.replicaCount == 2

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create']

    # first snapshot
    output1 = subprocess.check_output(cmd, encoding='utf-8').strip()
    chain = grpc_replica_client.replica_get().chain
    assert len(chain) == 2
    assert chain[0] == 'volume-head-001.img'
    assert chain[1] == 'volume-snap-{}.img'.format(output1)

    # second snapshot
    output2 = subprocess.check_output(cmd, encoding='utf-8').strip()
    chain = grpc_replica_client.replica_get().chain
    assert len(chain) == 3
    assert chain[0] == 'volume-head-002.img'
    assert chain[1] == 'volume-snap-{}.img'.format(output2)
    assert chain[2] == 'volume-snap-{}.img'.format(output1)

    # remove the first snapshot(empty), it will fold second snapshot(empty)
    # to the first snapshot(empty) and rename it to second snapshot
    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'rm', output1]
    subprocess.check_call(cmd)
    new_chain = grpc_replica_client.replica_get().chain
    assert len(new_chain) == 2
    assert chain[0] == new_chain[0]
    assert chain[1] == new_chain[1]


def test_snapshot_last(bin, grpc_controller_client,  # NOQA
                       grpc_replica_client,  # NOQA
                       grpc_replica_client2):  # NOQA
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(replicas=[
        r1_url,
    ])
    assert v.replicaCount == 1

    cmd = [bin, '--url', grpc_controller_client.address,
           'add', r2_url]
    subprocess.check_output(cmd)
    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'ls']
    output = subprocess.check_output(cmd, encoding='utf-8')
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
    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'rm', output]
    subprocess.check_call(cmd)


def get_backup_url(bin, url, backupID):
    assert backupID != ""
    rValue = ""
    cmd = [bin, '--url', url, 'backup', 'status', backupID]
    for x in range(RETRY_COUNTS2):
        output = subprocess.check_output(cmd, encoding='utf-8').strip()
        backup = json.loads(output)
        assert 'state' in backup.keys()
        if backup['state'] == "complete":
            assert 'backupURL' in backup.keys()
            assert backup['backupURL'] != ""
            assert 'progress' in backup.keys()
            assert backup['progress'] == 100
            rValue = backup['backupURL']
            break
        elif backup['state'] == "error":
            assert 'error' in backup.keys()
            assert backup['error'] != ""
            rValue = backup['error']
            break
        else:
            assert backup['state'] == "in_progress"
        time.sleep(1)
    return rValue


def restore_backup(engine_manager_client,  # NOQA
                   bin, url, backup_url, env, grpc_c):
    engine_manager_client.frontend_shutdown(ENGINE_NAME)
    v = grpc_c.volume_get()
    assert v.frontendState == "down"

    cmd = [bin, '--url', url, 'backup', 'restore', backup_url]
    subprocess.check_call(cmd, env=env)

    status_cmd = [bin, '--url', url, 'backup', 'restore-status']
    completed = 0
    rs = json.loads(subprocess.check_output(status_cmd, encoding='utf-8').
                    strip())
    for x in range(RETRY_COUNTS2):
        completed = 0
        rs = json.loads(subprocess.check_output(status_cmd, encoding='utf-8').
                        strip())
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
    engine_manager_client.frontend_start(ENGINE_NAME,
                                         FRONTEND_TGT_BLOCKDEV)
    v = grpc_c.volume_get()
    assert v.frontendState == "up"


def backup_core(bin, engine_manager_client,  # NOQA
                grpc_controller_client,  # NOQA
                grpc_replica_client,  # NOQA
                grpc_replica_client2,  # NOQA
                backup_target):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(replicas=[
        r1_url,
        r2_url,
    ])
    assert v.replicaCount == 2

    env = dict(os.environ)
    backup_type = urlparse(backup_target).scheme
    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create']
    snapshot1 = subprocess.check_output(cmd, encoding='utf-8').strip()
    output = grpc_replica_client.replica_get().chain[1]

    assert output == 'volume-snap-{}.img'.format(snapshot1)

    cmd = [bin, '--url', grpc_controller_client.address,
           'backup', 'create', snapshot1,
           '--dest', backup_target,
           '--label', 'name=backup1',
           '--label', 'type=' + backup_type]
    backup = json.loads(subprocess.
                        check_output(cmd, env=env, encoding='utf-8').strip())
    assert "backupID" in backup.keys()
    assert "isIncremental" in backup.keys()
    assert backup["isIncremental"] is False
    backup1 = get_backup_url(bin, grpc_controller_client.address,
                             backup["backupID"])

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create']
    snapshot2 = subprocess.check_output(cmd, encoding='utf-8').strip()
    output = grpc_replica_client.replica_get().chain[1]

    assert output == 'volume-snap-{}.img'.format(snapshot2)

    cmd = [bin, '--url', grpc_controller_client.address,
           'backup', 'create', snapshot2,
           '--dest', backup_target]
    backup = json.loads(subprocess.
                        check_output(cmd, env=env, encoding='utf-8').strip())
    assert "backupID" in backup.keys()
    assert "isIncremental" in backup.keys()
    assert backup["isIncremental"] is True
    backup2 = get_backup_url(bin, grpc_controller_client.address,
                             backup["backupID"])

    cmd = [bin, '--url', grpc_controller_client.address,
           'backup', 'inspect', backup1]
    data = subprocess.check_output(cmd, env=env)
    backup1_info = json.loads(data)
    assert backup1_info["URL"] == backup1
    assert backup1_info["VolumeName"] == VOLUME_NAME
    assert backup1_info["VolumeSize"] == SIZE_STR
    assert backup1_info["SnapshotName"] == snapshot1
    assert len(backup1_info["Labels"]) == 2
    assert backup1_info["Labels"]["name"] == "backup1"
    assert backup1_info["Labels"]["type"] == backup_type

    cmd = [bin, '--url', grpc_controller_client.address,
           'backup', 'inspect', backup2]
    data = subprocess.check_output(cmd, env=env)
    backup2_info = json.loads(data)
    assert backup2_info["URL"] == backup2
    assert backup2_info["VolumeName"] == VOLUME_NAME
    assert backup2_info["VolumeSize"] == SIZE_STR
    assert backup2_info["SnapshotName"] == snapshot2
    if backup2_info["Labels"] is not None:
        assert len(backup2_info["Labels"]) == 0

    cmd = [bin, '--url', grpc_controller_client.address,
           'backup', 'ls', backup_target]
    data = subprocess.check_output(cmd, env=env, encoding='utf-8').strip()
    volume_info = json.loads(data)[VOLUME_NAME]
    assert volume_info["Name"] == VOLUME_NAME
    assert volume_info["Size"] == SIZE_STR
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

    cmd = [bin, '--url', grpc_controller_client.address,
           'backup', 'ls', '--volume-only', backup_target]
    data = subprocess.check_output(cmd, env=env)
    volume_info = json.loads(data)

    assert volume_info
    assert volume_info[VOLUME_NAME] is not None
    assert volume_info[VOLUME_NAME]["Messages"] is not None
    assert MESSAGE_TYPE_ERROR in volume_info[VOLUME_NAME]["Messages"]

    os.rename(volume_tmp_cfg_path, volume_cfg_path)
    assert path.exists(volume_cfg_path)

    cmd = [bin, '--url', grpc_controller_client.address,
           'backup', 'ls', '--volume-only', backup_target]
    data = subprocess.check_output(cmd, env=env)
    volume_info = json.loads(data)

    assert volume_info
    assert volume_info[VOLUME_NAME] is not None
    assert volume_info[VOLUME_NAME]["Messages"] is not None
    assert MESSAGE_TYPE_ERROR not in volume_info[VOLUME_NAME]["Messages"]

    cmd = [bin, '--url', grpc_controller_client.address,
           'backup', 'inspect',
           backup_target + "?backup=backup-1234"
           + "&volume=" + VOLUME_NAME]
    # cannot find the backup
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_call(cmd, env=env)

    restore_backup(engine_manager_client,
                   bin, grpc_controller_client.address,
                   backup1, env, grpc_controller_client)
    restore_backup(engine_manager_client,
                   bin, grpc_controller_client.address,
                   backup2, env, grpc_controller_client)

    cmd = [bin, '--url', grpc_controller_client.address,
           'backup', 'rm', backup1]
    subprocess.check_call(cmd, env=env)
    cmd = [bin, '--url', grpc_controller_client.address,
           'backup', 'rm', backup2]
    subprocess.check_call(cmd, env=env)

    assert os.path.exists(BACKUP_DEST)

    cmd = [bin, '--url', grpc_controller_client.address,
           'backup', 'inspect',
           backup_target + "?backup=backup-1234"
           + "&volume=" + VOLUME_NAME]
    # cannot find the backup
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_call(cmd, env=env)

    cmd = [bin, '--url', grpc_controller_client.address,
           'backup', 'inspect', "xxx"]
    # cannot find the backup
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_call(cmd, env=env)
    engine_manager_client.frontend_start(ENGINE_NAME,
                                         FRONTEND_TGT_BLOCKDEV)
    v = grpc_controller_client.volume_get()
    assert v.frontendState == "up"


def test_snapshot_purge_basic(bin, grpc_controller_client,  # NOQA
                              grpc_replica_client,  # NOQA
                              grpc_replica_client2):  # NOQA
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(replicas=[
        r1_url,
        r2_url,
    ])
    assert v.replicaCount == 2

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create']
    snap0 = subprocess.check_output(cmd, encoding='utf-8').strip()
    snap1 = subprocess.check_output(cmd, encoding='utf-8').strip()

    chain = grpc_replica_client.replica_get().chain
    assert len(chain) == 3
    assert chain[0] == 'volume-head-002.img'
    assert chain[1] == 'volume-snap-{}.img'.format(snap1)
    assert chain[2] == 'volume-snap-{}.img'.format(snap0)

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'rm', snap0]
    subprocess.check_call(cmd)

    new_chain = grpc_replica_client.replica_get().chain
    assert len(new_chain) == 2
    assert chain[0] == new_chain[0]
    assert chain[1] == new_chain[1]

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'info']
    output = subprocess.check_output(cmd)
    info = json.loads(output)

    assert len(info) == 3
    assert info[snap0]["parent"] == ""
    assert info[snap0]["removed"] is True
    assert info[snap1]["parent"] == snap0
    assert info[snap1]["removed"] is False
    assert info[VOLUME_HEAD]["parent"] == snap1

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'purge']
    subprocess.check_call(cmd)

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'info']
    output = subprocess.check_output(cmd)
    info = json.loads(output)

    assert len(info) == 2
    assert snap0 not in info
    assert info[snap1]["parent"] == ""


def test_snapshot_purge_head_parent(bin, grpc_controller_client,  # NOQA
                                    grpc_replica_client,  # NOQA
                                    grpc_replica_client2):  # NOQA
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(replicas=[
        r1_url,
        r2_url,
    ])
    assert v.replicaCount == 2

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create']
    snap0 = subprocess.check_output(cmd, encoding='utf-8').strip()
    snap1 = subprocess.check_output(cmd, encoding='utf-8').strip()

    chain = grpc_replica_client.replica_get().chain
    assert len(chain) == 3
    assert chain[0] == 'volume-head-002.img'
    assert chain[1] == 'volume-snap-{}.img'.format(snap1)
    assert chain[2] == 'volume-snap-{}.img'.format(snap0)

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'rm', snap1]
    subprocess.check_call(cmd)

    new_chain = grpc_replica_client.replica_get().chain
    assert len(new_chain) == 2
    assert chain[0] == new_chain[0]
    assert chain[2] == new_chain[1]

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'info']
    output = subprocess.check_output(cmd)
    info = json.loads(output)

    assert len(info) == 3
    assert info[snap0]["parent"] == ""
    assert info[snap0]["removed"] is False
    assert info[snap1]["parent"] == snap0
    assert info[snap1]["removed"] is True
    assert info[VOLUME_HEAD]["parent"] == snap1

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'purge']
    subprocess.check_call(cmd)

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'info']
    output = subprocess.check_output(cmd)
    info = json.loads(output)

    # Current we're unable to purge the head's parent
    assert len(info) == 3
    assert info[snap0]["parent"] == ""
    assert info[snap0]["removed"] is False
    assert info[snap1]["parent"] == snap0
    assert info[snap1]["removed"] is True
    assert info[VOLUME_HEAD]["parent"] == snap1


def test_backup_cli(bin, engine_manager_client,  # NOQA
                    grpc_controller_client,  # NOQA
                    grpc_replica_client, grpc_replica_client2,  # NOQA
                    backup_targets):
    for backup_target in backup_targets:
        backup_core(bin, engine_manager_client,
                    grpc_controller_client,
                    grpc_replica_client, grpc_replica_client2,
                    backup_target)
        cleanup_replica(grpc_replica_client)
        cleanup_replica(grpc_replica_client2)
        cleanup_controller(grpc_controller_client)


def test_volume_expand_with_snapshots(  # NOQA
        bin, grpc_controller_client,  # NOQA
        grpc_replica_client, grpc_replica_client2):  # NOQA
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(replicas=[
        r1_url,
        r2_url,
    ])
    assert v.replicaCount == 2

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create']
    snap0 = subprocess.check_output(cmd, encoding='utf-8').strip()
    expected = grpc_replica_client.replica_get().chain[1]
    assert expected == 'volume-snap-{}.img'.format(snap0)

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create',
           '--label', 'name=snap1', '--label', 'key=value']
    snap1 = subprocess.check_output(cmd, encoding='utf-8').strip()

    cmd = [bin, '--url', grpc_controller_client.address,
           'expand', '--size', EXPANDED_SIZE_STR]
    subprocess.check_call(cmd)

    wait_for_volume_expansion(grpc_controller_client, EXPANDED_SIZE)

    # `expand` will create a snapshot then apply the new size
    # on the new head file
    snap_expansion = get_expansion_snapshot_name()
    r1 = grpc_replica_client.replica_get()
    assert r1.chain[1] == 'volume-snap-{}.img'.format(snap_expansion)
    assert r1.size == EXPANDED_SIZE_STR
    r2 = grpc_replica_client2.replica_get()
    assert r2.chain[1] == 'volume-snap-{}.img'.format(snap_expansion)
    assert r2.size == EXPANDED_SIZE_STR

    replica_paths = get_replica_paths_from_snapshot_name(snap_expansion)
    assert replica_paths
    for p in replica_paths:
        snap_path = get_snapshot_file_paths(
            p, snap_expansion)
        assert snap_path is not None
        assert os.path.exists(snap_path)
        assert os.path.getsize(snap_path) == SIZE
        head_path = get_replica_head_file_path(p)
        assert head_path is not None
        assert os.path.exists(head_path)
        assert os.path.getsize(head_path) == EXPANDED_SIZE

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create',
           '--label', 'name=snap2']
    snap2 = subprocess.check_output(cmd, encoding='utf-8').strip()

    cmd = [bin, '--debug',
           '--url', grpc_controller_client.address,
           'snapshot', 'ls']
    ls_output = subprocess.check_output(cmd, encoding='utf-8')

    assert ls_output == '''ID
{}
{}
{}
{}
'''.format(snap2,
           snap_expansion,
           snap1,
           snap0)

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'info']
    output = subprocess.check_output(cmd)
    info = json.loads(output)

    # cannot check the snapshot size here since the output will return
    # the actual file size
    assert info[snap_expansion]["parent"] == snap1
    assert info[snap_expansion]["removed"] is False
    assert info[snap_expansion]["usercreated"] is False
    assert len(info[snap_expansion]["labels"]) == 1
    assert \
        info[snap_expansion]["labels"]["replica-expansion"] \
        == EXPANDED_SIZE_STR
    assert info[VOLUME_HEAD]["parent"] == snap2
    assert len(info[VOLUME_HEAD]["labels"]) == 0

    # snapshot purge command will coalesce the expansion snapshot
    # with its child snapshot `snap2`
    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'purge']
    subprocess.check_call(cmd)

    cmd = [bin, '--debug',
           '--url', grpc_controller_client.address,
           'snapshot', 'ls']
    ls_output = subprocess.check_output(cmd, encoding='utf-8')

    assert ls_output == '''ID
{}
{}
{}
'''.format(snap2,
           snap1,
           snap0)

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'info']
    output = subprocess.check_output(cmd)
    info = json.loads(output)
    assert snap_expansion not in info
    assert info[snap2]["parent"] == snap1
    assert info[snap2]["removed"] is False
    assert info[snap2]["usercreated"] is True

    for p in replica_paths:
        snap1_path = get_snapshot_file_paths(
            p, snap1)
        assert snap1_path is not None
        assert os.path.exists(snap1_path)
        assert os.path.getsize(snap1_path) == SIZE
        snap2_path = get_snapshot_file_paths(
            p, snap2)
        assert snap2_path is not None
        assert os.path.exists(snap2_path)
        assert os.path.getsize(snap2_path) == EXPANDED_SIZE

    # Make sure the smaller snapshot `snap1` can be folded to
    # the larger one `snap2` and the replica size won't change.
    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'rm', snap1]
    subprocess.check_call(cmd)
    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'purge']
    subprocess.check_call(cmd)

    cmd = [bin, '--debug',
           '--url', grpc_controller_client.address,
           'snapshot', 'ls']
    ls_output = subprocess.check_output(cmd, encoding='utf-8')

    assert ls_output == '''ID
{}
{}
'''.format(snap2, snap0)

    for p in replica_paths:
        snap0_path = get_snapshot_file_paths(
            p, snap0)
        assert snap0_path is not None
        assert os.path.exists(snap0_path)
        assert os.path.getsize(snap0_path) == SIZE
        snap2_path = get_snapshot_file_paths(
            p, snap2)
        assert snap2_path is not None
        assert os.path.exists(snap2_path)
        assert os.path.getsize(snap2_path) == EXPANDED_SIZE
        head_path = get_replica_head_file_path(p)
        assert head_path is not None
        assert os.path.exists(head_path)
        assert os.path.getsize(head_path) == EXPANDED_SIZE

    r1 = grpc_replica_client.replica_get()
    assert r1.chain[1] == 'volume-snap-{}.img'.format(snap2)
    assert r1.size == EXPANDED_SIZE_STR
    r2 = grpc_replica_client2.replica_get()
    assert r2.chain[1] == 'volume-snap-{}.img'.format(snap2)
    assert r2.size == EXPANDED_SIZE_STR
