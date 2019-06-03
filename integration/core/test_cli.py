import time
import random
import sys
from os import path
import os
import subprocess
import json
import datetime

import pytest
import cattle

from urlparse import urlparse

# include directory intergration/rpc for module import
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.split(__file__)[0], "../rpc")
    )
)
from replica.replica_client import ReplicaClient  # NOQA


REPLICA = 'tcp://localhost:9502'
REPLICA2 = 'tcp://localhost:9512'

GRPC_REPLICA = 'localhost:9505'
GRPC_REPLICA2 = 'localhost:9515'

BACKUP_DEST = '/data/backupbucket'

VOLUME_NAME = 'test-volume_1.0'
VOLUME_SIZE = str(4 * 1024 * 1024)  # 4M

VOLUME_HEAD = "volume-head"

VOLUME_CONFIG_FILE = "volume.cfg"
VOLUME_TMP_CONFIG_FILE = "volume.cfg.tmp"
MESSAGE_TYPE_ERROR = "error"


@pytest.fixture
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


@pytest.fixture
def replica_client(request):
    url = 'http://localhost:9502/v1/schemas'
    c = cattle.from_env(url=url)
    grpc_c = ReplicaClient(GRPC_REPLICA)
    request.addfinalizer(lambda: cleanup_replica(c, grpc_c))
    return cleanup_replica(c, grpc_c)


@pytest.fixture
def replica_client2(request):
    url = 'http://localhost:9512/v1/schemas'
    c = cattle.from_env(url=url)
    grpc_c = ReplicaClient(GRPC_REPLICA2)
    request.addfinalizer(lambda: cleanup_replica(c, grpc_c))
    return cleanup_replica(c, grpc_c)


@pytest.fixture
def grpc_replica_client():
    return ReplicaClient(GRPC_REPLICA)


@pytest.fixture
def grpc_replica_client2():
    return ReplicaClient(GRPC_REPLICA2)


def cleanup_replica(client, grpc_client):
    r = client.list_replica()[0]
    if r.state == 'initial':
        return client
    if 'open' in r:
        grpc_client.replica_open()
    r = client.list_replica()[0]
    client.delete(r)
    r = client.reload(r)
    assert r.state == 'initial'
    return client


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


def open_replica(client, grpc_client):
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


def test_replica_add_start(bin, controller_client,
                           replica_client, grpc_replica_client):
    open_replica(replica_client, grpc_replica_client)

    cmd = [bin, '--debug', 'add-replica', REPLICA]
    subprocess.check_call(cmd)

    volume = controller_client.list_volume()[0]
    assert volume.replicaCount == 1


def getNow():
    time.sleep(1)
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat()


def test_replica_add_rebuild(bin, controller_client,
                             replica_client, replica_client2,
                             grpc_replica_client, grpc_replica_client2):
    open_replica(replica_client, grpc_replica_client)
    open_replica(replica_client2, grpc_replica_client2)

    snap0 = "000"
    snap1 = "001"
    grpc_replica_client.replica_open()
    r = replica_client.list_replica()[0]
    createtime0 = getNow()
    r = r.snapshot(name=snap0, created=createtime0,
                   labels={"name": "snap0", "key": "value"})
    createtime1 = getNow()
    r = r.snapshot(name=snap1, usercreated=True, created=createtime1)

    l = replica_client2.list_replica()[0]

    assert r.chain == ['volume-head-002.img',
                       'volume-snap-001.img',
                       'volume-snap-000.img']

    assert l.chain != ['volume-head-002.img',
                       'volume-snap-001.img',
                       'volume-snap-000.img']

    grpc_replica_client.replica_close()
    cmd = [bin, '--debug', 'add-replica', REPLICA]
    subprocess.check_call(cmd)

    volume = controller_client.list_volume()[0]
    assert volume.replicaCount == 1

    cmd = [bin, '--debug', 'add-replica', REPLICA2]
    subprocess.check_call(cmd)

    volume = controller_client.list_volume()[0]
    assert volume.replicaCount == 2

    replicas = controller_client.list_replica()
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


def test_replica_add_after_rebuild_failed(bin, controller_client,
                                          replica_client, replica_client2,
                                          grpc_replica_client,
                                          grpc_replica_client2):
    open_replica(replica_client, grpc_replica_client)
    open_replica(replica_client2, grpc_replica_client2)

    grpc_replica_client.replica_open()
    r = replica_client.list_replica()[0]
    r.snapshot(name='000', created=datetime.datetime.utcnow().isoformat())
    grpc_replica_client.replica_close()

    cmd = [bin, '--debug', 'add-replica', REPLICA]
    subprocess.check_call(cmd)

    volume = controller_client.list_volume()[0]
    assert volume.replicaCount == 1

    grpc_replica_client2.replica_open()
    l = replica_client2.list_replica()[0]
    l = l.setrebuilding(rebuilding=True)
    grpc_replica_client2.replica_close()

    cmd = [bin, '--debug', 'add-replica', REPLICA2]
    subprocess.check_call(cmd)

    volume = controller_client.list_volume()[0]
    assert volume.replicaCount == 2

    replicas = controller_client.list_replica()
    assert len(replicas) == 2

    for r in replicas:
        assert r.mode == 'RW'


def test_replica_failure_detection(bin, controller_client,
                                   replica_client, replica_client2,
                                   grpc_replica_client,
                                   grpc_replica_client2):
    open_replica(replica_client, grpc_replica_client)
    open_replica(replica_client2, grpc_replica_client2)

    v = controller_client.list_volume()[0]
    v = v.start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    # wait for initial read/write period to pass
    time.sleep(2)

    cleanup_replica(replica_client, grpc_replica_client)

    detected = False
    for i in range(10):
        replicas = controller_client.list_replica()
        assert len(replicas) == 2
        for r in replicas:
            if r.address == REPLICA and r.mode == 'ERR':
                detected = True
                break
        if detected:
            break
        time.sleep(1)
    assert detected


def test_revert(bin, controller_client, replica_client, replica_client2,
                grpc_replica_client, grpc_replica_client2):
    open_replica(replica_client, grpc_replica_client)
    open_replica(replica_client2, grpc_replica_client2)

    v = controller_client.list_volume()[0]
    v = v.start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    snap = v.snapshot(name='foo1')
    assert snap.id == 'foo1'

    snap2 = v.snapshot(name='foo2')
    assert snap2.id == 'foo2'

    r1 = replica_client.list_replica()[0]
    r2 = replica_client2.list_replica()[0]

    assert r1.chain == ['volume-head-002.img', 'volume-snap-foo2.img',
                        'volume-snap-foo1.img']
    assert r1.chain == r2.chain

    v.revert(name='foo1')

    r1 = replica_client.list_replica()[0]
    r2 = replica_client2.list_replica()[0]
    assert r1.chain == ['volume-head-003.img', 'volume-snap-foo1.img']
    assert r1.chain == r2.chain


def test_snapshot(bin, controller_client, replica_client, replica_client2,
                  grpc_replica_client, grpc_replica_client2):
    open_replica(replica_client, grpc_replica_client)
    open_replica(replica_client2, grpc_replica_client2)

    v = controller_client.list_volume()[0]
    v = v.start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    snap = v.snapshot(name='foo1')
    assert snap.id == 'foo1'

    snap2 = v.snapshot(name='foo2')
    assert snap2.id == 'foo2'

    cmd = [bin, '--debug', 'snapshot']
    output = subprocess.check_output(cmd)

    assert output == '''ID
{}
{}
'''.format(snap2.id, snap.id)


def test_snapshot_ls(bin, controller_client, replica_client, replica_client2,
                     grpc_replica_client, grpc_replica_client2):
    open_replica(replica_client, grpc_replica_client)
    open_replica(replica_client2, grpc_replica_client2)

    v = controller_client.list_volume()[0]
    v = v.start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    snap = v.snapshot()
    assert snap.id != ''

    snap2 = v.snapshot()
    assert snap2.id != ''

    cmd = [bin, '--debug', 'snapshot', 'ls']
    output = subprocess.check_output(cmd)

    assert output == '''ID
{}
{}
'''.format(snap2.id, snap.id)


def test_snapshot_info(bin, controller_client,
                       replica_client, replica_client2,
                       grpc_replica_client, grpc_replica_client2):
    open_replica(replica_client, grpc_replica_client)
    open_replica(replica_client2, grpc_replica_client2)

    v = controller_client.list_volume()[0]
    v = v.start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    snap = v.snapshot()
    assert snap.id != ''

    snap2 = v.snapshot(labels={"name": "snap", "key": "value"})
    assert snap2.id != ''

    cmd = [bin, '--debug', 'snapshot', 'info']
    output = subprocess.check_output(cmd)
    info = json.loads(output)

    assert len(info) == 3

    volumehead = "volume-head"

    head_info = info[volumehead]
    assert head_info["name"] == volumehead
    assert head_info["parent"] == snap2.id
    assert not head_info["children"]
    assert head_info["removed"] is False
    assert head_info["usercreated"] is False
    assert head_info["created"] != ""
    assert len(head_info["labels"]) == 0

    snap2_info = info[snap2.id]
    assert snap2_info["name"] == snap2.id
    assert snap2_info["parent"] == snap.id
    assert volumehead in snap2_info["children"]
    assert snap2_info["removed"] is False
    assert snap2_info["usercreated"] is True
    assert snap2_info["created"] != ""
    assert snap2_info["labels"]["name"] == "snap"
    assert snap2_info["labels"]["key"] == "value"

    snap_info = info[snap.id]
    assert snap_info["name"] == snap.id
    assert snap_info["parent"] == ""
    assert snap2.id in snap_info["children"]
    assert snap_info["removed"] is False
    assert snap_info["usercreated"] is True
    assert snap_info["created"] != ""
    assert len(snap_info["labels"]) == 0


def test_snapshot_create(bin, controller_client,
                         replica_client, replica_client2,
                         grpc_replica_client,
                         grpc_replica_client2):
    open_replica(replica_client, grpc_replica_client)
    open_replica(replica_client2, grpc_replica_client2)

    v = controller_client.list_volume()[0]
    v = v.start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    cmd = [bin, 'snapshot', 'create']
    snap0 = subprocess.check_output(cmd).strip()
    expected = replica_client.list_replica()[0].chain[1]
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


def test_snapshot_rm(bin, controller_client, replica_client, replica_client2,
                     grpc_replica_client, grpc_replica_client2):
    open_replica(replica_client, grpc_replica_client)
    open_replica(replica_client2, grpc_replica_client2)

    v = controller_client.list_volume()[0]
    v = v.start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    cmd = [bin, 'snapshot', 'create']
    subprocess.check_call(cmd)
    output = subprocess.check_output(cmd).strip()

    chain = replica_client.list_replica()[0].chain
    assert len(chain) == 3
    assert chain[0] == 'volume-head-002.img'
    assert chain[1] == 'volume-snap-{}.img'.format(output)

    cmd = [bin, 'snapshot', 'rm', output]
    subprocess.check_call(cmd)

    new_chain = replica_client.list_replica()[0].chain
    assert len(new_chain) == 2
    assert chain[0] == new_chain[0]
    assert chain[2] == new_chain[1]


def test_snapshot_rm_empty(
        bin, controller_client, replica_client, replica_client2,
        grpc_replica_client, grpc_replica_client2):
    open_replica(replica_client, grpc_replica_client)
    open_replica(replica_client2, grpc_replica_client2)

    v = controller_client.list_volume()[0]
    v = v.start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    cmd = [bin, 'snapshot', 'create']

    # first snapshot
    output1 = subprocess.check_output(cmd).strip()
    chain = replica_client.list_replica()[0].chain
    assert len(chain) == 2
    assert chain[0] == 'volume-head-001.img'
    assert chain[1] == 'volume-snap-{}.img'.format(output1)

    # second snapshot
    output2 = subprocess.check_output(cmd).strip()
    chain = replica_client.list_replica()[0].chain
    assert len(chain) == 3
    assert chain[0] == 'volume-head-002.img'
    assert chain[1] == 'volume-snap-{}.img'.format(output2)
    assert chain[2] == 'volume-snap-{}.img'.format(output1)

    # remove the first snapshot(empty), it will fold second snapshot(empty)
    # to the first snapshot(empty) and rename it to second snapshot
    cmd = [bin, 'snapshot', 'rm', output1]
    subprocess.check_call(cmd)
    new_chain = replica_client.list_replica()[0].chain
    assert len(new_chain) == 2
    assert chain[0] == new_chain[0]
    assert chain[1] == new_chain[1]


def test_snapshot_last(bin, controller_client,
                       replica_client, replica_client2,
                       grpc_replica_client,
                       grpc_replica_client2):
    open_replica(replica_client, grpc_replica_client)
    open_replica(replica_client2, grpc_replica_client2)

    v = controller_client.list_volume()[0]
    v = v.start(replicas=[
        REPLICA,
    ])
    assert v.replicaCount == 1

    cmd = [bin, 'add', REPLICA2]
    subprocess.check_output(cmd)
    output = subprocess.check_output([bin, 'snapshot', 'ls'])
    output = output.splitlines()[1]

    chain = replica_client.list_replica()[0].chain
    assert len(chain) == 2
    assert chain[0] == 'volume-head-001.img'
    assert chain[1] == 'volume-snap-{}.img'.format(output)

    chain = replica_client2.list_replica()[0].chain
    assert len(chain) == 2
    assert chain[0] == 'volume-head-001.img'
    assert chain[1] == 'volume-snap-{}.img'.format(output)

    # it will be marked as removed
    cmd = [bin, 'snapshot', 'rm', output]
    subprocess.check_call(cmd)


def backup_core(bin, controller_client,
                replica_client, replica_client2,
                grpc_replica_client,
                grpc_replica_client2,
                backup_target):
    open_replica(replica_client, grpc_replica_client)
    open_replica(replica_client2, grpc_replica_client2)

    v = controller_client.list_volume()[0]
    v = v.start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    env = dict(os.environ)
    backup_type = urlparse(backup_target).scheme
    cmd = [bin, 'snapshot', 'create']
    snapshot1 = subprocess.check_output(cmd).strip()
    output = replica_client.list_replica()[0].chain[1]

    assert output == 'volume-snap-{}.img'.format(snapshot1)

    cmd = [bin, 'backup', 'create', snapshot1,
           '--dest', backup_target,
           '--label', 'name=backup1',
           '--label', 'type=' + backup_type]
    backup1 = subprocess.check_output(cmd, env=env).strip()

    cmd = [bin, 'snapshot', 'create']
    snapshot2 = subprocess.check_output(cmd).strip()
    output = replica_client.list_replica()[0].chain[1]

    assert output == 'volume-snap-{}.img'.format(snapshot2)

    cmd = [bin, 'backup', 'create', snapshot2,
           '--dest', backup_target]
    backup2 = subprocess.check_output(cmd, env=env).strip()

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

    cmd = [bin, 'backup', 'restore', backup1]
    subprocess.check_call(cmd, env=env)

    cmd = [bin, 'backup', 'restore', backup2]
    subprocess.check_call(cmd, env=env)

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


def test_snapshot_purge_basic(bin, controller_client,
                              replica_client, replica_client2,
                              grpc_replica_client,
                              grpc_replica_client2):
    open_replica(replica_client, grpc_replica_client)
    open_replica(replica_client2, grpc_replica_client2)

    v = controller_client.list_volume()[0]
    v = v.start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    cmd = [bin, 'snapshot', 'create']
    snap0 = subprocess.check_output(cmd).strip()
    snap1 = subprocess.check_output(cmd).strip()

    chain = replica_client.list_replica()[0].chain
    assert len(chain) == 3
    assert chain[0] == 'volume-head-002.img'
    assert chain[1] == 'volume-snap-{}.img'.format(snap1)
    assert chain[2] == 'volume-snap-{}.img'.format(snap0)

    cmd = [bin, 'snapshot', 'rm', snap0]
    subprocess.check_call(cmd)

    new_chain = replica_client.list_replica()[0].chain
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


def test_snapshot_purge_head_parent(bin, controller_client,
                                    replica_client, replica_client2,
                                    grpc_replica_client,
                                    grpc_replica_client2):
    open_replica(replica_client, grpc_replica_client)
    open_replica(replica_client2, grpc_replica_client2)

    v = controller_client.list_volume()[0]
    v = v.start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    cmd = [bin, 'snapshot', 'create']
    snap0 = subprocess.check_output(cmd).strip()
    snap1 = subprocess.check_output(cmd).strip()

    chain = replica_client.list_replica()[0].chain
    assert len(chain) == 3
    assert chain[0] == 'volume-head-002.img'
    assert chain[1] == 'volume-snap-{}.img'.format(snap1)
    assert chain[2] == 'volume-snap-{}.img'.format(snap0)

    cmd = [bin, 'snapshot', 'rm', snap1]
    subprocess.check_call(cmd)

    new_chain = replica_client.list_replica()[0].chain
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


def test_backup_cli(bin, controller_client,
                    replica_client, replica_client2,
                    grpc_replica_client, grpc_replica_client2,
                    backup_targets):
    for backup_target in backup_targets:
        backup_core(bin, controller_client,
                    replica_client, replica_client2,
                    grpc_replica_client, grpc_replica_client2,
                    backup_target)
        cleanup_replica(replica_client, grpc_replica_client)
        cleanup_replica(replica_client2, grpc_replica_client2)
        cleanup_controller(controller_client)
