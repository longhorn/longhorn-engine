import time
import random
from os import path
import os
import subprocess
import json

import pytest
import cattle


REPLICA = 'tcp://localhost:9502'
REPLICA2 = 'tcp://localhost:9505'

BACKUP_DEST = '/tmp/longhorn-backup'

VOLUME_NAME = 'test-volume_1.0'
VOLUME_SIZE = str(4 * 1024 * 1024)  # 4M


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
    request.addfinalizer(lambda: cleanup_replica(c))
    return cleanup_replica(c)


@pytest.fixture
def replica_client2(request):
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


def setup_module():
    if os.path.exists(BACKUP_DEST):
        subprocess.check_call(["rm", "-rf", BACKUP_DEST])

    os.makedirs(BACKUP_DEST)
    assert os.path.exists(BACKUP_DEST)


def open_replica(client):
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


def test_replica_add_start(bin, controller_client, replica_client):
    open_replica(replica_client)

    cmd = [bin, '--debug', 'add-replica', REPLICA]
    subprocess.check_call(cmd)

    volume = controller_client.list_volume()[0]
    assert volume.replicaCount == 1


def test_replica_add_rebuild(bin, controller_client, replica_client,
                             replica_client2):
    open_replica(replica_client)
    open_replica(replica_client2)

    r = replica_client.list_replica()[0]
    r = r.open()
    r = r.snapshot(name='000')
    r = r.snapshot(name='001')

    l = replica_client2.list_replica()[0]

    assert r.chain == ['volume-head-002.img',
                       'volume-snap-001.img',
                       'volume-snap-000.img']

    assert l.chain != ['volume-head-002.img',
                       'volume-snap-001.img',
                       'volume-snap-000.img']

    r = r.close()
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


def test_replica_add_after_rebuild_failed(bin, controller_client,
                                          replica_client, replica_client2):
    open_replica(replica_client)
    open_replica(replica_client2)

    r = replica_client.list_replica()[0]
    r = r.open()
    r = r.snapshot(name='000')
    r.close()

    cmd = [bin, '--debug', 'add-replica', REPLICA]
    subprocess.check_call(cmd)

    volume = controller_client.list_volume()[0]
    assert volume.replicaCount == 1

    l = replica_client2.list_replica()[0]
    l = l.open()
    l = l.setrebuilding(rebuilding=True)
    l.close()

    cmd = [bin, '--debug', 'add-replica', REPLICA2]
    subprocess.check_call(cmd)

    volume = controller_client.list_volume()[0]
    assert volume.replicaCount == 2

    replicas = controller_client.list_replica()
    assert len(replicas) == 2

    for r in replicas:
        assert r.mode == 'RW'


def test_revert(bin, controller_client, replica_client, replica_client2):
    open_replica(replica_client)
    open_replica(replica_client2)

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


def test_snapshot(bin, controller_client, replica_client, replica_client2):
    open_replica(replica_client)
    open_replica(replica_client2)

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


def test_snapshot_ls(bin, controller_client, replica_client, replica_client2):
    open_replica(replica_client)
    open_replica(replica_client2)

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


def test_snapshot_create(bin, controller_client, replica_client,
                         replica_client2):
    open_replica(replica_client)
    open_replica(replica_client2)

    v = controller_client.list_volume()[0]
    v = v.start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    cmd = [bin, 'snapshot', 'create']
    output = subprocess.check_output(cmd).strip()
    expected = replica_client.list_replica()[0].chain[1]

    assert expected == 'volume-snap-{}.img'.format(output)

    cmd = [bin, '--debug', 'snapshot', 'ls']
    ls_output = subprocess.check_output(cmd)

    assert ls_output == '''ID
{}
'''.format(output)


def test_snapshot_rm(bin, controller_client, replica_client, replica_client2):
    open_replica(replica_client)
    open_replica(replica_client2)

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
        bin, controller_client, replica_client, replica_client2):
    open_replica(replica_client)
    open_replica(replica_client2)

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


def test_snapshot_last(bin, controller_client, replica_client,
                       replica_client2):
    open_replica(replica_client)
    open_replica(replica_client2)

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


def test_backup_core(bin, controller_client, replica_client,
                     replica_client2):
    open_replica(replica_client)
    open_replica(replica_client2)

    v = controller_client.list_volume()[0]
    v = v.start(replicas=[
        REPLICA,
        REPLICA2,
    ])
    assert v.replicaCount == 2

    cmd = [bin, 'snapshot', 'create']
    output = subprocess.check_output(cmd).strip()
    snapshot1 = replica_client.list_replica()[0].chain[1]

    assert snapshot1 == 'volume-snap-{}.img'.format(output)

    cmd = [bin, 'backup', 'create', snapshot1,
           '--dest', "vfs://" + BACKUP_DEST]
    backup1 = subprocess.check_output(cmd).strip()

    cmd = [bin, 'snapshot', 'create']
    output = subprocess.check_output(cmd).strip()
    snapshot2 = replica_client.list_replica()[0].chain[1]

    assert snapshot2 == 'volume-snap-{}.img'.format(output)

    cmd = [bin, 'backup', 'create', snapshot2,
           '--dest', "vfs://" + BACKUP_DEST]
    backup2 = subprocess.check_output(cmd).strip()

    cmd = [bin, 'backup', 'inspect', backup1]
    data = subprocess.check_output(cmd)
    backup1_info = json.loads(data)
    assert backup1_info["BackupURL"] == backup1
    assert backup1_info["VolumeName"] == VOLUME_NAME
    assert backup1_info["VolumeSize"] == VOLUME_SIZE
    assert backup1_info["SnapshotName"] == snapshot1

    cmd = [bin, 'backup', 'inspect', backup2]
    data = subprocess.check_output(cmd)
    backup2_info = json.loads(data)
    assert backup2_info["BackupURL"] == backup2
    assert backup2_info["VolumeName"] == VOLUME_NAME
    assert backup2_info["VolumeSize"] == VOLUME_SIZE
    assert backup2_info["SnapshotName"] == snapshot2

    cmd = [bin, 'backup', 'inspect',
           "vfs:///tmp/longhorn-backup?backup=backup-1234"
           + "&volume=test-volume_1.0"]
    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.check_call(cmd)
        assert 'cannot find' in str(e.value)

    cmd = [bin, 'backup', 'restore', backup1]
    subprocess.check_call(cmd)

    cmd = [bin, 'backup', 'restore', backup2]
    subprocess.check_call(cmd)

    cmd = [bin, 'backup', 'rm', backup1]
    subprocess.check_call(cmd)
    cmd = [bin, 'backup', 'rm', backup2]
    subprocess.check_call(cmd)

    assert os.path.exists(BACKUP_DEST)

    cmd = [bin, 'backup', 'inspect',
           "vfs:///tmp/longhorn-backup?backup=backup-1234"
           + "&volume=test-volume_1.0"]
    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.check_call(cmd)
        assert 'cannot find' in str(e.value)

    cmd = [bin, 'backup', 'inspect', "xxx"]
    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.check_call(cmd)
        assert 'not supported' in str(e.value)
