import time
import random
import os
import subprocess
import json
import datetime
import pytest
import tempfile
import grpc

from urllib.parse import urlparse

from common.core import (  # NOQA
    cleanup_replica, cleanup_controller,
    get_expansion_snapshot_name,
    get_replica_paths_from_snapshot_name,
    get_snapshot_file_paths,
    get_replica_head_file_path,
    expand_volume_with_frontend,
    wait_and_check_volume_expansion,
    wait_for_rebuild_complete,
    wait_for_purge_completion,

    create_engine_process, create_replica_process,
    cleanup_process,
    get_process_address,
    wait_for_process_error,
    restore_with_frontend,
    reset_volume,
    get_replica_client_with_delay,
    get_controller_version_detail,
    verify_replica_mode,
    get_backup_volume_url,
    open_replica,
)

import common.cmd as cmd
from common.constants import (
    VOLUME_NAME,
    FRONTEND_TGT_BLOCKDEV,
    SIZE, EXPANDED_SIZE,
    SIZE_STR, EXPANDED_SIZE_STR,
    BACKUP_DIR,

    INSTANCE_MANAGER_ENGINE, INSTANCE_MANAGER_REPLICA,
    REPLICA_NAME, REPLICA_2_NAME, ENGINE_NAME,
    RETRY_COUNTS_SHORT,
    RETRY_INTERVAL_SHORT,
    MESSAGE_TYPE_ERROR,
)

from common.util import (
    finddir, findfile
)

from rpc.controller.controller_client import ControllerClient
from rpc.replica.replica_client import ReplicaClient
from rpc.instance_manager.process_manager_client import ProcessManagerClient

VOLUME_HEAD = "volume-head"

VOLUME_CONFIG_FILE = "volume.cfg"
VOLUME_TMP_CONFIG_FILE = "volume.cfg.tmp"


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
    return os.path.join(_base(), '../../{}'.format(f))


def _base():
    return os.path.dirname(__file__)


@pytest.fixture(scope='session')
def bin():
    c = _file('bin/longhorn')
    assert os.path.exists(c)
    return c


def setup_module():
    if os.path.exists(BACKUP_DIR):
        subprocess.check_call(["rm", "-rf", BACKUP_DIR])

    os.makedirs(BACKUP_DIR)
    assert os.path.exists(BACKUP_DIR)


def getNow():
    time.sleep(1)
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat()


def test_replica_add_start(bin, grpc_controller_client,  # NOQA
                           grpc_replica_client):  # NOQA
    open_replica(grpc_replica_client)

    cmd = [bin, '--debug', '--url', grpc_controller_client.address,
           'add-replica',
           '--size', SIZE_STR,
           '--current-size', SIZE_STR,
           grpc_replica_client.url]
    subprocess.check_call(cmd)
    wait_for_rebuild_complete(grpc_controller_client.address)

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
           '--size', SIZE_STR,
           '--current-size', SIZE_STR,
           grpc_replica_client.url]
    subprocess.check_call(cmd)
    wait_for_rebuild_complete(grpc_controller_client.address)

    volume = grpc_controller_client.volume_get()
    assert volume.replicaCount == 1

    cmd = [bin, '--debug', '--url', grpc_controller_client.address,
           'add-replica',
           '--size', SIZE_STR,
           '--current-size', SIZE_STR,
           grpc_replica_client2.url]
    subprocess.check_call(cmd)
    wait_for_rebuild_complete(grpc_controller_client.address)

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
           '--size', SIZE_STR,
           '--current-size', SIZE_STR,
           grpc_replica_client.url]
    subprocess.check_call(cmd)

    volume = grpc_controller_client.volume_get()
    assert volume.replicaCount == 1

    grpc_replica_client2.replica_open()
    grpc_replica_client2.rebuilding_set(rebuilding=True)
    grpc_replica_client2.replica_close()

    cmd = [bin, '--debug', '--url', grpc_controller_client.address,
           'add-replica',
           '--size', SIZE_STR,
           '--current-size', SIZE_STR,
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
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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

    grpc_controller_client.volume_frontend_shutdown()
    grpc_controller_client.volume_revert(name='foo1')
    grpc_controller_client.volume_frontend_start(
        frontend=FRONTEND_TGT_BLOCKDEV)
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
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url])
    assert v.replicaCount == 1

    cmd = [bin, '--url', grpc_controller_client.address,
           'add',
           '--size', SIZE_STR,
           '--current-size', SIZE_STR,
           r2_url]
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


def backup_core(bin, engine_manager_client,  # NOQA
                grpc_controller_client,  # NOQA
                grpc_replica_client,  # NOQA
                grpc_replica_client2,  # NOQA
                backup_target):
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
    assert v.replicaCount == 2

    backup_type = urlparse(backup_target).scheme

    # create & process backup1
    snapshot1 = cmd.snapshot_create(grpc_controller_client.address)
    output = grpc_replica_client.replica_get().chain[1]
    assert output == 'volume-snap-{}.img'.format(snapshot1)

    backup1 = cmd.backup_create(grpc_controller_client.address,
                                snapshot1, backup_target,
                                {'name': 'backup1', 'type': backup_type})
    backup1_info = cmd.backup_inspect(grpc_controller_client.address, backup1)
    assert backup1_info["URL"] == backup1
    assert backup1_info["IsIncremental"] is False
    assert backup1_info["VolumeName"] == VOLUME_NAME
    assert backup1_info["VolumeSize"] == SIZE_STR
    assert backup1_info["SnapshotName"] == snapshot1
    assert len(backup1_info["Labels"]) == 2
    assert backup1_info["Labels"]["name"] == "backup1"
    assert backup1_info["Labels"]["type"] == backup_type

    # create & process backup2
    snapshot2 = cmd.snapshot_create(grpc_controller_client.address)
    output = grpc_replica_client.replica_get().chain[1]
    assert output == 'volume-snap-{}.img'.format(snapshot2)

    backup2 = cmd.backup_create(grpc_controller_client.address,
                                snapshot2, backup_target)

    backup2_info = cmd.backup_inspect(grpc_controller_client.address, backup2)
    assert backup2_info["URL"] == backup2
    assert backup2_info["IsIncremental"] is True
    assert backup2_info["VolumeName"] == VOLUME_NAME
    assert backup2_info["VolumeSize"] == SIZE_STR
    assert backup2_info["SnapshotName"] == snapshot2
    if backup2_info["Labels"] is not None:
        assert len(backup2_info["Labels"]) == 0

    # list all known backups for volume
    volume_info = cmd.backup_volume_list(
        grpc_controller_client.address,
        VOLUME_NAME, backup_target,
        include_backup_details=True)
    assert volume_info[VOLUME_NAME] is not None
    backup_list = volume_info[VOLUME_NAME]["Backups"]
    assert backup_list[backup1_info["Name"]] is not None
    assert backup_list[backup2_info["Name"]] is not None

    # inspect backup volume metadata
    url = get_backup_volume_url(backup_target, VOLUME_NAME)
    volume_info = cmd.backup_inspect_volume(
        grpc_controller_client.address, url)
    assert volume_info["Name"] == VOLUME_NAME
    assert volume_info["Size"] == SIZE_STR

    # test that corrupt backups are signaled during a list operation
    # https://github.com/longhorn/longhorn/issues/1212
    volume_dir = finddir(BACKUP_DIR, VOLUME_NAME)
    assert volume_dir
    assert os.path.exists(volume_dir)
    backup_dir = os.path.join(volume_dir, "backups")
    assert os.path.exists(backup_dir)
    backup_cfg_name = "backup_" + backup2_info["Name"] + ".cfg"
    assert backup_cfg_name
    backup_cfg_path = findfile(backup_dir, backup_cfg_name)
    assert os.path.exists(backup_cfg_path)
    backup_tmp_cfg_path = os.path.join(volume_dir, backup_cfg_name)
    os.rename(backup_cfg_path, backup_tmp_cfg_path)
    assert os.path.exists(backup_tmp_cfg_path)

    corrupt_backup = open(backup_cfg_path, "w")
    assert corrupt_backup
    assert corrupt_backup.write("{corrupt: definitely") > 0
    corrupt_backup.close()

    # request the new backup list
    volume_info = cmd.backup_volume_list(
        grpc_controller_client.address,
        VOLUME_NAME, backup_target,
        include_backup_details=True)
    assert volume_info[VOLUME_NAME] is not None
    assert backup_list[backup1_info["Name"]] is not None
    assert backup_list[backup2_info["Name"]] is not None

    # inspect backup volume metadata
    url = get_backup_volume_url(backup_target, VOLUME_NAME)
    volume_info = cmd.backup_inspect_volume(
        grpc_controller_client.address, url)
    assert volume_info["Name"] == VOLUME_NAME

    # inspect volume snapshot backup metadata
    backup1_info = cmd.backup_inspect(grpc_controller_client.address, backup1)
    assert backup1_info["Messages"] is None
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_inspect(grpc_controller_client.address, backup2)

    # switch back to valid cfg
    os.rename(backup_tmp_cfg_path, backup_cfg_path)
    assert cmd.backup_inspect(grpc_controller_client.address, backup2)

    # test that list returns a volume_info with an error message
    # for a missing volume.cfg instead of failing with an error
    # https://github.com/rancher/longhorn/issues/399
    volume_cfg_path = findfile(volume_dir, VOLUME_CONFIG_FILE)
    assert os.path.exists(volume_cfg_path)
    volume_tmp_cfg_path = volume_cfg_path.replace(
        VOLUME_CONFIG_FILE, VOLUME_TMP_CONFIG_FILE)
    os.rename(volume_cfg_path, volume_tmp_cfg_path)
    assert os.path.exists(volume_tmp_cfg_path)

    url = get_backup_volume_url(backup_target, VOLUME_NAME)
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_inspect_volume(grpc_controller_client.address, url)

    os.rename(volume_tmp_cfg_path, volume_cfg_path)
    assert os.path.exists(volume_cfg_path)

    url = get_backup_volume_url(backup_target, VOLUME_NAME)
    volume_info = cmd.backup_inspect_volume(
        grpc_controller_client.address, url)
    assert volume_info["Messages"] is not None
    assert MESSAGE_TYPE_ERROR not in volume_info["Messages"]

    # backup doesn't exists so it should error
    with pytest.raises(subprocess.CalledProcessError):
        url = backup_target + "?backup=backup-unk" + "&volume=" + VOLUME_NAME
        cmd.backup_inspect(grpc_controller_client.address, url)

    # this returns unsupported driver since `bad` is not a known scheme
    with pytest.raises(subprocess.CalledProcessError):
        cmd.backup_inspect(grpc_controller_client.address, "bad://xxx")

    reset_volume(grpc_controller_client,
                 grpc_replica_client, grpc_replica_client2)
    restore_with_frontend(grpc_controller_client.address,
                          ENGINE_NAME, backup1)
    restore_with_frontend(grpc_controller_client.address,
                          ENGINE_NAME, backup2)

    # remove backups + volume
    cmd.backup_rm(grpc_controller_client.address, backup1)
    cmd.backup_rm(grpc_controller_client.address, backup2)
    cmd.backup_volume_rm(grpc_controller_client.address,
                         VOLUME_NAME, backup_target)

    assert os.path.exists(BACKUP_DIR)
    assert not os.path.exists(volume_cfg_path)


def test_snapshot_purge_basic(bin, grpc_controller_client,  # NOQA
                              grpc_replica_client,  # NOQA
                              grpc_replica_client2):  # NOQA
    open_replica(grpc_replica_client)
    open_replica(grpc_replica_client2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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

    wait_for_purge_completion(grpc_controller_client.address)

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
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
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

    expand_volume_with_frontend(grpc_controller_client, EXPANDED_SIZE)
    wait_and_check_volume_expansion(
        grpc_controller_client, EXPANDED_SIZE)

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
    wait_for_purge_completion(grpc_controller_client.address)

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
    wait_for_purge_completion(grpc_controller_client.address)

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


def test_expand_multiple_times():
    for i in range(30):
        em_client = ProcessManagerClient(INSTANCE_MANAGER_ENGINE)
        engine_process = create_engine_process(em_client)
        grpc_controller_client = ControllerClient(
            get_process_address(engine_process))
        get_controller_version_detail(grpc_controller_client)
        rm_client = ProcessManagerClient(INSTANCE_MANAGER_REPLICA)
        replica_process = create_replica_process(rm_client, REPLICA_NAME)
        grpc_replica_client = ReplicaClient(
            get_process_address(replica_process))
        cleanup_replica(grpc_replica_client)

        open_replica(grpc_replica_client)
        r1_url = grpc_replica_client.url
        v = grpc_controller_client.volume_start(
            SIZE, SIZE, replicas=[r1_url])
        assert v.replicaCount == 1

        expand_volume_with_frontend(
            grpc_controller_client, EXPANDED_SIZE)
        wait_and_check_volume_expansion(
            grpc_controller_client, EXPANDED_SIZE)

        cleanup_process(em_client)
        cleanup_process(rm_client)


def test_single_replica_failure_during_engine_start(bin):  # NOQA
    """
    Test if engine still works fine if there is an invalid
    replica/backend in the starting phase

    1. Create then initialize 1 engine and 2 replicas.
    2. Start the engine.
    3. Create 2 snapshots.
    4. Mess up the replica1 by manually modifying the snapshot meta file.
    5. Mock volume detachment by deleting
       the engine process and replicas processes.
    6. Mock volume reattachment by recreating processes and
       re-starting the engine.
    7. Check if the engine is up and if replica1 is mode ERR
       in the engine.
    8. Check if the engine still works fine
       by creating one more snapshot.
    9. Remove the ERR replica from the engine
       then check snapshot remove and snapshot purge work fine.
    10. Check if the snapshot list is correct.
    """
    em_client = ProcessManagerClient(INSTANCE_MANAGER_ENGINE)
    engine_process = create_engine_process(em_client)
    grpc_controller_client = ControllerClient(
        get_process_address(engine_process))
    get_controller_version_detail(grpc_controller_client)

    rm_client = ProcessManagerClient(INSTANCE_MANAGER_REPLICA)
    replica_dir1 = tempfile.mkdtemp()
    replica_dir2 = tempfile.mkdtemp()
    replica_process1 = create_replica_process(rm_client, REPLICA_NAME,
                                              replica_dir=replica_dir1)
    grpc_replica_client1 = ReplicaClient(
        get_process_address(replica_process1))
    cleanup_replica(grpc_replica_client1)
    replica_process2 = create_replica_process(rm_client, REPLICA_2_NAME,
                                              replica_dir=replica_dir2)
    grpc_replica_client2 = ReplicaClient(
        get_process_address(replica_process2))
    cleanup_replica(grpc_replica_client2)

    open_replica(grpc_replica_client1)
    open_replica(grpc_replica_client2)
    r1_url = grpc_replica_client1.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
    assert v.replicaCount == 2

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create']
    snap0 = subprocess.check_output(cmd, encoding='utf-8').strip()
    expected = grpc_replica_client1.replica_get().chain[1]
    assert expected == 'volume-snap-{}.img'.format(snap0)

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create',
           '--label', 'name=snap1', '--label', 'key=value']
    snap1 = subprocess.check_output(cmd, encoding='utf-8').strip()

    # Mess up the replica1 by manually modifying the snapshot meta file
    r1_snap1_meta_path = os.path.join(replica_dir1,
                                      'volume-snap-{}.img.meta'.format(snap1))
    with open(r1_snap1_meta_path, 'r') as f:
        snap1_meta_info = json.load(f)
    with open(r1_snap1_meta_path, 'w') as f:
        snap1_meta_info["Parent"] = "invalid-parent.img"
        json.dump(snap1_meta_info, f)

    # Mock detach:
    cleanup_process(em_client)
    cleanup_process(rm_client)

    # Mock reattach:
    #   1. Directly create replicas processes.
    #   2. Call replica_create() to init replica servers for replica processes.
    #   3. Create one engine process and start the engine with replicas.
    replica_process1 = create_replica_process(rm_client, REPLICA_NAME,
                                              replica_dir=replica_dir1)
    grpc_replica_client1 = get_replica_client_with_delay(ReplicaClient(
        get_process_address(replica_process1)))
    grpc_replica_client1.replica_create(size=SIZE_STR)
    replica_process2 = create_replica_process(rm_client, REPLICA_2_NAME,
                                              replica_dir=replica_dir2)
    grpc_replica_client2 = get_replica_client_with_delay(ReplicaClient(
        get_process_address(replica_process2)))
    grpc_replica_client2.replica_create(size=SIZE_STR)

    engine_process = create_engine_process(em_client)
    grpc_controller_client = ControllerClient(
        get_process_address(engine_process))
    get_controller_version_detail(grpc_controller_client)
    r1_url = grpc_replica_client1.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
    assert v.replicaCount == 2

    # Check if replica1 is mode `ERR`
    rs = grpc_controller_client.replica_list()
    assert len(rs) == 2
    r1_verified = False
    r2_verified = False
    for r in rs:
        if r.address == r1_url:
            assert r.mode == 'ERR'
            r1_verified = True
        if r.address == r2_url:
            assert r.mode == 'RW'
            r2_verified = True
    assert r1_verified
    assert r2_verified

    # The engine still works fine
    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create']
    snap2 = subprocess.check_output(cmd, encoding='utf-8').strip()

    # Remove the ERR replica before removing snapshots
    grpc_controller_client.replica_delete(r1_url)
    rs = grpc_controller_client.replica_list()
    assert len(rs) == 1
    assert rs[0].address == r2_url
    assert rs[0].mode == "RW"

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'rm', snap1]
    subprocess.check_call(cmd)
    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'purge']
    subprocess.check_call(cmd)
    wait_for_purge_completion(grpc_controller_client.address)

    cmd = [bin, '--debug',
           '--url', grpc_controller_client.address,
           'snapshot', 'ls']
    ls_output = subprocess.check_output(cmd, encoding='utf-8')

    assert ls_output == '''ID
{}
{}
'''.format(snap2, snap0)

    cleanup_process(em_client)
    cleanup_process(rm_client)


def test_engine_restart_after_sigkill(bin):  # NOQA
    """
    Test if engine can be restarted after crashing by SIGKILL.

    1. Create then initialize 1 engine and 2 replicas.
    2. Start the engine.
    3. Create 2 snapshots.
    4. Use SIGKILL to kill the engine process.
    5. Wait for the engine errored.
    6. Mock volume detachment by deleting
       the engine process and replicas processes.
    7. Mock volume reattachment by recreating processes and
       re-starting the engine.
    8. Check if the engine is up with 2 replicas.
    9. Check if the engine still works fine
       by creating/removing/purging snapshots.
    """
    em_client = ProcessManagerClient(INSTANCE_MANAGER_ENGINE)
    engine_process = create_engine_process(em_client)
    grpc_controller_client = ControllerClient(
        get_process_address(engine_process))
    get_controller_version_detail(grpc_controller_client)

    rm_client = ProcessManagerClient(INSTANCE_MANAGER_REPLICA)
    replica_dir1 = tempfile.mkdtemp()
    replica_dir2 = tempfile.mkdtemp()
    replica_process1 = create_replica_process(rm_client, REPLICA_NAME,
                                              replica_dir=replica_dir1)
    grpc_replica_client1 = ReplicaClient(
        get_process_address(replica_process1))
    cleanup_replica(grpc_replica_client1)
    replica_process2 = create_replica_process(rm_client, REPLICA_2_NAME,
                                              replica_dir=replica_dir2)
    grpc_replica_client2 = ReplicaClient(
        get_process_address(replica_process2))
    cleanup_replica(grpc_replica_client2)

    open_replica(grpc_replica_client1)
    open_replica(grpc_replica_client2)
    r1_url = grpc_replica_client1.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
    assert v.replicaCount == 2

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create']
    snap0 = subprocess.check_output(cmd, encoding='utf-8').strip()
    expected = grpc_replica_client1.replica_get().chain[1]
    assert expected == 'volume-snap-{}.img'.format(snap0)

    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create',
           '--label', 'name=snap1', '--label', 'key=value']
    snap1 = subprocess.check_output(cmd, encoding='utf-8').strip()

    cmd = ["bash", "-c",
           "kill -9 $(ps aux | grep %s | grep -v grep | awk '{print $2}')" %
           VOLUME_NAME]
    subprocess.check_call(cmd)
    wait_for_process_error(em_client, ENGINE_NAME)

    # Mock detach:
    cleanup_process(em_client)
    cleanup_process(rm_client)

    # Mock reattach:
    #   1. Directly create replicas processes.
    #   2. Call replica_create() to init replica servers for replica processes.
    #   3. Create one engine process and start the engine with replicas.
    replica_process1 = create_replica_process(rm_client, REPLICA_NAME,
                                              replica_dir=replica_dir1)
    grpc_replica_client1 = get_replica_client_with_delay(ReplicaClient(
        get_process_address(replica_process1)))
    grpc_replica_client1.replica_create(size=SIZE_STR)
    replica_process2 = create_replica_process(rm_client, REPLICA_2_NAME,
                                              replica_dir=replica_dir2)
    grpc_replica_client2 = get_replica_client_with_delay(ReplicaClient(
        get_process_address(replica_process2)))
    grpc_replica_client2.replica_create(size=SIZE_STR)

    engine_process = create_engine_process(em_client)
    grpc_controller_client = ControllerClient(
        get_process_address(engine_process))
    get_controller_version_detail(grpc_controller_client)
    r1_url = grpc_replica_client1.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
    assert v.replicaCount == 2

    # Verify the engine still works fine
    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'create']
    snap2 = subprocess.check_output(cmd, encoding='utf-8').strip()
    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'rm', snap1]
    subprocess.check_call(cmd)
    cmd = [bin, '--url', grpc_controller_client.address,
           'snapshot', 'purge']
    subprocess.check_call(cmd)
    wait_for_purge_completion(grpc_controller_client.address)
    cmd = [bin, '--debug',
           '--url', grpc_controller_client.address,
           'snapshot', 'ls']
    ls_output = subprocess.check_output(cmd, encoding='utf-8')

    assert ls_output == '''ID
{}
{}
'''.format(snap2, snap0)

    cleanup_process(em_client)
    cleanup_process(rm_client)


def test_replica_removal_and_recreation(bin):  # NOQA
    """
    Test if engine can be remove and recreate a replica.

    1. Create then initialize 1 engine and 2 replicas.
    2. Start the engine.
    3. Start the replicas.
    4. Remove one replica.
    5. Immediately recreate the removed replica.
    6. Check that the recreated replica is created and it's mode
       isn't error.
    """

    em_client = ProcessManagerClient(INSTANCE_MANAGER_ENGINE)
    engine_process = create_engine_process(em_client)
    grpc_controller_client = ControllerClient(
        get_process_address(engine_process))
    get_controller_version_detail(grpc_controller_client)

    rm_client = ProcessManagerClient(INSTANCE_MANAGER_REPLICA)
    replica_dir1 = tempfile.mkdtemp()
    replica_dir2 = tempfile.mkdtemp()
    replica_process1 = create_replica_process(rm_client, REPLICA_NAME,
                                              replica_dir=replica_dir1)
    grpc_replica_client1 = ReplicaClient(
        get_process_address(replica_process1))
    cleanup_replica(grpc_replica_client1)
    replica_process2 = create_replica_process(rm_client, REPLICA_2_NAME,
                                              replica_dir=replica_dir2)
    grpc_replica_client2 = ReplicaClient(
        get_process_address(replica_process2))
    cleanup_replica(grpc_replica_client2)

    open_replica(grpc_replica_client1)
    open_replica(grpc_replica_client2)
    r1_url = grpc_replica_client1.url
    r2_url = grpc_replica_client2.url
    v = grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])
    assert v.replicaCount == 2

    # Remove the first replica and recreate it immediately.
    grpc_controller_client.replica_delete(r1_url)

    for i in range(RETRY_COUNTS_SHORT):
        try:
            grpc_controller_client.replica_create(r1_url, True, "RW")
        except grpc.RpcError as grpc_error:
            assert "replica must be closed" in grpc_error.details()
            continue
        break
        time.sleep(RETRY_INTERVAL_SHORT)

    # If the issue in #1628 occurs the mode will be ERR.
    verify_replica_mode(grpc_controller_client, r1_url, "RW")

    cleanup_process(em_client)
    cleanup_process(rm_client)


def test_replica_with_mismatched_size_add_start(bin, grpc_controller_client,  # NOQA
                                                grpc_replica_client,  # NOQA
                                                grpc_replica_client2):  # NOQA
    open_replica(grpc_replica_client, size=SIZE)
    open_replica(grpc_replica_client2, size=SIZE * 2)

    r1_url = grpc_replica_client.url
    r2_url = grpc_replica_client2.url
    grpc_controller_client.volume_start(
        SIZE, SIZE, replicas=[r1_url, r2_url])

    rs = grpc_controller_client.replica_list()
    for r in rs:
        if r.address == r1_url:
            assert r.mode == 'RW'
        else:
            assert r.mode == 'ERR'
