import tempfile

import pytest

from common.core import (
    create_replica_process, create_engine_process,
    delete_process,
    wait_for_process_running, wait_for_process_error,
    wait_for_process_deletion,
    check_dev_existence, wait_for_dev_deletion,
    upgrade_engine,
)

from common.constants import (
    LONGHORN_UPGRADE_BINARY, SIZE,
    PROC_STATE_RUNNING, PROC_STATE_STOPPING, PROC_STATE_STOPPED,
    PROC_STATE_ERROR,
    VOLUME_NAME_BASE, ENGINE_NAME_BASE, REPLICA_NAME_BASE,
)

from common.cli import (  # NOQA
    em_client, pm_client,  # NOQA
)


def test_start_stop_replicas(pm_client):  # NOQA
    rs = pm_client.process_list()
    assert len(rs) == 0

    for i in range(10):
        tmp_dir = tempfile.mkdtemp()
        name = REPLICA_NAME_BASE + str(i)
        r = create_replica_process(pm_client, name=name, replica_dir=tmp_dir)

        assert r.spec.name == name
        assert r.status.state == PROC_STATE_RUNNING

        r = pm_client.process_get(name=name)
        assert r.spec.name == name
        assert r.status.state == PROC_STATE_RUNNING

        rs = pm_client.process_list()
        assert len(rs) == (i+1)
        assert name in rs
        assert r.spec.name == name
        assert r.status.state == PROC_STATE_RUNNING

    for i in range(10):
        rs = pm_client.process_list()
        assert len(rs) == (10-i)

        name = REPLICA_NAME_BASE + str(i)
        r = pm_client.process_delete(name=name)
        assert r.spec.name == name
        assert r.status.state in (PROC_STATE_STOPPING,
                                  PROC_STATE_STOPPED)
        wait_for_process_deletion(pm_client, name)

        rs = pm_client.process_list()
        assert len(rs) == (9-i)

    rs = pm_client.process_list()
    assert len(rs) == 0


def test_process_creation_failure(pm_client):  # NOQA
    rs = pm_client.process_list()
    assert len(rs) == 0

    count = 5
    for i in range(count):
        tmp_dir = tempfile.mkdtemp()
        name = REPLICA_NAME_BASE + str(i)

        args = ["replica", tmp_dir, "--size", str(SIZE)]
        pm_client.process_create(
            name=name, binary="/opt/non-existing-binary", args=args,
            port_count=15, port_args=["--listen,localhost:"])
        wait_for_process_error(pm_client, name)

        r = pm_client.process_get(name=name)
        assert r.spec.name == name
        assert r.status.state == PROC_STATE_ERROR
        assert "no such file or directory" in r.status.error_msg

    for i in range(count):
        rs = pm_client.process_list()
        assert len(rs) == (count-i)

        name = REPLICA_NAME_BASE + str(i)
        pm_client.process_delete(name=name)
        wait_for_process_deletion(pm_client, name)

        rs = pm_client.process_list()
        assert len(rs) == (count-1-i)

    rs = pm_client.process_list()
    assert len(rs) == 0


def test_one_volume(pm_client, em_client):  # NOQA
    rs = pm_client.process_list()
    assert len(rs) == 0

    replica_args = []

    for i in range(3):
        tmp_dir = tempfile.mkdtemp()
        name = REPLICA_NAME_BASE + str(i)
        r = create_replica_process(pm_client, name=name, replica_dir=tmp_dir)

        assert r.spec.name == name
        assert r.status.state == PROC_STATE_RUNNING

        r = pm_client.process_get(name=name)
        assert r.spec.name == name
        assert r.status.state == PROC_STATE_RUNNING

        rs = pm_client.process_list()
        assert len(rs) == (i+1)
        assert name in rs
        assert r.spec.name == name
        assert r.status.state == PROC_STATE_RUNNING

        replica_args.append("tcp://localhost:"+str(r.status.port_start))

    engine_name = ENGINE_NAME_BASE + "0"
    volume_name = VOLUME_NAME_BASE + "0"
    e = create_engine_process(em_client, name=engine_name,
                              volume_name=volume_name,
                              replicas=replica_args)

    assert e.spec.name == engine_name

    check_dev_existence(volume_name)

    es = em_client.process_list()
    assert len(es) == 1
    assert engine_name in es
    e = es[engine_name]
    assert e.spec.name == engine_name
    assert e.status.state == PROC_STATE_RUNNING

    ps = pm_client.process_list()
    assert len(ps) == 3

    delete_process(em_client, engine_name)
    # test duplicate call
    delete_process(em_client, engine_name)
    wait_for_process_deletion(em_client, engine_name)
    # test duplicate call
    delete_process(em_client, engine_name)

    ps = pm_client.process_list()
    assert len(ps) == 3

    for i in range(3):
        name = REPLICA_NAME_BASE + str(i)
        r = pm_client.process_delete(name=name)
        assert r.spec.name == name
        assert r.status.state in (PROC_STATE_STOPPING,
                                  PROC_STATE_STOPPED)

        wait_for_process_deletion(pm_client, name)

    ps = pm_client.process_list()
    assert len(ps) == 0


def test_multiple_volumes(pm_client, em_client):  # NOQA
    rs = pm_client.process_list()
    assert len(rs) == 0

    cnt = 5

    for i in range(cnt):
        replica_args = []
        tmp_dir = tempfile.mkdtemp()
        replica_name = REPLICA_NAME_BASE + str(i)
        r = create_replica_process(pm_client,
                                   name=replica_name, replica_dir=tmp_dir)

        assert r.spec.name == replica_name
        assert r.status.state == PROC_STATE_RUNNING

        r = pm_client.process_get(name=replica_name)
        assert r.spec.name == replica_name
        assert r.status.state == PROC_STATE_RUNNING

        rs = pm_client.process_list()
        assert len(rs) == i+1
        assert replica_name in rs
        r = rs[replica_name]
        assert r.spec.name == replica_name
        assert r.status.state == PROC_STATE_RUNNING

        replica_args.append("tcp://localhost:"+str(r.status.port_start))

        engine_name = ENGINE_NAME_BASE + str(i)
        volume_name = VOLUME_NAME_BASE + str(i)
        e = create_engine_process(em_client, name=engine_name,
                                  volume_name=volume_name,
                                  replicas=replica_args)

        assert e.spec.name == engine_name
        check_dev_existence(volume_name)

        es = em_client.process_list()
        assert len(es) == i+1
        assert engine_name in es
        e = es[engine_name]
        assert e.spec.name == engine_name
        assert e.status.state == PROC_STATE_RUNNING

        ps = pm_client.process_list()
        assert len(ps) == i+1

    for i in range(cnt):
        engine_name = ENGINE_NAME_BASE + str(i)
        volume_name = VOLUME_NAME_BASE + str(i)
        delete_process(em_client, engine_name)
        wait_for_process_deletion(em_client, engine_name)
        wait_for_dev_deletion(volume_name)

        es = em_client.process_list()
        assert len(es) == (cnt-1-i)
        assert engine_name not in es


@pytest.mark.skip(reason="debug")
def test_engine_upgrade(pm_client, em_client):  # NOQA
    rs = pm_client.process_list()
    assert len(rs) == 0

    dir_base = "/tmp/replica"
    cnt = 3

    for i in range(cnt):
        replica_args = []
        dir = dir_base + str(i)
        replica_name = REPLICA_NAME_BASE + str(i)
        r = create_replica_process(pm_client, name=replica_name,
                                   replica_dir=dir)

        assert r.spec.name == replica_name
        assert r.status.state == PROC_STATE_RUNNING

        r = pm_client.process_get(name=replica_name)
        assert r.spec.name == replica_name
        assert r.status.state == PROC_STATE_RUNNING

        rs = pm_client.process_list()
        assert len(rs) == i+1
        assert replica_name in rs
        r = rs[replica_name]
        assert r.spec.name == replica_name
        assert r.status.state == PROC_STATE_RUNNING

        replica_args.append("tcp://localhost:"+str(r.status.port_start))

        engine_name = ENGINE_NAME_BASE + str(i)
        volume_name = VOLUME_NAME_BASE + str(i)
        e = create_engine_process(em_client, name=engine_name,
                                  volume_name=volume_name,
                                  replicas=replica_args)

        assert e.spec.name == engine_name
        check_dev_existence(volume_name)

        es = em_client.process_list()
        assert len(es) == i+1
        assert engine_name in es
        e = es[engine_name]
        assert e.spec.name == engine_name
        assert e.status.state == PROC_STATE_RUNNING

    dir = dir_base + "0"
    engine_name = ENGINE_NAME_BASE + "0"
    replica_name = REPLICA_NAME_BASE + "0"
    volume_name = VOLUME_NAME_BASE + "0"
    replica_name_upgrade = REPLICA_NAME_BASE + "0-upgrade"
    r = create_replica_process(pm_client, name=replica_name_upgrade,
                               binary=LONGHORN_UPGRADE_BINARY,
                               replica_dir=dir)
    assert r.spec.name == replica_name_upgrade
    assert r.status.state == PROC_STATE_RUNNING

    replicas = ["tcp://localhost:"+str(r.status.port_start)]
    e = upgrade_engine(em_client, LONGHORN_UPGRADE_BINARY,
                       engine_name, volume_name, replicas)
    assert e.spec.name == engine_name
    check_dev_existence(volume_name)

    r = pm_client.process_delete(name=replica_name)
    assert r.spec.name == replica_name
    assert r.status.state in (PROC_STATE_STOPPING,
                              PROC_STATE_STOPPED)

    wait_for_process_deletion(pm_client, replica_name)

    check_dev_existence(volume_name)

    wait_for_process_running(em_client, engine_name)
    es = em_client.process_list()
    assert engine_name in es
    e = es[engine_name]
    assert e.spec.name == engine_name
    assert e.status.state == PROC_STATE_RUNNING

    delete_process(em_client, engine_name)
    wait_for_process_deletion(em_client, engine_name)
    wait_for_dev_deletion(volume_name)
