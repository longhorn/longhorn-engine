import tempfile

from common import (  # NOQA
    em_client, pm_client,  # NOQA
    create_replica_process, create_engine_process,
    delete_engine_process, wait_for_process_running,
    wait_for_process_deletion, wait_for_engine_deletion,
    check_dev_existence, wait_for_dev_deletion,

    SIZE, UPGRADE_LONGHORN_BINARY,
    INSTANCE_MANAGER_TYPE_ENGINE, PROC_STATE_RUNNING,
    PROC_STATE_STOPPING, PROC_STATE_STOPPED,
)


def test_start_stop_replicas(pm_client):  # NOQA
    rs = pm_client.process_list()
    assert len(rs) == 0

    name_base = "replica"

    for i in range(10):
        tmp_dir = tempfile.mkdtemp()
        name = name_base + str(i)
        r = create_replica_process(pm_client, name=name, dir=tmp_dir)

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

        name = name_base + str(i)
        r = pm_client.process_delete(name=name)
        assert r.spec.name == name
        assert r.status.state in (PROC_STATE_STOPPING,
                                  PROC_STATE_STOPPED)
        wait_for_process_deletion(pm_client, name)

        rs = pm_client.process_list()
        assert len(rs) == (9-i)

    rs = pm_client.process_list()
    assert len(rs) == 0


def test_one_volume(pm_client, em_client):  # NOQA
    rs = pm_client.process_list()
    assert len(rs) == 0

    replica_args = []

    name_base = "replica"
    for i in range(3):
        tmp_dir = tempfile.mkdtemp()
        name = name_base + str(i)
        r = create_replica_process(pm_client, name=name, dir=tmp_dir)

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

    engine_name = "testengine"
    volume_name = "testvol"
    e = create_engine_process(em_client, name=engine_name,
                              volume_name=volume_name,
                              replicas=replica_args)

    assert e.spec.name == engine_name

    check_dev_existence(volume_name)

    es = em_client.engine_list()
    assert len(es) == 1
    assert engine_name in es
    e = es[engine_name]
    assert e.spec.name == engine_name
    assert e.status.process_status.state == PROC_STATE_RUNNING

    ps = pm_client.process_list()
    assert len(ps) == 4

    delete_engine_process(em_client, engine_name)
    # test duplicate call
    delete_engine_process(em_client, engine_name)
    wait_for_engine_deletion(em_client, engine_name)

    # test duplicate call
    delete_engine_process(em_client, engine_name)

    ps = pm_client.process_list()
    assert len(ps) == 3

    for i in range(3):
        name = name_base + str(i)
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

    replica_name_base = "testreplica"
    engine_name_base = "testengine"
    volume_name_base = "testvol"
    cnt = 5

    for i in range(cnt):
        replica_args = []
        tmp_dir = tempfile.mkdtemp()
        replica_name = replica_name_base + str(i)
        r = create_replica_process(pm_client, name=replica_name, dir=tmp_dir)

        assert r.spec.name == replica_name
        assert r.status.state == PROC_STATE_RUNNING

        r = pm_client.process_get(name=replica_name)
        assert r.spec.name == replica_name
        assert r.status.state == PROC_STATE_RUNNING

        rs = pm_client.process_list()
        assert len(rs) == (2*i+1)
        assert replica_name in rs
        r = rs[replica_name]
        assert r.spec.name == replica_name
        assert r.status.state == PROC_STATE_RUNNING

        replica_args.append("tcp://localhost:"+str(r.status.port_start))

        engine_name = engine_name_base + str(i)
        volume_name = volume_name_base + str(i)
        e = create_engine_process(em_client, name=engine_name,
                                  volume_name=volume_name,
                                  replicas=replica_args)

        assert e.spec.name == engine_name
        check_dev_existence(volume_name)

        es = em_client.engine_list()
        assert len(es) == (i+1)
        assert engine_name in es
        e = es[engine_name]
        assert e.spec.name == engine_name
        assert e.status.process_status.state == PROC_STATE_RUNNING

        ps = pm_client.process_list()
        assert len(ps) == 2*(i+1)

    for i in range(cnt):
        engine_name = engine_name_base + str(i)
        volume_name = volume_name_base + str(i)
        delete_engine_process(em_client, engine_name)
        wait_for_engine_deletion(em_client, engine_name)
        wait_for_dev_deletion(volume_name)

        es = em_client.engine_list()
        assert len(es) == (cnt-1-i)
        assert engine_name not in es


def test_engine_upgrade(pm_client, em_client):  # NOQA
    rs = pm_client.process_list()
    assert len(rs) == 0

    replica_name_base = "testreplica"
    engine_name_base = "testengine"
    volume_name_base = "testvol"
    dir_base = "/tmp/replica"
    cnt = 3

    for i in range(cnt):
        replica_args = []
        dir = dir_base + str(i)
        replica_name = replica_name_base + str(i)
        r = create_replica_process(pm_client, name=replica_name, dir=dir)

        assert r.spec.name == replica_name
        assert r.status.state == PROC_STATE_RUNNING

        r = pm_client.process_get(name=replica_name)
        assert r.spec.name == replica_name
        assert r.status.state == PROC_STATE_RUNNING

        rs = pm_client.process_list()
        assert len(rs) == (2*i+1)
        assert replica_name in rs
        r = rs[replica_name]
        assert r.spec.name == replica_name
        assert r.status.state == PROC_STATE_RUNNING

        replica_args.append("tcp://localhost:"+str(r.status.port_start))

        engine_name = engine_name_base + str(i)
        volume_name = volume_name_base + str(i)
        e = create_engine_process(em_client, name=engine_name,
                                  volume_name=volume_name,
                                  replicas=replica_args)

        assert e.spec.name == engine_name
        check_dev_existence(volume_name)

        es = em_client.engine_list()
        assert len(es) == (i+1)
        assert engine_name in es
        e = es[engine_name]
        assert e.spec.name == engine_name
        assert e.status.process_status.state == PROC_STATE_RUNNING

        ps = pm_client.process_list()
        assert len(ps) == 2*(i+1)

    dir = dir_base + "0"
    engine_name = engine_name_base + "0"
    replica_name = replica_name_base + "0"
    volume_name = volume_name_base + "0"
    replica_name_upgrade = replica_name_base + "0-upgrade"
    r = create_replica_process(pm_client, name=replica_name_upgrade,
                               binary=UPGRADE_LONGHORN_BINARY, dir=dir)
    assert r.spec.name == replica_name_upgrade
    assert r.status.state == PROC_STATE_RUNNING

    replica_args = ["tcp://localhost:"+str(r.status.port_start)]
    e = em_client.engine_upgrade(engine_name,
                                 UPGRADE_LONGHORN_BINARY,
                                 SIZE, replica_args)
    assert e.spec.name == engine_name
    check_dev_existence(volume_name)

    r = pm_client.process_delete(name=replica_name)
    assert r.spec.name == replica_name
    assert r.status.state in (PROC_STATE_STOPPING,
                              PROC_STATE_STOPPED)

    wait_for_process_deletion(pm_client, replica_name)

    check_dev_existence(volume_name)

    wait_for_process_running(em_client, engine_name,
                             INSTANCE_MANAGER_TYPE_ENGINE)
    es = em_client.engine_list()
    assert engine_name in es
    e = es[engine_name]
    assert e.spec.name == engine_name
    assert e.status.process_status.state == PROC_STATE_RUNNING

    delete_engine_process(em_client, engine_name)
    wait_for_engine_deletion(em_client, engine_name)
    wait_for_dev_deletion(volume_name)
