import tempfile
# import time
# import os.path

from common import em_client, pm_client  # NOQA
from common import create_replica_process
from common import wait_for_process_deletion
from common import create_engine_process
from common import check_engine_existence


PROC_STATE_RUNNING = "running"
PROC_STATE_STOPPED = "stopped"
PROC_STATE_ERROR = "error"

SIZE = 4 * 1024 * 1024


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

    engine_name = "controller"
    volume_name = "testvol"
    e = create_engine_process(em_client, name=engine_name,
                              volume_name=volume_name,
                              replicas=replica_args)

    assert e.spec.name == engine_name

    check_engine_existence(volume_name)

    ps = pm_client.process_list()
    assert len(ps) == 4
    assert engine_name in ps
    assert ps[engine_name].status.state == PROC_STATE_RUNNING

    em_client.engine_delete(engine_name)

    ps = pm_client.process_list()
    assert len(ps) == 3

    for i in range(3):
        name = name_base + str(i)
        r = pm_client.process_delete(name=name)
        assert r.spec.name == name

        wait_for_process_deletion(pm_client, name)

    ps = pm_client.process_list()
    assert len(ps) == 0


# def test_two_volumes():
#     rs = process_list()
#     assert(len(rs) == 0)
#
#     listen = "--listen,localhost:"
#     port_count = 15
#     name_base = "replica"
#
#     for i in range(2):
#         tmp_dir = tempfile.mkdtemp()
#         name = name_base + str(i)
#         r = process_create(name, ["replica", tmp_dir, "--size", str(SIZE)],
#                            port_count=port_count, port_args=[listen])
#         assert(r["name"] == name)
#         assert(r["state"] == PROC_STATE_RUNNING)
#
#         r = process_get(name)
#         assert(r["name"] == name)
#         assert(r["state"] == PROC_STATE_RUNNING)
#
#         rs = process_list()
#         assert(len(rs) == i+1)
#         assert(name in rs)
#         assert(rs[name]["name"] == name)
#         assert(rs[name]["state"] == PROC_STATE_RUNNING)
#
#     ps = process_list()
#     replica_args = []
#     for r in ps:
#         replica_args.append(["--replica",
#                             "tcp://localhost:"+str(ps[r]["portStart"])])
#
#     controller_launcher_listen = "--launcher-listen,localhost:"
#     controller_listen = "--listen,localhost:"
#     controller_port_count = 2
#
#     cname_base = "controller"
#     volume_name_base = "testvol"
#
#     for i in range(2):
#         name = cname_base + str(i)
#         volname = volume_name_base + str(i)
#         c = process_create(name,
#                            ["start", volname,
#                             "--longhorn-binary", cmd.LONGHORN_BINARY,
#                             "--size", str(SIZE),
#                             "--frontend", "tgt-blockdev"] + replica_args[i],
#                            port_count=controller_port_count,
#                            port_args=[controller_launcher_listen,
#                                       controller_listen],
#                            binary="/usr/local/bin/longhorn-engine-launcher")
#         assert(c["name"] == name)
#         assert(c["state"] == PROC_STATE_RUNNING)
#
#         found = False
#         for i in range(RETRY_COUNTS):
#             if os.path.exists(os.path.join("/dev/longhorn/", volname)):
#                 found = True
#                 break
#             time.sleep(RETRY_INTERVAL)
#         assert found
#
#     ps = process_list()
#     assert(len(ps) == 4)
#
#     for i in range(2):
#         name = cname_base + str(i)
#         c = process_delete(name)
#         assert(c["name"] == name)
#
#     ps = process_list()
#     assert(len(ps) == 2)
#
#     for i in range(2):
#         name = name_base + str(i)
#         r = process_delete(name)
#         assert(r["name"] == name)
#
#         wait_for_process_deletion(name)
#
#     ps = process_list()
#     assert(len(ps) == 0)
