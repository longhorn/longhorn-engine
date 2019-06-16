import tempfile
import time
import os.path

import cmd
from cmd import process_create, process_delete
from cmd import process_get, process_list

PROC_STATE_RUNNING = "running"
PROC_STATE_STOPPED = "stopped"
PROC_STATE_ERROR = "error"

RETRY_INTERVAL = 1
RETRY_COUNTS = 30

SIZE = 4 * 1024 * 1024


def wait_for_process_deletion(name):
    deleted = False
    for i in range(RETRY_COUNTS):
        rs = process_list()
        if name not in rs:
            deleted = True
            break
        time.sleep(1)
    assert deleted


def test_start_stop_replicas():
    rs = process_list()
    assert(len(rs) == 0)

    listen = "--listen,localhost:"
    port_count = 15
    name_base = "replica"

    for i in range(10):
        tmp_dir = tempfile.mkdtemp()
        name = name_base + str(i)
        r = process_create(name, ["replica", tmp_dir, "--size", str(SIZE)],
                           port_count=port_count, port_args=[listen])
        assert(r["name"] == name)
        assert(r["state"] == PROC_STATE_RUNNING)

        r = process_get(name)
        assert(r["name"] == name)
        assert(r["state"] == PROC_STATE_RUNNING)

        rs = process_list()
        assert(len(rs) == i+1)
        assert(name in rs)
        assert(rs[name]["name"] == name)
        assert(rs[name]["state"] == PROC_STATE_RUNNING)

    for i in range(10):
        rs = process_list()
        assert(len(rs) == 10 - i)

        name = name_base + str(i)
        r = process_delete(name)
        assert(r["name"] == name)

        wait_for_process_deletion(name)

        rs = process_list()
        assert(len(rs) == 9 - i)

    rs = process_list()
    assert(len(rs) == 0)


def test_one_volume():
    rs = process_list()
    assert(len(rs) == 0)

    listen = "--listen,localhost:"
    port_count = 15
    name_base = "replica"

    for i in range(3):
        tmp_dir = tempfile.mkdtemp()
        name = name_base + str(i)
        r = process_create(name, ["replica", tmp_dir, "--size", str(SIZE)],
                           port_count=port_count, port_args=[listen])
        assert(r["name"] == name)
        assert(r["state"] == PROC_STATE_RUNNING)

        r = process_get(name)
        assert(r["name"] == name)
        assert(r["state"] == PROC_STATE_RUNNING)

        rs = process_list()
        assert(len(rs) == i+1)
        assert(name in rs)
        assert(rs[name]["name"] == name)
        assert(rs[name]["state"] == PROC_STATE_RUNNING)

    ps = process_list()
    replica_args = []
    for r in ps:
        replica_args = replica_args + \
            ["--replica", "tcp://localhost:"+str(ps[r]["portStart"])]

    controller_name = "controller"
    controller_launcher_listen = "--launcher-listen,localhost:"
    controller_listen = "--listen,localhost:"
    controller_port_count = 2
    volume_name = "testvol"

    c = process_create(controller_name,
                       ["start", volume_name,
                        "--longhorn-binary", cmd.LONGHORN_BINARY,
                        "--size", str(SIZE),
                        "--frontend", "tgt-blockdev"] + replica_args,
                       port_count=controller_port_count,
                       port_args=[controller_launcher_listen,
                                  controller_listen],
                       binary="/usr/local/bin/longhorn-engine-launcher")
    assert(c["name"] == controller_name)
    assert(c["state"] == PROC_STATE_RUNNING)

    found = False
    for i in range(RETRY_COUNTS):
        if os.path.exists(os.path.join("/dev/longhorn/", volume_name)):
            found = True
            break
        time.sleep(RETRY_INTERVAL)
    assert found

    ps = process_list()
    assert(len(ps) == 4)

    process_delete(controller_name)

    ps = process_list()
    assert(len(ps) == 3)

    for i in range(3):
        name = name_base + str(i)
        r = process_delete(name)
        assert(r["name"] == name)

        wait_for_process_deletion(name)

    ps = process_list()
    assert(len(ps) == 0)
