import tempfile
import time

from cmd import process_create, process_delete
from cmd import process_get, process_list

PROC_STATE_RUNNING = "running"
PROC_STATE_STOPPED = "stopped"
PROC_STATE_ERROR = "error"

RETRY_INTERVAL = 1
RETRY_COUNTS = 30


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

    tmp_dir = tempfile.mkdtemp()
    listen = "--listen,localhost:"
    port_count = 15
    name_base = "replica"

    for i in range(10):
        name = name_base + str(i)
        r = process_create(name, ["replica", tmp_dir],
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
