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


def test_start_stop_replica():
    rs = process_list()
    assert(len(rs) == 0)

    tmp_dir = tempfile.mkdtemp()
    listen1 = "localhost:10001"
    name = "replica1"
    r = process_create(name, ["replica", "--listen", listen1, tmp_dir])
    assert(r["name"] == name)
    assert(r["state"] == PROC_STATE_RUNNING)

    r = process_get(name)
    assert(r["name"] == name)
    assert(r["state"] == PROC_STATE_RUNNING)

    rs = process_list()
    assert(len(rs) == 1)
    assert(name in rs)
    assert(rs[name]["name"] == name)
    assert(rs[name]["state"] == PROC_STATE_RUNNING)

    r = process_delete(name)
    assert(r["name"] == name)

    wait_for_process_deletion(name)

    rs = process_list()
    assert(len(rs) == 0)
