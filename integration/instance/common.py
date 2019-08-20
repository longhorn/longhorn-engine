import time
import sys
import os.path
import grpc

import pytest

# include directory intergration/rpc for module import
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.split(__file__)[0], "../rpc")
    )
)
from instance_manager.engine_manager_client import EngineManagerClient  # NOQA
from instance_manager.process_manager_client import ProcessManagerClient  # NOQA


INSTANCE_MANAGER = "localhost:8500"

LONGHORN_BINARY = "./bin/longhorn"
UPGRADE_LONGHORN_BINARY = "/opt/longhorn"

INSTANCE_MANAGER_TYPE_ENGINE = "engine"
INSTANCE_MANAGER_TYPE_REPLICA = "replica"

SIZE = 4 * 1024 * 1024

RETRY_INTERVAL = 1
RETRY_COUNTS = 30

PROC_STATE_STARTING = "starting"
PROC_STATE_RUNNING = "running"
PROC_STATE_STOPPING = "stopping"
PROC_STATE_STOPPED = "stopped"
PROC_STATE_ERROR = "error"

TEST_PREFIX = dict(os.environ)["TESTPREFIX"]
VOLUME_NAME_BASE = TEST_PREFIX + "instance-volume-"
ENGINE_NAME_BASE = TEST_PREFIX + "instance-engine-"
REPLICA_NAME_BASE = TEST_PREFIX + "instance-replica-"


@pytest.fixture()
def em_client(request, address=INSTANCE_MANAGER):
    c = EngineManagerClient(address)
    request.addfinalizer(lambda: cleanup_engine(c))
    return cleanup_engine(c)


def cleanup_engine(client):
    for _, engine in client.engine_list().iteritems():
        delete_engine_process(client, engine.spec.name)
    for i in range(RETRY_COUNTS):
        es = client.engine_list()
        if len(es) == 0:
            break
        time.sleep(RETRY_INTERVAL)

    es = client.engine_list()
    assert len(es) == 0
    return client


@pytest.fixture()
def pm_client(request, address=INSTANCE_MANAGER):
    c = ProcessManagerClient(address)
    request.addfinalizer(lambda: cleanup_process(c))
    return cleanup_process(c)


def cleanup_process(client):
    cleanup_engine(EngineManagerClient(client.address))
    for name in client.process_list():
        delete_replica_process(client, name)
    for i in range(RETRY_COUNTS):
        ps = client.process_list()
        if len(ps) == 0:
            break
        time.sleep(RETRY_INTERVAL)

    ps = client.process_list()
    assert len(ps) == 0
    return client


def wait_for_process_running(client, name, type):
    healthy = False
    for i in range(RETRY_COUNTS):
        if type == INSTANCE_MANAGER_TYPE_ENGINE:
            e = client.engine_get(name)
            state = e.status.process_status.state
        elif type == INSTANCE_MANAGER_TYPE_REPLICA:
            state = client.process_get(name).status.state
        else:
            # invalid type
            assert False

        if state == PROC_STATE_RUNNING:
            healthy = True
            break
        elif state != PROC_STATE_STARTING:
            # invalid state
            assert False
        time.sleep(RETRY_INTERVAL)
    assert healthy


def create_replica_process(client, name, dir,
                           binary=LONGHORN_BINARY,
                           size=SIZE, port_count=15,
                           port_args=["--listen,localhost:"]):
    client.process_create(
        name=name, binary=binary,
        args=["replica", dir, "--size", str(size)],
        port_count=port_count, port_args=port_args)
    wait_for_process_running(client, name,
                             INSTANCE_MANAGER_TYPE_REPLICA)

    return client.process_get(name)


def delete_engine_process(client, name):
    try:
        client.engine_delete(name)
    except grpc.RpcError as e:
        if 'cannot find engine' not in e.details():
            raise e


def delete_replica_process(client, name):
    try:
        client.process_delete(name)
    except grpc.RpcError as e:
        if 'cannot find process' not in e.details():
            raise e


def get_replica_address(r):
    return "localhost:" + str(r.status.port_start)


def wait_for_process_deletion(client, name):
    deleted = False
    for i in range(RETRY_COUNTS):
        rs = client.process_list()
        if name not in rs:
            deleted = True
            break
        time.sleep(RETRY_INTERVAL)
    assert deleted


def wait_for_engine_deletion(client, name):
    deleted = False
    for i in range(RETRY_COUNTS):
        es = client.engine_list()
        if name not in es:
            deleted = True
            break
        time.sleep(RETRY_INTERVAL)
    assert deleted


def create_engine_process(client, name, volume_name,
                          replicas, binary=LONGHORN_BINARY,
                          listen="", listen_ip="localhost",
                          size=SIZE, frontend="tgt-blockdev"):
    client.engine_create(
        name=name, volume_name=volume_name,
        binary=binary, listen=listen, listen_ip=listen_ip,
        size=size, frontend=frontend, replicas=replicas)
    wait_for_process_running(client, name,
                             INSTANCE_MANAGER_TYPE_ENGINE)

    return client.engine_get(name)


def get_dev_path(volume_name):
    return os.path.join("/dev/longhorn/", volume_name)


def check_dev_existence(volume_name):
    found = False
    for i in range(RETRY_COUNTS):
        if os.path.exists(get_dev_path(volume_name)):
            found = True
            break
        time.sleep(RETRY_INTERVAL)
    assert found


def wait_for_dev_deletion(volume_name):
    found = True
    for i in range(RETRY_COUNTS):
        if not os.path.exists(get_dev_path(volume_name)):
            found = False
            break
        time.sleep(RETRY_INTERVAL)
    assert not found
