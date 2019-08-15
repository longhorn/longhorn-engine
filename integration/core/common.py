import sys
import os
import grpc
import tempfile
import time
import random

import pytest

# include directory intergration/rpc for module import
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.split(__file__)[0], "../rpc")
    )
)
from instance_manager.engine_manager_client import EngineManagerClient  # NOQA
from instance_manager.process_manager_client import ProcessManagerClient  # NOQA
from replica.replica_client import ReplicaClient  # NOQA
from controller.controller_client import ControllerClient  # NOQA


INSTANCE_MANAGER = "localhost:8500"

INSTANCE_MANAGER_TYPE_ENGINE = "engine"
INSTANCE_MANAGER_TYPE_REPLICA = "replica"

LONGHORN_BINARY = "./bin/longhorn"
BINARY_PATH_IN_TEST = "../bin/longhorn"

RETRY_INTERVAL = 0.5
RETRY_COUNTS = 30
RETRY_COUNTS2 = 100

SIZE = 4 * 1024 * 1024
SIZE_STR = str(SIZE)

VOLUME_NAME = "core-test-vol"
ENGINE_NAME = "core-test-vol-engine"
REPLICA_NAME = "core-test-vol-replica"
REPLICA_2_NAME = "core-test-vol-replica_2"

PROC_STATE_STARTING = "starting"
PROC_STATE_RUNNING = "running"
PROC_STATE_STOPPING = "stopping"
PROC_STATE_STOPPED = "stopped"
PROC_STATE_ERROR = "error"

FRONTEND_TGT_BLOCKDEV = "tgt-blockdev"


@pytest.fixture
def process_manager_client(request, address=INSTANCE_MANAGER):
    c = ProcessManagerClient(address)
    request.addfinalizer(lambda: cleanup_process(c))
    return c


def cleanup_process(pm_client):
    cleanup_engine_process(EngineManagerClient(pm_client.address))
    for name in pm_client.process_list():
        try:
            pm_client.process_delete(name)
        except grpc.RpcError as e:
            if 'cannot find process' not in e.details():
                raise e
    for i in range(RETRY_COUNTS):
        ps = pm_client.process_list()
        if len(ps) == 0:
            break
        time.sleep(RETRY_INTERVAL)

    ps = pm_client.process_list()
    assert len(ps) == 0
    return pm_client


@pytest.fixture
def engine_manager_client(request, address=INSTANCE_MANAGER):
    c = EngineManagerClient(address)
    request.addfinalizer(lambda: cleanup_engine_process(c))
    return c


def cleanup_engine_process(em_client):
    for _, engine in em_client.engine_list().iteritems():
        try:
            em_client.engine_delete(engine.spec.name)
        except grpc.RpcError as e:
            if 'cannot find engine' not in e.details():
                raise e
    for i in range(RETRY_COUNTS):
        es = em_client.engine_list()
        if len(es) == 0:
            break
        time.sleep(RETRY_INTERVAL)

    es = em_client.engine_list()
    assert len(es) == 0
    return em_client


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


def create_replica_process(client, name, replica_dir="",
                           binary=LONGHORN_BINARY,
                           size=SIZE, port_count=15,
                           port_args=["--listen,localhost:"]):
    if not replica_dir:
        replica_dir = tempfile.mkdtemp()
    client.process_create(
        name=name, binary=binary,
        args=["replica", replica_dir, "--size", str(size)],
        port_count=port_count, port_args=port_args)
    wait_for_process_running(client, name,
                             INSTANCE_MANAGER_TYPE_REPLICA)

    return client.process_get(name)


def create_engine_process(client, name=ENGINE_NAME,
                          volume_name=VOLUME_NAME,
                          binary=LONGHORN_BINARY,
                          listen="", listen_ip="localhost",
                          size=SIZE, frontend="tgt-blockdev",
                          replicas=[], backends=["file"]):
    client.engine_create(
        name=name, volume_name=volume_name,
        binary=binary, listen=listen, listen_ip=listen_ip,
        size=size, frontend=frontend, replicas=replicas,
        backends=backends)
    wait_for_process_running(client, name,
                             INSTANCE_MANAGER_TYPE_ENGINE)

    return client.engine_get(name)


def get_replica_address(r):
    return "localhost:" + str(r.status.port_start)


def get_backend_replica_url(address):
    return "tcp://"+address


@pytest.fixture
def grpc_controller_client(request):
    em_client = engine_manager_client(request)
    e = create_engine_process(em_client)

    return ControllerClient(e.spec.listen)


def cleanup_controller(grpc_client):
    try:
        v = grpc_client.volume_get()
    except grpc.RpcError as grpc_err:
        if "Socket closed" not in grpc_err.details() and \
                "failed to connect to all addresses" not in grpc_err.details():

            raise grpc_err
        return grpc_client

    if v.replicaCount != 0:
        grpc_client.volume_shutdown()
    for r in grpc_client.replica_list():
        grpc_client.replica_delete(r.address)
    return grpc_client


@pytest.fixture
def grpc_replica_client(request):
    pm_client = process_manager_client(request)
    r = create_replica_process(pm_client, REPLICA_NAME)

    listen = get_replica_address(r)
    c = ReplicaClient(listen)
    return cleanup_replica(c)


@pytest.fixture
def grpc_replica_client2(request):
    pm_client = process_manager_client(request)
    r = create_replica_process(pm_client, REPLICA_2_NAME)

    listen = get_replica_address(r)
    c = ReplicaClient(listen)
    return cleanup_replica(c)


def cleanup_replica(grpc_client):
    r = grpc_client.replica_get()
    if r.state == 'initial':
        return grpc_client
    if r.state == 'closed':
        grpc_client.replica_open()
    grpc_client.replica_delete()
    r = grpc_client.replica_reload()
    assert r.state == 'initial'
    return grpc_client


def random_str():
    return 'random-{0}-{1}'.format(random_num(), int(time.time()))


def random_num():
    return random.randint(0, 1000000)


def create_backend_file():
    name = random_str()
    fo = open(name, "w+")
    fo.truncate(SIZE)
    fo.close()
    return os.path.abspath(name)


def cleanup_backend_file(paths):
    for path in paths:
        if os.path.exists(path):
            os.remove(path)
