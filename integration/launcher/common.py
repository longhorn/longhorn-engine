import time
import sys
import os.path

import pytest

# include directory intergration/rpc for module import
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.split(__file__)[0], "../rpc")
    )
)
from engine_manager.engine_manager_client import EngineManagerClient  # NOQA
from engine_manager.process_manager_client import ProcessManagerClient  # NOQA


LAUNCHER = "localhost:8500"
LONGHORN_BINARY = "./bin/longhorn"

SIZE = 4 * 1024 * 1024

RETRY_INTERVAL = 1
RETRY_COUNTS = 30


@pytest.fixture()
def em_client(request, address=LAUNCHER):
    c = EngineManagerClient(address)
    request.addfinalizer(lambda: cleanup_engine(c))
    return cleanup_engine(c)


def cleanup_engine(client):
    for name, _ in client.engine_list():
        client.engine_delete(name)
    return client


@pytest.fixture()
def pm_client(request, address=LAUNCHER):
    c = ProcessManagerClient(address)
    request.addfinalizer(lambda: cleanup_process(c))
    return cleanup_process(c)


def cleanup_process(client):
    cleanup_engine(EngineManagerClient(client.address))
    for name, _ in client.process_list():
        client.process_delete(name)
    return client


def create_replica_process(client, name, dir,
                           size=SIZE, port_count=15,
                           port_args=["--listen,localhost:"]):
    return client.process_create(
        name=name, binary=LONGHORN_BINARY,
        args=["replica", dir, "--size", str(size)],
        port_count=port_count, port_args=port_args)


def wait_for_process_deletion(client, name):
    deleted = False
    for i in range(RETRY_COUNTS):
        rs = client.process_list()
        if name not in rs:
            deleted = True
            break
        time.sleep(RETRY_INTERVAL)
    assert deleted


def create_engine_process(client, name, volume_name, replicas,
                          listen="", listen_addr="localhost",
                          size=SIZE, frontend="tgt-blockdev"):
    return client.engine_create(
        name=name, volume_name=volume_name, binary=LONGHORN_BINARY,
        listen=listen, listen_addr=listen_addr, size=size,
        frontend=frontend, replicas=replicas)


def check_engine_existence(volume_name):
    found = False
    for i in range(RETRY_COUNTS):
        if os.path.exists(os.path.join("/dev/longhorn/", volume_name)):
            found = True
            break
        time.sleep(RETRY_INTERVAL)
    assert found
