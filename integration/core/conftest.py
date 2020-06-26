import pytest
import time

from common.core import cleanup_process
from common.core import cleanup_replica
from common.core import create_engine_process
from common.core import create_replica_process
from common.core import get_process_address

from common.constants import (
    REPLICA_NAME, REPLICA_2_NAME,
    INSTANCE_MANAGER_REPLICA, INSTANCE_MANAGER_ENGINE,
)


from rpc.controller.controller_client import ControllerClient
from rpc.replica.replica_client import ReplicaClient
from rpc.instance_manager.process_manager_client import ProcessManagerClient


@pytest.fixture
def process_manager_client(request, address=INSTANCE_MANAGER_REPLICA):
    c = ProcessManagerClient(address)
    request.addfinalizer(lambda: cleanup_process(c))
    return c


@pytest.fixture
def engine_manager_client(request, address=INSTANCE_MANAGER_ENGINE):
    c = ProcessManagerClient(address)
    request.addfinalizer(lambda: cleanup_process(c))
    return c


@pytest.fixture
def grpc_controller_client(request, engine_manager_client):
    e = create_engine_process(engine_manager_client)
    return ControllerClient(get_process_address(e))


@pytest.fixture
def grpc_replica_client(process_manager_client):
    time.sleep(3)
    r = create_replica_process(process_manager_client, REPLICA_NAME)

    listen = get_process_address(r)
    c = ReplicaClient(listen)
    return cleanup_replica(c)


@pytest.fixture
def grpc_replica_client2(process_manager_client):
    time.sleep(3)
    r = create_replica_process(process_manager_client, REPLICA_2_NAME)

    listen = get_process_address(r)
    c = ReplicaClient(listen)
    return cleanup_replica(c)
