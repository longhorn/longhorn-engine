import pytest

from core.common import cleanup_process
from core.common import cleanup_replica
from core.common import cleanup_engine_process
from core.common import create_engine_process
from core.common import create_replica_process
from core.common import REPLICA_NAME
from core.common import REPLICA_2_NAME
from core.common import get_replica_address
from core.common import ReplicaClient


from core.common import INSTANCE_MANAGER

from rpc.controller.controller_client import ControllerClient
from rpc.instance_manager.engine_manager_client import EngineManagerClient
from rpc.instance_manager.process_manager_client import ProcessManagerClient


@pytest.fixture
def process_manager_client(request, address=INSTANCE_MANAGER):
    c = ProcessManagerClient(address)
    request.addfinalizer(lambda: cleanup_process(c))
    return c


@pytest.fixture
def engine_manager_client(request, address=INSTANCE_MANAGER):
    c = EngineManagerClient(address)
    request.addfinalizer(lambda: cleanup_engine_process(c))
    return c


@pytest.fixture
def grpc_controller_client(request, engine_manager_client):
    e = create_engine_process(engine_manager_client)
    return ControllerClient(e.spec.listen)


@pytest.fixture
def grpc_replica_client(process_manager_client):
    r = create_replica_process(process_manager_client, REPLICA_NAME)

    listen = get_replica_address(r)
    c = ReplicaClient(listen)
    return cleanup_replica(c)


@pytest.fixture
def grpc_replica_client2(process_manager_client):
    r = create_replica_process(process_manager_client, REPLICA_2_NAME)

    listen = get_replica_address(r)
    c = ReplicaClient(listen)
    return cleanup_replica(c)
