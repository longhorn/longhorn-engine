import pytest
import os
import tempfile

from common.constants import INSTANCE_MANAGER_REPLICA
from common.constants import INSTANCE_MANAGER_ENGINE
from common.constants import VOLUME_NAME
from common.constants import VOLUME_BACKING_NAME
from common.constants import VOLUME_NO_FRONTEND_NAME
from common.constants import ENGINE_NAME
from common.constants import ENGINE_BACKING_NAME
from common.constants import ENGINE_NO_FRONTEND_NAME
from common.constants import REPLICA_NAME
from common.constants import SIZE
from common.constants import FRONTEND_TGT_BLOCKDEV
from common.constants import FIXED_REPLICA_PATH1
from common.constants import FIXED_REPLICA_PATH2
from common.constants import BACKING_FILE_PATH1
from common.constants import BACKING_FILE_PATH2

from common.core import cleanup_replica
from common.core import cleanup_process
from common.core import create_replica_process
from common.core import create_engine_process
from common.core import cleanup_replica_dir
from common.core import get_process_address
from common.core import get_dev


from rpc.instance_manager.process_manager_client import ProcessManagerClient
from rpc.replica.replica_client import ReplicaClient
from rpc.controller.controller_client import ControllerClient


@pytest.fixture
def grpc_engine_manager(request, engine_manager_client):
    return engine_manager_client


@pytest.fixture
def grpc_controller(request, grpc_controller_client):
    return grpc_controller_client(ENGINE_NAME, VOLUME_NAME)


@pytest.fixture
def grpc_controller_no_frontend(request, grpc_controller_client):
    return grpc_controller_client(ENGINE_NO_FRONTEND_NAME,
                                  VOLUME_NO_FRONTEND_NAME,
                                  frontend="")


@pytest.fixture
def grpc_backing_controller(request, grpc_controller_client):
    return grpc_controller_client(ENGINE_BACKING_NAME,
                                  VOLUME_BACKING_NAME)


@pytest.fixture
def grpc_replica1(request, grpc_replica_client):
    return grpc_replica_client(REPLICA_NAME + "-1")


@pytest.fixture
def grpc_replica2(request, grpc_replica_client):
    return grpc_replica_client(REPLICA_NAME + "-2")


@pytest.fixture
def grpc_backing_replica1(request, grpc_replica_client):
    return grpc_replica_client(
        REPLICA_NAME + "-backing-1",
        args=["replica", tempfile.mkdtemp(),
              "--backing-file", BACKING_FILE_PATH1,
              "--size", str(SIZE)])


@pytest.fixture
def grpc_backing_replica2(request, grpc_replica_client):
    return grpc_replica_client(
        REPLICA_NAME + "-backing-2",
        args=["replica", tempfile.mkdtemp(),
              "--backing-file", BACKING_FILE_PATH2,
              "--size", str(SIZE)])


@pytest.fixture
def grpc_fixed_dir_replica1(request, grpc_replica_client):
    request.addfinalizer(lambda: cleanup_replica_dir(
        FIXED_REPLICA_PATH1))
    return grpc_replica_client(
        REPLICA_NAME + "-fixed-dir-1",
        args=["replica", FIXED_REPLICA_PATH1, "--size", str(SIZE)])


@pytest.fixture
def grpc_fixed_dir_replica2(request, grpc_replica_client):
    request.addfinalizer(lambda: cleanup_replica_dir(
        FIXED_REPLICA_PATH2))
    return grpc_replica_client(
        REPLICA_NAME + "-fixed-dir-2",
        args=["replica", FIXED_REPLICA_PATH2, "--size", str(SIZE)])


@pytest.fixture
def grpc_extra_replica1(request, grpc_replica_client):
    request.addfinalizer(lambda: cleanup_replica_dir(
        FIXED_REPLICA_PATH1))
    return grpc_replica_client(
        REPLICA_NAME + "-extra-1",
        args=["replica", FIXED_REPLICA_PATH1, "--size", str(SIZE)])


@pytest.fixture
def grpc_extra_replica2(request, grpc_replica_client):
    request.addfinalizer(lambda: cleanup_replica_dir(
        FIXED_REPLICA_PATH2))
    return grpc_replica_client(
        REPLICA_NAME + "-extra-2",
        args=["replica", FIXED_REPLICA_PATH2, "--size", str(SIZE)])


@pytest.fixture
def process_manager_client(request, address=INSTANCE_MANAGER_REPLICA):
    c = ProcessManagerClient(address)
    request.addfinalizer(lambda: cleanup_process(c))
    return c


@pytest.fixture
def grpc_replica_client(request, process_manager_client):

    def generate_grpc_replica_client(replica_name, args=[]):
        r = create_replica_process(process_manager_client,
                                   replica_name,
                                   args=args)

        listen = get_process_address(r)

        c = ReplicaClient(listen)
        grpc_replica_client.replica_client = cleanup_replica(c)
        return grpc_replica_client.replica_client

    yield generate_grpc_replica_client


@pytest.fixture
def engine_manager_client(request, address=INSTANCE_MANAGER_ENGINE):
    c = ProcessManagerClient(address)
    request.addfinalizer(lambda: cleanup_process(c))
    return c


@pytest.fixture
def grpc_controller_client(request, engine_manager_client):
    def generate_grpc_controller_client(engine_name,
                                        volume_name,
                                        frontend=FRONTEND_TGT_BLOCKDEV):

        e = create_engine_process(engine_manager_client,
                                  name=engine_name,
                                  volume_name=volume_name,
                                  frontend=frontend)

        grpc_controller_client.process_client = \
            ControllerClient(get_process_address(e))
        return grpc_controller_client.process_client

    yield generate_grpc_controller_client


@pytest.fixture
def backup_targets():
    env = dict(os.environ)
    assert env["BACKUPTARGETS"] != ""
    return env["BACKUPTARGETS"].split(",")


@pytest.fixture
def dev(request, grpc_replica_client, grpc_controller_client):
    grpc_replica1 = grpc_replica_client(REPLICA_NAME + "-1")
    grpc_replica2 = grpc_replica_client(REPLICA_NAME + "-2")
    grpc_controller = grpc_controller_client(ENGINE_NAME, VOLUME_NAME)

    return get_dev(grpc_replica1, grpc_replica2, grpc_controller)
