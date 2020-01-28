import pytest

from common.core import (
    cleanup_process,
)

from common.constants import (
    INSTANCE_MANAGER_REPLICA, INSTANCE_MANAGER_ENGINE,
)

from rpc.instance_manager.process_manager_client import ProcessManagerClient


@pytest.fixture()
def em_client(request, address=INSTANCE_MANAGER_ENGINE):
    c = ProcessManagerClient(address)
    request.addfinalizer(lambda: cleanup_process(c))
    return c


@pytest.fixture()
def pm_client(request, address=INSTANCE_MANAGER_REPLICA):
    c = ProcessManagerClient(address)
    request.addfinalizer(lambda: cleanup_process(c))
    return c

