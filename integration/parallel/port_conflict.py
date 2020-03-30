import grpc

from common.core import (
    create_replica_process,
    wait_for_process_running, wait_for_process_deletion,
)

from common.constants import (
    INSTANCE_MANAGER_REPLICA,
    REPLICA_NAME_BASE,
)

from rpc.instance_manager.process_manager_client import ProcessManagerClient  # NOQA


def restart_process(pm_client, name, dir):
    for i in range(100):
        create_replica_process(pm_client,
                               name=name, replica_dir=dir)
        wait_for_process_running(pm_client, name)

        # Send duplicate deletion calls
        try:
            pm_client.process_delete(name=name)
            pm_client.process_delete(name=name)
            pm_client.process_delete(name=name)
        except grpc.RpcError as e:
            if 'cannot find process' not in e.details():
                raise e

        wait_for_process_deletion(pm_client, name)


def test_process_restart_1():
    restart_process(
        ProcessManagerClient(INSTANCE_MANAGER_REPLICA),
        REPLICA_NAME_BASE + "1",
        "/tmp/replica-1"
    )


def test_process_restart_2():
    restart_process(
        ProcessManagerClient(INSTANCE_MANAGER_REPLICA),
        REPLICA_NAME_BASE + "2",
        "/tmp/replica-2"
    )
