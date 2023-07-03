import grpc
import pytest
import subprocess

from common.core import (
    create_replica_process, create_engine_process, get_process_address,
    get_controller_version_detail, get_replica
)

from common.constants import (
    ENGINE_NAME, REPLICA_NAME, REPLICA_NAME_BASE, SIZE_STR, VOLUME_NAME,
    VOLUME_NAME_BASE
)

from core.test_cli import bin # NOQA

from rpc.controller.controller_client import ControllerClient
from rpc.replica.replica_client import ReplicaClient


def test_validation_fails_with_client(engine_manager_client,
                                      process_manager_client):
    engine = create_engine_process(engine_manager_client, name=ENGINE_NAME,
                                   volume_name=VOLUME_NAME)
    replica = create_replica_process(process_manager_client, name=REPLICA_NAME,
                                     volume_name=VOLUME_NAME)

    # CASE 1:
    # Our engine client is created with a different volume name than the
    # engine.
    # We cannot communicate with the engine.
    e_client = ControllerClient(get_process_address(engine), 'wrong',
                                ENGINE_NAME)
    with pytest.raises(grpc.RpcError) as e:
        get_controller_version_detail(e_client)  # Retries until open socket.
    assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION

    # CASE 2:
    # Our engine client is created with a different instance name than the
    # engine.
    # We cannot communicate with the engine.
    e_client = ControllerClient(get_process_address(engine), VOLUME_NAME,
                                'wrong')
    with pytest.raises(grpc.RpcError) as e:
        get_controller_version_detail(e_client)  # Retries until open socket.
    assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION

    # CASE 3:
    # Our replica client is created with a different volume name than the
    # replica.
    # We cannot communicate with the replica.
    r_client = ReplicaClient(get_process_address(replica), 'wrong',
                             REPLICA_NAME)
    with pytest.raises(grpc.RpcError) as e:
        get_replica(r_client)  # Retries until open socket.
    assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION

    # CASE 4:
    # Our replica client is created with a different instance name than the
    # replica.
    # We cannot communicate with the replica.
    r_client = ReplicaClient(get_process_address(replica), VOLUME_NAME,
                             'wrong')
    with pytest.raises(grpc.RpcError) as e:
        get_replica(r_client)  # Retries until open socket.
    assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION

    # CASE 5:
    # Our engine client is right, so we can send it instructions, but it has
    # a different volume name than the replica it is trying to communicate
    # with.
    # The engine cannot communicate with the replica.
    volume_name = VOLUME_NAME_BASE + 'different'
    replica_name = REPLICA_NAME_BASE + 'different'
    replica = create_replica_process(process_manager_client, name=replica_name,
                                     volume_name=volume_name)
    e_client = ControllerClient(get_process_address(engine), VOLUME_NAME,
                                ENGINE_NAME)
    get_controller_version_detail(e_client)  # Retries until open socket.
    with pytest.raises(grpc.RpcError) as e:
        e_client.replica_create('tcp://' + get_process_address(replica))
    assert e.value.code() == grpc.StatusCode.UNKNOWN
    assert (f'incorrect volume name {VOLUME_NAME}; check replica address') \
        in e.value.details()


def test_validation_fails_with_cli(bin, engine_manager_client, # NOQA
                                   process_manager_client):
    # Run an engine with the expected identifying information that has a
    # backend with the expected identifying information.
    engine = create_engine_process(engine_manager_client, name=ENGINE_NAME,
                                   volume_name=VOLUME_NAME)
    e_address = get_process_address(engine)
    replica = create_replica_process(process_manager_client, name=REPLICA_NAME,
                                     volume_name=VOLUME_NAME)
    r_url = 'tcp://' + get_process_address(replica)
    cmd = [bin, '--debug', '--volume-name', VOLUME_NAME, '--url', e_address,
           'add-replica', '--size', SIZE_STR, '--current-size', SIZE_STR,
           r_url]
    subprocess.check_call(cmd)

    # Run a replica with different identifying information.
    v_other_name = VOLUME_NAME_BASE + 'other'
    r_other_name = REPLICA_NAME_BASE + 'other'
    replica_other = create_replica_process(process_manager_client,
                                           name=r_other_name,
                                           volume_name=v_other_name)
    r_other_address = 'tcp://' + get_process_address(replica_other)

    # CASE 1:
    # We pass the wrong volume name when executing an engine command.
    # We cannot communicate with the engine.
    cmd = [bin, '--debug', '--volume-name', 'wrong', '--url', e_address,
           'info']
    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.run(cmd, stderr=subprocess.PIPE, check=True)
    assert 'incorrect volume name wrong; check controller address' \
        in str(e.value.stderr)

    # CASE 2:
    # We pass the wrong engine instance name when executing an engine command.
    # We cannot communicate with the engine.
    cmd = [bin, '--debug', '--engine-instance-name', 'wrong', '--url',
           e_address, 'info']
    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.run(cmd, stderr=subprocess.PIPE, check=True)
    assert 'incorrect instance name wrong; check controller address' in \
        str(e.value.stderr)

    # CASE 3:
    # We pass the wrong volume name when executing a replica command.
    # We cannot communicate with the replica.
    cmd = [bin, '--debug', '--volume-name', VOLUME_NAME, '--url', e_address,
           'add-replica', '--size', SIZE_STR, '--current-size', SIZE_STR,
           r_other_address]
    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.run(cmd, stderr=subprocess.PIPE, check=True)
    assert f'incorrect volume name {VOLUME_NAME}; check replica address' \
        in str(e.value.stderr)

    # CASE 4:
    # We pass the wrong replica instance name when executing a replica command.
    # We cannot communicate with the replica.
    cmd = [bin, '--debug', '--url', e_address, 'add-replica', '--size',
           SIZE_STR, '--current-size', SIZE_STR, r_other_address,
           '--replica-instance-name', REPLICA_NAME]
    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.run(cmd, stderr=subprocess.PIPE, check=True)
    assert f'incorrect instance name {REPLICA_NAME}; check replica address' \
        in str(e.value.stderr)

    # CASE 5:
    # We ask an engine using one volume name to communicate with a replica
    # using a different volume name.
    # The engine cannot communicate with the replica.
    cmd = [bin, '--debug', '--url', e_address, 'add-replica', '--size',
           SIZE_STR, '--current-size', SIZE_STR, r_other_address]
    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.run(cmd, stderr=subprocess.PIPE, check=True)
    assert f'incorrect volume name {VOLUME_NAME}; check replica address' \
        in str(e.value.stderr)
