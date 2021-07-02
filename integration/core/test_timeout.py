import time
import os
import subprocess
import pytest

from common.core import (  # NOQA
        cleanup_controller,
        cleanup_process,
        cleanup_replica,
        create_engine_process,
        create_replica_process,
        get_process_address,
        get_controller_version_detail,
        open_replica,
        verify_replica_mode
)

from rpc.controller.controller_client import ControllerClient  # NOQA


def _file(f):
    return os.path.join(_base(), '../../{}'.format(f))


def _base():
    return os.path.dirname(__file__)


@pytest.fixture(scope='session')
def bin():
    c = _file('bin/longhorn')
    assert os.path.exists(c)
    return c


def test_timeout(bin, grpc_controller_client):  # NOQA
    replicas = grpc_controller_client.replica_list()
    assert len(replicas) == 0

    cmd = [bin, 'replica', '--listen', '127.0.0.1:5001',
           '--size', '2g', '/tmp/vol']

    process = subprocess.Popen(cmd)

    # Wait until replica up
    time.sleep(1)

    grpc_controller_client.replica_create('tcp://127.0.0.1:5001', True, "RW")

    time.sleep(1)

    replicas = grpc_controller_client.replica_list()
    assert len(replicas) == 1

    # Stop replica process.
    process.send_signal(19)

    # Wait until a timeout occurs.
    time.sleep(12)

    verify_replica_mode(grpc_controller_client, 'tcp://127.0.0.1:5001', "ERR")

    process.send_signal(18)

    cleanup_controller(grpc_controller_client)
