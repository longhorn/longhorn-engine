import pytest
import subprocess

import launcher
import common
from common import dev  # NOQA
from common import PAGE_SIZE, SIZE  # NOQA
from common import controller, replica1, replica2, read_dev, write_dev  # NOQA


def test_upgrade(dev):  # NOQA
    offset = 0
    length = 128

    data = common.random_string(length)
    common.verify_data(dev, offset, data)

    # both set pointed to the same volume underlying
    replicas = [common.REPLICA1, common.REPLICA2]
    upgrade_replicas = [common.UPGRADE_REPLICA1, common.UPGRADE_REPLICA2]

    launcher.upgrade(common.LONGHORN_UPGRADE_BINARY, upgrade_replicas)
    common.verify_data(dev, offset, data)

    # cannot start with same binary
    with pytest.raises(subprocess.CalledProcessError):
        launcher.upgrade(common.LONGHORN_BINARY, replicas)
    common.verify_data(dev, offset, data)

    # cannot start with wrong replica, would trigger rollback
    with pytest.raises(subprocess.CalledProcessError):
        launcher.upgrade(common.LONGHORN_UPGRADE_BINARY, "random")
    common.verify_data(dev, offset, data)

    launcher.upgrade(common.LONGHORN_UPGRADE_BINARY, replicas)
    common.verify_data(dev, offset, data)
