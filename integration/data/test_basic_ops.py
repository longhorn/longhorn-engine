import random
from os import path

import pytest

import cmd
import common
from common import dev  # NOQA
from common import PAGE_SIZE, SIZE  # NOQA
from common import controller, replica1, replica2, read_dev, write_dev  # NOQA



def test_basic_rw(dev):  # NOQA
    for i in range(0, 10):
        base = random.randint(0, SIZE - PAGE_SIZE)
        offset = (base / PAGE_SIZE) * PAGE_SIZE
        length = base - offset
        if length == 0:
            length = 128
        data = common.random_string(length)
        common.verify_data(dev, offset, data)


# See also BUG: https://github.com/rancher/longhorn/issues/131
def test_beyond_boundary(dev):  # NOQA
    # check write at the boundary
    data = common.random_string(128)
    common.verify_data(dev, SIZE - 128, data)

    # out of bounds
    with pytest.raises(EnvironmentError) as err:
        write_dev(dev, SIZE, "1")
        assert 'No space left' in str(err.value)
    assert len(read_dev(dev, SIZE, 1)) == 0

    # normal writes to verify controller/replica survival
    test_basic_rw(dev)


def test_frontend_show(controller, replica1, replica2):  # NOQA
    common.open_replica(replica1)
    common.open_replica(replica2)

    replicas = controller.list_replica()
    assert len(replicas) == 0

    v = controller.list_volume()[0]
    v = v.start(replicas=[
        common.REPLICA1,
        common.REPLICA2
    ])

    assert v["endpoint"] == path.join(common.LONGHORN_DEV_DIR,
                                      common.VOLUME_NAME)

    info = cmd.info()
    assert info["name"] == common.VOLUME_NAME
    assert info["endpoint"] == v["endpoint"]
