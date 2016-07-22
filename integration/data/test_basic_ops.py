import random

import pytest

import common
from common import dev  # NOQA
from common import SIZE, read_dev, write_dev


def test_basic_rw(dev):  # NOQA
    for i in range(0, 10):
        offset = random.randint(0, SIZE - 256)
        length = random.randint(0, 256)
        data = common.random_string(length)
        common.verify_data(dev, offset, data)


# See also BUG: https://github.com/rancher/longhorn/issues/131
def test_beyond_boundary(dev):  # NOQA
    # check write at the boundary
    data = common.random_string(128)
    common.verify_data(dev, SIZE - 128, data)

    # out of bounds
    with pytest.raises(IOError) as err:
        write_dev(dev, SIZE, "1")
        assert 'No space left' in str(err.value)
    assert len(read_dev(dev, SIZE, 1)) == 0

    # normal writes to verify controller/replica survival
    for i in range(0, 10):
        offset = random.randint(0, SIZE - 256)
        length = random.randint(0, 256)
        data = common.random_string(length)
        common.verify_data(dev, offset, data)
