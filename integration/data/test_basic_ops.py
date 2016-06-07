import random

import pytest
import cattle

import cmd
import common
from common import dev, SIZE, read_dev, write_dev


def test_basic_rw(dev):
    for i in range(0, 10):
        offset = random.randint(0, SIZE - 256)
        length = random.randint(0, 256)
        data = common.random_string(length)
        common.verify_data(dev, offset, data)


# TODO BUG: https://github.com/rancher/longhorn/issues/131
@pytest.mark.xfail(strict=True)
def test_beyond_bounary(dev):
    with pytest.raises(cattle.ApiError) as err:
        write_dev(dev, SIZE, "1")
        assert 'EOF' in str(err.value)
    with pytest.raises(cattle.ApiError) as err:
        r = read_dev(dev, SIZE, 1)
        assert 'EOF' in str(err.value)

    for i in range(0, 10):
        offset = random.randint(0, SIZE - 256)
        length = random.randint(0, 256)
        data = common.random_string(length)
        common.verify_data(dev, offset, data)
