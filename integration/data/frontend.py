import base64
import os
from os import path
import stat
import mmap
import directio

import cattle


LONGHORN_DEV_DIR = '/dev/longhorn'

PAGE_SIZE = 512


def readat_direct(dev, offset, length):
    pg = offset / PAGE_SIZE
    in_page_offset = offset % PAGE_SIZE
    # either read less than a page, or whole pages
    if in_page_offset != 0:
        assert pg == (offset + length - 1) / PAGE_SIZE
        to_read = PAGE_SIZE
    else:
        assert length % PAGE_SIZE == 0
        to_read = length
    pg_offset = pg * PAGE_SIZE

    f = os.open(dev, os.O_DIRECT | os.O_RDONLY)
    try:
        os.lseek(f, pg_offset, os.SEEK_SET)
        ret = directio.read(f, to_read)
    finally:
        os.close(f)
    return ret[in_page_offset: in_page_offset + length]


def writeat_direct(dev, offset, data):
    pg = offset / PAGE_SIZE
    # don't support across page write
    assert pg == (offset + len(data) - 1) / PAGE_SIZE
    pg_offset = pg * PAGE_SIZE

    f = os.open(dev, os.O_DIRECT | os.O_RDWR)
    m = mmap.mmap(-1, PAGE_SIZE)
    try:
        os.lseek(f, pg_offset, os.SEEK_SET)
        pg_data = readat_direct(dev, pg_offset, PAGE_SIZE)
        m.write(pg_data)
        m.seek(offset % PAGE_SIZE)
        m.write(data)
        ret = directio.write(f, m)
    finally:
        m.close()
        os.close(f)
    return ret


class restdev:

    def __init__(self, volume):
        url = 'http://localhost:9414/v1/schemas'
        c = cattle.from_env(url=url)
        dev = c.list_volume()[0]
        assert dev.name == volume
        self.dev = dev

    def readat(self, offset, length):
        try:
            data = self.dev.readat(offset=offset, length=length)["data"]
        except cattle.ApiError as e:
            if 'EOF' in str(e):
                return []
            raise e
        return base64.decodestring(data)

    def writeat(self, offset, data):
        l = len(data)
        encoded_data = base64.encodestring(data)
        try:
            ret = self.dev.writeat(offset=offset, length=l, data=encoded_data)
        except cattle.ApiError as e:
            if 'EOF' in str(e):
                raise IOError('No space left on the disk')
            raise e
        return ret

    def ready(self):
        return True


class blockdev:

    def __init__(self, volume):
        self.dev = path.join(LONGHORN_DEV_DIR, volume)

    def readat(self, offset, length):
        with open(self.dev, 'r') as f:
            f.seek(offset)
            ret = f.read(length)
        return ret
        # return readat_direct(self.dev, offset, length)

    def writeat(self, offset, data):
        return writeat_direct(self.dev, offset, data)

    def ready(self):
        if not os.path.exists(self.dev):
            return False
        mode = os.stat(self.dev).st_mode
        if not stat.S_ISBLK(mode):
            return False
        return True
