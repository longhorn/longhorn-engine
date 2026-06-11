import errno
import os
from os import path
import stat
import mmap
import directio

from common.constants import (
    LONGHORN_SOCKET_DIR,
    LONGHORN_DEV_DIR,
    PAGE_SIZE,
)


def readat_direct(dev, offset, length):
    pg = offset // PAGE_SIZE
    pg_offset = pg * PAGE_SIZE
    in_page_offset = offset % PAGE_SIZE

    # either read less than a page, or the whole page
    if in_page_offset != 0:
        assert pg == (offset + length) // PAGE_SIZE
        to_read = PAGE_SIZE
    else:
        assert length % PAGE_SIZE == 0
        to_read = length

    f = os.open(dev, os.O_DIRECT | os.O_RDONLY)
    try:
        os.lseek(f, pg_offset, os.SEEK_SET)
        ret = directio.read(f, to_read)
    finally:
        os.close(f)

    return ret[in_page_offset : in_page_offset + length]


def writeat_direct(dev, offset, data):
    pg = offset // PAGE_SIZE
    pg_offset = pg * PAGE_SIZE

    # don't support across page write
    if len(data) == PAGE_SIZE:
        assert pg == (offset + len(data) - 1) // PAGE_SIZE
    else:
        assert pg == (offset + len(data)) // PAGE_SIZE

    f = os.open(dev, os.O_DIRECT | os.O_RDWR)
    m = mmap.mmap(-1, PAGE_SIZE)
    try:
        os.lseek(f, pg_offset, os.SEEK_SET)
        pg_data = readat_direct(dev, pg_offset, PAGE_SIZE)
        m.write(pg_data)
        m.seek(offset % PAGE_SIZE)
        m.write(data.encode("utf-8"))
        ret = directio.write(f, m[:PAGE_SIZE])
    finally:
        m.close()
        os.close(f)

    return ret


def get_socket_path(volume):
    return path.join(LONGHORN_SOCKET_DIR, "longhorn-" + volume + ".sock")


def get_block_device_path(volume):
    return path.join(LONGHORN_DEV_DIR, volume)


class blockdev:
    def __init__(self, volume):
        self.dev = get_block_device_path(volume)

    def readat(self, offset, length):
        assert self.ready()
        with open(self.dev, "r") as f:
            f.seek(offset)
            ret = f.read(length)
        return ret
        # return readat_direct(self.dev, offset, length)

    def writeat(self, offset, data):
        assert self.ready()
        return writeat_direct(self.dev, offset, data)

    def ready(self):
        if not os.path.exists(self.dev):
            return False
        mode = os.stat(self.dev).st_mode
        if not stat.S_ISBLK(mode):
            return False
        # The block device node can exist before the underlying iSCSI
        # device is able to service I/O. Probe it with a non-blocking
        # open + small read so we only report ready once it truly works.
        fd = None
        try:
            fd = os.open(self.dev, os.O_RDONLY | os.O_NONBLOCK)
            os.pread(fd, PAGE_SIZE, 0)
        except OSError as e:
            if e.errno in (errno.ENXIO, errno.ENODEV, errno.ENOENT, errno.EIO,
            errno.EAGAIN, errno.EWOULDBLOCK, errno.EBUSY):
                return False
            raise
        finally:
            if fd is not None:
                os.close(fd)
        return True
