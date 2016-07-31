import base64
from os import path

import cattle


LONGHORN_DEV_DIR = '/dev/longhorn'


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


class fusedev:

    def __init__(self, volume):
        self.dev = path.join(LONGHORN_DEV_DIR, volume)

    def readat(self, offset, length):
        with open(self.dev, 'r') as f:
            f.seek(offset)
            ret = f.read(length)
        return ret

    def writeat(self, offset, data):
        with open(self.dev, 'r+b') as f:
            f.seek(offset)
            ret = f.write(data)
        return ret
