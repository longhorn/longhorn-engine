import base64

import cattle


class restdev:

    def __init__(self, volume):
        url = 'http://localhost:9414/v1/schemas'
        c = cattle.from_env(url=url)
        dev = c.list_volume()[0]
        assert dev.name == volume
        self.dev = dev

    def readat(self, offset, length):
        data = self.dev.readat(offset=offset, length=length)["data"]
        return base64.decodestring(data)

    def writeat(self, offset, data):
        l = len(data)
        encoded_data = base64.encodestring(data)
        return self.dev.writeat(offset=offset, length=l, data=encoded_data)
