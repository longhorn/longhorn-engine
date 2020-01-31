import os
import hashlib


def file(f):
    return os.path.join(_base(), '../../{}'.format(f))


def _base():
    return os.path.dirname(__file__)


def read_file(file_path, offset, length):
    assert os.path.exists(file_path)
    f = open(file_path, 'r')
    f.seek(offset)
    data = f.read(length)
    f.close()
    return data


def checksum_data(data):
    return hashlib.sha512(data).hexdigest()
