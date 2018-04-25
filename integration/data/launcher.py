import json
import subprocess
from os import path


def _bin():
    c = '/usr/local/bin/longhorn-engine-launcher'
    assert path.exists(c)
    return c


def info():
    cmd = [_bin(), 'info']
    return json.loads(subprocess.check_output(cmd))
