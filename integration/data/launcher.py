import json
import subprocess
from os import path


LAUNCHER = "localhost:9510"
LAUNCHER_NO_FRONTEND = "localhost:9810"


def _bin():
    c = '/usr/local/bin/longhorn-engine-launcher'
    assert path.exists(c)
    return c


def launcher(url):
    return [_bin(), "--url", url]


def info(url=LAUNCHER):
    cmd = launcher(url) + ['info']
    return json.loads(subprocess.check_output(cmd))


def upgrade(binary, replicas, url=LAUNCHER):
    cmd = launcher(url) + ['upgrade', '--longhorn-binary', binary]
    for replica in replicas:
        cmd = cmd + ['--replica', replica]
    subprocess.check_output(cmd)


def start_engine_frontend(frontend, url=LAUNCHER):
    cmd = launcher(url) + ['engine-frontend-start', frontend]
    subprocess.check_output(cmd)


def shutdown_engine_frontend(url=LAUNCHER):
    cmd = launcher(url) + ['engine-frontend-shutdown']
    subprocess.check_output(cmd)
