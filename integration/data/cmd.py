import json
import subprocess
from os import path


def _file(f):
    return path.join(_base(), '../../{}'.format(f))


def _base():
    return path.dirname(__file__)


def _bin():
    c = _file('bin/longhorn')
    assert path.exists(c)
    return c


def snapshot_create():
    cmd = [_bin(), '--debug', 'snapshot', 'create']
    return subprocess.check_output(cmd).strip()


def snapshot_rm(name):
    cmd = [_bin(), '--debug', 'snapshot', 'rm', name]
    subprocess.check_call(cmd)


def snapshot_revert(name):
    cmd = [_bin(), '--debug', 'snapshot', 'revert', name]
    subprocess.check_call(cmd)


def snapshot_ls():
    cmd = [_bin(), '--debug', 'snapshot', 'ls']
    return subprocess.check_output(cmd)


def snapshot_info():
    cmd = [_bin(), '--debug', 'snapshot', 'info']
    output = subprocess.check_output(cmd)
    return json.loads(output)


def snapshot_purge():
    cmd = [_bin(), '--debug', 'snapshot', 'purge']
    return subprocess.check_call(cmd)


def backup_create(snapshot, dest):
    cmd = [_bin(), '--debug', 'backup', 'create', snapshot, '--dest', dest]
    return subprocess.check_output(cmd).strip()


def backup_rm(backup):
    cmd = [_bin(), '--debug', 'backup', 'rm', backup]
    return subprocess.check_call(cmd)


def backup_restore(backup):
    cmd = [_bin(), '--debug', 'backup', 'restore', backup]
    return subprocess.check_output(cmd).strip()


def backup_inspect(backup):
    cmd = [_bin(), '--debug', 'backup', 'inspect', backup]
    return json.loads(subprocess.check_output(cmd))


def add_replica(url):
    cmd = [_bin(), '--debug', 'add', url]
    return subprocess.check_output(cmd).strip()
