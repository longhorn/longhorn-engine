import json
import time
import subprocess
from os import path

from common.constants import RETRY_COUNTS, RETRY_INTERVAL


def _file(f):
    return path.join(_base(), '../../{}'.format(f))


def _base():
    return path.dirname(__file__)


def _bin():
    c = _file('bin/longhorn')
    assert path.exists(c)
    return c


def info_get(url):
    cmd = [_bin(), '--url', url, '--debug', 'info']
    return json.loads(subprocess.check_output(cmd, encoding='utf-8'))


def snapshot_create(url):
    cmd = [_bin(), '--url', url, '--debug', 'snapshot', 'create']
    return subprocess.check_output(cmd, encoding='utf-8').strip()


def snapshot_rm(url, name):
    cmd = [_bin(), '--url', url, '--debug', 'snapshot', 'rm', name]
    subprocess.check_call(cmd)


def snapshot_revert(url, name):
    cmd = [_bin(), '--url', url, '--debug', 'snapshot', 'revert', name]
    subprocess.check_call(cmd)


def snapshot_ls(url):
    cmd = [_bin(), '--url', url, '--debug', 'snapshot', 'ls']
    return subprocess.check_output(cmd, encoding='utf-8')


def snapshot_info(url):
    cmd = [_bin(), '--url', url, '--debug', 'snapshot', 'info']
    output = subprocess.check_output(cmd, encoding='utf-8')
    return json.loads(output)


def snapshot_purge(url):
    cmd = [_bin(), '--url', url, '--debug', 'snapshot', 'purge']
    return subprocess.check_call(cmd)


def snapshot_purge_status(url):
    cmd = [_bin(), '--url', url, '--debug', 'snapshot', 'purge-status']
    output = subprocess.check_output(cmd, encoding='utf-8')
    return json.loads(output)


def backup_status(url, backupID):
    output = ""
    cmd = [_bin(), '--url', url, 'backup', 'status', backupID]
    for x in range(RETRY_COUNTS):
        backup = json.loads(subprocess.
                            check_output(cmd, encoding='utf-8').strip())
        assert 'state' in backup.keys()
        if backup['state'] == "complete":
            assert 'backupURL' in backup.keys()
            assert backup['backupURL'] != ""
            assert 'progress' in backup.keys()
            assert backup['progress'] == 100
            output = backup['backupURL']
            break
        elif backup['state'] == "error":
            assert 'error' in backup.keys()
            assert backup['error'] != ""
            output = backup['error']
            break
        else:
            assert backup['state'] == "in_progress"
        time.sleep(RETRY_INTERVAL)
    return output


def backup_create(url, snapshot, dest):
    cmd = [_bin(), '--url', url, '--debug',
           'backup', 'create', snapshot, '--dest', dest]
    backup = json.loads(subprocess.check_output(cmd, encoding='utf-8').strip())
    assert "backupID" in backup.keys()
    assert "isIncremental" in backup.keys()
    return backup_status(url, backup["backupID"])


def backup_rm(url, backup):
    cmd = [_bin(), '--url', url, '--debug', 'backup', 'rm', backup]
    return subprocess.check_call(cmd)


def backup_restore(url, backup):
    cmd = [_bin(), '--url', url, '--debug', 'backup', 'restore', backup]
    return subprocess.check_output(cmd, encoding='utf-8').strip()


def backup_inspect(url, backup):
    cmd = [_bin(), '--url', url, '--debug', 'backup', 'inspect', backup]
    return json.loads(subprocess.check_output(cmd, encoding='utf-8'))


def backup_volume_rm(url, name, dest):
    cmd = [_bin(), '--url', url, '--debug', 'backup',
           'rm', '--volume', name, dest]
    return subprocess.check_call(cmd)


def backup_volume_list(url, name, dest):
    cmd = [_bin(), '--url', url, '--debug', 'backup', 'ls',
           '--volume', name, '--volume-only', dest]
    return json.loads(subprocess.check_output(cmd, encoding='utf-8'))


def add_replica(url, replica_url):
    cmd = [_bin(), '--url', url, '--debug', 'add', replica_url]
    return subprocess.check_output(cmd, encoding='utf-8').strip()


def replica_rebuild_status(url):
    cmd = [_bin(), '--url', url, '--debug', 'replica-rebuild-status']
    return json.loads(subprocess.check_output(cmd).strip())


def restore_to_file(url, backup_url,
                    backing_file='', output_file='', format=''):
    cmd = [_bin(), '--url', url, '--debug',
           'backup', 'restore-to-file', backup_url]
    if backing_file:
        cmd.append('--backing-file')
        cmd.append(backing_file)
    if output_file:
        cmd.append('--output-file')
        cmd.append(output_file)
    if format:
        cmd.append('--output-format')
        cmd.append(format)
    return subprocess.check_output(cmd, encoding='utf-8')


def restore_inc(url, backup_url, last_restored):
    cmd = [_bin(), '--url', url, '--debug', 'backup', 'restore',
           backup_url, '--incrementally', '--last-restored', last_restored]
    return subprocess.check_output(cmd, encoding='utf-8')


def sync_agent_server_reset(url):
    cmd = [_bin(), '--url', url, '--debug', 'sync-agent-server-reset']
    return subprocess.check_output(cmd, encoding='utf-8')


def restore_status(url):
    cmd = [_bin(), '--url', url, '--debug', 'backup', 'restore-status']
    return json.loads(subprocess.check_output(cmd, encoding='utf-8'))
