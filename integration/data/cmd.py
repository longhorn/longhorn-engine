import json
import time
import subprocess
from os import path


CONTROLLER = "http://localhost:9501"
CONTROLLER_NO_FRONTEND = "http://localhost:9801"

RETRY_COUNTS = 100


def _file(f):
    return path.join(_base(), '../../{}'.format(f))


def _base():
    return path.dirname(__file__)


def _bin():
    c = _file('bin/longhorn')
    assert path.exists(c)
    return c


def info(url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug', 'info']
    return json.loads(subprocess.check_output(cmd))


def snapshot_create(url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug', 'snapshot', 'create']
    return subprocess.check_output(cmd).strip()


def snapshot_rm(name, url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug', 'snapshot', 'rm', name]
    subprocess.check_call(cmd)


def snapshot_revert(name, url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug', 'snapshot', 'revert', name]
    subprocess.check_call(cmd)


def snapshot_ls(url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug', 'snapshot', 'ls']
    return subprocess.check_output(cmd)


def snapshot_info(url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug', 'snapshot', 'info']
    output = subprocess.check_output(cmd)
    return json.loads(output)


def snapshot_purge(url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug', 'snapshot', 'purge']
    return subprocess.check_call(cmd)


def backup_status(backupID, url=CONTROLLER):
    output = ""
    cmd = [_bin(), '--url', url, 'backup', 'status', backupID]
    for x in range(RETRY_COUNTS):
        backup = json.loads(subprocess.check_output(cmd).strip())
        if 'backupURL' in backup.keys():
            output = backup['backupURL']
            break
        elif 'error' in backup.keys():
            output = backup['error']
            break
        time.sleep(1)
    return output


def wait_for_restore_completion(url):
    completed = 0
    rs = restore_status(url)
    for x in range(RETRY_COUNTS):
        time.sleep(3)
        completed = 0
        rs = restore_status(url)
        for status in rs.values():
            if 'progress' in status.keys() and status['progress'] == 100:
                completed += 1
            if 'error' in status.keys():
                assert status['error'] == ""
        if completed == len(rs):
            return
    assert completed == len(rs)


def backup_create(snapshot, dest, url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug',
           'backup', 'create', snapshot, '--dest', dest]
    return backup_status(subprocess.check_output(cmd).strip())


def backup_rm(backup, url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug', 'backup', 'rm', backup]
    return subprocess.check_call(cmd)


def backup_restore(backup, url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug', 'backup', 'restore', backup]
    subprocess.check_output(cmd).strip()
    return wait_for_restore_completion(url)


def backup_inspect(backup, url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug', 'backup', 'inspect', backup]
    return json.loads(subprocess.check_output(cmd))


def backup_volume_rm(name, dest, url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug', 'backup',
           'rm', '--volume', name, dest]
    return subprocess.check_call(cmd)


def backup_volume_list(name, dest, url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug', 'backup', 'ls',
           '--volume', name, '--volume-only', dest]
    return json.loads(subprocess.check_output(cmd))


def add_replica(url, engine_url=CONTROLLER):
    cmd = [_bin(), '--url', engine_url, '--debug', 'add', url]
    return subprocess.check_output(cmd).strip()


def restore_to_file(backup_url, backing_file='', output_file='', format='',
                    url=CONTROLLER):
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
    return subprocess.check_output(cmd)


def restore_inc(backup_url, last_restored, url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug', 'backup', 'restore',
           backup_url, '--incrementally', '--last-restored', last_restored]
    subprocess.check_output(cmd)
    return wait_for_restore_completion(url)


def sync_agent_server_reset(url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug', 'sync-agent-server-reset']
    return subprocess.check_output(cmd)


def restore_status(url=CONTROLLER):
    cmd = [_bin(), '--url', url, '--debug', 'backup', 'restore-status']
    return json.loads(subprocess.check_output(cmd))
