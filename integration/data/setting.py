INSTANCE_MANAGER = "localhost:8500"

INSTANCE_MANAGER_TYPE_ENGINE = "engine"
INSTANCE_MANAGER_TYPE_REPLICA = "replica"

LONGHORN_BINARY = './bin/longhorn'
LONGHORN_UPGRADE_BINARY = '/opt/longhorn'

LONGHORN_DEV_DIR = '/dev/longhorn'
LONGHORN_SOCKET_DIR = '/var/run'

FIXED_REPLICA_PATH1 = '/tmp/replica_fixed_dir_1/'
FIXED_REPLICA_PATH2 = '/tmp/replica_fixed_dir_2/'

BACKUP_DIR = '/data/backupbucket'
BACKUP_DEST = 'vfs://' + BACKUP_DIR

BACKING_FILE = 'backing_file.raw'
BACKING_FILE_QCOW = 'backing_file.qcow2'
BACKING_FILE_PATH1 = '/tmp/replica_backing_dir_1/' + BACKING_FILE_QCOW
BACKING_FILE_PATH2 = '/tmp/replica_backing_dir_2/' + BACKING_FILE_QCOW

VFS_DIR = "/data/backupbucket/"

VOLUME_NAME = 'test-volume'
VOLUME_BACKING_NAME = 'test-volume-backing'
VOLUME_NO_FRONTEND_NAME = 'test-volume-no-frontend'
VOLUME_HEAD = 'volume-head'

ENGINE_NAME = 'test-engine'
ENGINE_BACKING_NAME = 'test-engine-backing'
ENGINE_NO_FRONTEND_NAME = 'test-engine-no-frontend'

REPLICA_NAME = 'test-replica'

FRONTEND_TGT_BLOCKDEV = "tgt-blockdev"

RETRY_COUNTS = 100
RETRY_INTERVAL = 1

RETRY_INTERVAL2 = 0.5
RETRY_COUNTS2 = 20

SIZE = 4 * 1024 * 1024
SIZE_STR = str(SIZE)  # 4M
BLOCK_SIZE = 2 * 1024 * 1024
BLOCK_SIZE_STR = str(BLOCK_SIZE)  # 2M
PAGE_SIZE = 512

PROC_STATE_STARTING = "starting"
PROC_STATE_RUNNING = "running"
PROC_STATE_STOPPING = "stopping"
PROC_STATE_STOPPED = "stopped"
PROC_STATE_ERROR = "error"
