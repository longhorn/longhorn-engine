import os

INSTANCE_MANAGER_REPLICA = "localhost:8500"
INSTANCE_MANAGER_ENGINE = "localhost:8501"

INSTANCE_MANAGER_TYPE_ENGINE = "engine"
INSTANCE_MANAGER_TYPE_REPLICA = "replica"

LONGHORN_BINARY = "./bin/longhorn"
BINARY_PATH_IN_TEST = "../bin/longhorn"
LONGHORN_UPGRADE_BINARY = '/opt/longhorn'

LONGHORN_DEV_DIR = '/dev/longhorn'
LONGHORN_SOCKET_DIR = '/var/run'

RETRY_COUNTS = 100
RETRY_INTERVAL = 1

RETRY_COUNTS2 = 100

RETRY_INTERVAL_SHORT = 0.5
RETRY_COUNTS_SHORT = 20

SIZE = 4 * 1024 * 1024
SIZE_STR = str(SIZE)

EXPANDED_SIZE = 2 * SIZE
EXPANDED_SIZE_STR = str(EXPANDED_SIZE)

BLOCK_SIZE = 2 * 1024 * 1024
BLOCK_SIZE_STR = str(BLOCK_SIZE)  # 2M
PAGE_SIZE = 512

TEST_PREFIX = dict(os.environ)["TESTPREFIX"]

VOLUME_NAME = TEST_PREFIX + "volume"
VOLUME2_NAME = TEST_PREFIX + "volume-2"
VOLUME_BACKING_NAME = TEST_PREFIX + 'volume-backing'
VOLUME_NO_FRONTEND_NAME = TEST_PREFIX + 'volume-no-frontend'

ENGINE_NAME = TEST_PREFIX + "engine"
ENGINE2_NAME = TEST_PREFIX + "engine-2"
ENGINE_BACKING_NAME = TEST_PREFIX + 'engine-backing'
ENGINE_NO_FRONTEND_NAME = TEST_PREFIX + 'engine-no-frontend'

REPLICA_NAME = TEST_PREFIX + "replica-1"
REPLICA_2_NAME = TEST_PREFIX + "replica-2"

PROC_STATE_STARTING = "starting"
PROC_STATE_RUNNING = "running"
PROC_STATE_STOPPING = "stopping"
PROC_STATE_STOPPED = "stopped"
PROC_STATE_ERROR = "error"

FRONTEND_TGT_BLOCKDEV = "tgt-blockdev"

FIXED_REPLICA_PATH1 = '/tmp/replica_fixed_dir_1/'
FIXED_REPLICA_PATH2 = '/tmp/replica_fixed_dir_2/'

BACKUP_DIR = '/data/backupbucket'

BACKING_FILE_RAW = 'backing_file.raw'
BACKING_FILE_RAW_PATH1 = '/tmp/replica_backing_dir_1/' + BACKING_FILE_RAW
BACKING_FILE_RAW_PATH2 = '/tmp/replica_backing_dir_2/' + BACKING_FILE_RAW
BACKING_FILE_QCOW2 = 'backing_file.qcow2'
BACKING_FILE_QCOW2_PATH1 = '/tmp/replica_backing_dir_1/' + BACKING_FILE_QCOW2
BACKING_FILE_QCOW2_PATH2 = '/tmp/replica_backing_dir_2/' + BACKING_FILE_QCOW2

VFS_DIR = "/data/backupbucket/"

VOLUME_HEAD = 'volume-head'

VOLUME_NAME_BASE = TEST_PREFIX + "instance-volume-"
ENGINE_NAME_BASE = TEST_PREFIX + "instance-engine-"
REPLICA_NAME_BASE = TEST_PREFIX + "instance-replica-"

REPLICA_META_FILE_NAME = "volume.meta"
EXPANSION_DISK_TMP_META_NAME = \
    "volume-snap-expand-%d.img.meta.tmp" % EXPANDED_SIZE
EXPANSION_DISK_NAME = "volume-snap-expand-%d.img" % EXPANDED_SIZE

MESSAGE_TYPE_ERROR = "error"
