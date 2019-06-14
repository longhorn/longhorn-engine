package backup

import (
	// Involve backupstore drivers for registeration
	_ "github.com/longhorn/backupstore/nfs"
	_ "github.com/longhorn/backupstore/s3"
	_ "github.com/longhorn/backupstore/vfs"
)
