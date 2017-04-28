package backup

import (
	// Involve backupstore drivers for registeration
	_ "github.com/rancher/backupstore/nfs"
	_ "github.com/rancher/backupstore/s3"
	_ "github.com/rancher/backupstore/vfs"
)
