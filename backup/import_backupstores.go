package backup

import (
	// Involve backupstore drivers for registeration
	_ "github.com/yasker/backupstore/nfs"
	_ "github.com/yasker/backupstore/s3"
	_ "github.com/yasker/backupstore/vfs"
)
