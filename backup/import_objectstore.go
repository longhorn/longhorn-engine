package backup

import (
	// Involve S3 objecstore drivers for registeration
	_ "github.com/yasker/backupstore/s3"
	// Involve VFS objectstore driver for registeration
	_ "github.com/yasker/backupstore/vfs"
)
