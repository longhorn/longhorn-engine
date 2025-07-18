package logging

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

const (
<<<<<<< HEAD
	LogFieldVolume       = "volume"
	LogFieldVolumeDev    = "volume_dev"
	LogFieldVolumeName   = "volume_name"
	LogFieldOrigVolume   = "original_volume"
	LogFieldSnapshot     = "snapshot"
	LogFieldBackup       = "backup"
	LogFieldBackupType   = "backup_type"
	LogFieldLastBackup   = "last_backup"
	LogFieldLastSnapshot = "last_snapshot"
	LogEventBackupURL    = "backup_url"
	LogFieldDestURL      = "dest_url"
	LogFieldSourceURL    = "source_url"
	LogFieldKind         = "kind"
	LogFieldFilepath     = "filepath"
=======
	LogFieldVolume            = "volume"
	LogFieldDataEngine        = "data_engine"
	LogFieldSrcVolume         = "source_volume"
	LogFieldDstVolumeDev      = "destination_volume_dev"
	LogFieldSnapshot          = "snapshot"
	LogFieldLastSnapshot      = "last_snapshot"
	LogFieldBackup            = "backup"
	LogFieldBackupType        = "backup_type"
	LogFieldLastBackup        = "last_backup"
	LogFieldCompressionMethod = "compression_method"
	LogFieldBackupURL         = "backup_url"
	LogFieldDestURL           = "dest_url"
	LogFieldSourceURL         = "source_url"
	LogFieldKind              = "kind"
	LogFieldFilepath          = "filepath"
	LogFieldConcurrentLimit   = "concurrent_limit"
	LogFieldBackupBlockSize   = "backup_block_size"
>>>>>>> 3932ded (feat(backup): configurable bkup block size)

	LogFieldEvent        = "event"
	LogEventBackup       = "backup"
	LogEventList         = "list"
	LogEventRestore      = "restore"
	LogEventRestoreIncre = "restore_incrementally"
	LogEventCompare      = "compare"

	LogFieldReason    = "reason"
	LogReasonStart    = "start"
	LogReasonComplete = "complete"
	LogReasonFallback = "fallback"

	LogFieldObject    = "object"
	LogObjectBackup   = "backup"
	LogObjectSnapshot = "snapshot"
	LogObjectConfig   = "config"
)

// Error is a wrapper for a go error contains more details
type Error struct {
	entry *logrus.Entry
	error
}

// ErrorWithFields is a helper for searchable error fields output
func ErrorWithFields(pkg string, fields logrus.Fields, format string, v ...interface{}) Error {
	fields["pkg"] = pkg
	entry := logrus.WithFields(fields)
	entry.Message = fmt.Sprintf(format, v...)

	return Error{entry, fmt.Errorf(format, v...)}
}
