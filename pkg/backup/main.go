package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"

	"github.com/longhorn/longhorn-engine/pkg/backingfile"
	"github.com/longhorn/longhorn-engine/pkg/replica"
	"github.com/longhorn/longhorn-engine/pkg/util"
)

var (
	VERSION = "0.0.0"
	log     = logrus.WithFields(logrus.Fields{"pkg": "backup"})
)

type CreateBackupParameters struct {
	BackupName           string
	VolumeName           string
	SnapshotName         string
	DestURL              string
	BackingImageName     string
	BackingImageChecksum string
	CompressionMethod    string
	ConcurrentLimit      int32
	Labels               []string
}

type ErrorResponse struct {
	Error string
}

func ResponseLogAndError(v interface{}) {
	if e, ok := v.(*logrus.Entry); ok {
		e.Error(e.Message)
		fmt.Println(e.Message)
	} else {
		e, isErr := v.(error)
		_, isRuntimeErr := e.(runtime.Error)
		if isErr && !isRuntimeErr {
			logrus.Errorf(fmt.Sprint(e))
			fmt.Println(fmt.Sprint(e))
		} else {
			logrus.Errorf("Caught FATAL error: %s", v)
			debug.PrintStack()
			fmt.Println("Caught FATAL error: ", v)
		}
	}
}

// ResponseOutput would generate a JSON format byte array of object for output
func ResponseOutput(v interface{}) ([]byte, error) {
	j, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return nil, err
	}
	return j, nil
}

func RequiredMissingError(name string) error {
	return fmt.Errorf("cannot find valid required parameter: %v", name)
}

func DoBackupInit(params *CreateBackupParameters) (*replica.BackupStatus, *backupstore.DeltaBackupConfig, error) {
	log.Infof("Initializing backup %v for volume %v snapshot %v", params.BackupName, params.VolumeName, params.SnapshotName)

	var err error

	if params.VolumeName == "" || params.SnapshotName == "" || params.DestURL == "" {
		return nil, nil, fmt.Errorf("missing input parameter")
	}

	if !util.ValidVolumeName(params.VolumeName) {
		return nil, nil, fmt.Errorf("invalid volume name %v for backup %v", params.VolumeName, params.BackupName)
	}

	var labelMap map[string]string
	if params.Labels != nil {
		labelMap, err = util.ParseLabels(params.Labels)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "cannot parse backup labels for backup %v", params.BackupName)
		}
	}

	volumeInfo, err := getVolumeInfoFromVolumeMeta()
	if err != nil {
		return nil, nil, err
	}

	backingFile, err := openBackingFile(volumeInfo.BackingFilePath)
	if err != nil {
		return nil, nil, err
	}

	backup := replica.NewBackup(params.BackupName, params.VolumeName, params.SnapshotName, backingFile)

	volume := &backupstore.Volume{
		Name:                 params.VolumeName,
		Size:                 volumeInfo.Size,
		Labels:               labelMap,
		BackingImageName:     params.BackingImageName,
		BackingImageChecksum: params.BackingImageChecksum,
		CompressionMethod:    params.CompressionMethod,
		CreatedTime:          util.Now(),
	}

	snapshot := &backupstore.Snapshot{
		Name:        params.SnapshotName,
		CreatedTime: util.Now(),
	}

	config := &backupstore.DeltaBackupConfig{
		BackupName:      params.BackupName,
		ConcurrentLimit: params.ConcurrentLimit,
		Volume:          volume,
		Snapshot:        snapshot,
		DestURL:         params.DestURL,
		DeltaOps:        backup,
		Labels:          labelMap,
	}
	return backup, config, nil
}

func DoBackupCreate(replicaBackup *replica.BackupStatus, config *backupstore.DeltaBackupConfig) error {
	log.Infof("Start creating backup %v", replicaBackup.Name)

	isIncremental, err := backupstore.CreateDeltaBlockBackup(replicaBackup.Name, config)
	if err != nil {
		return err
	}

	replicaBackup.IsIncremental = isIncremental
	return nil
}

func getVolumeInfoFromVolumeMeta() (replica.Info, error) {
	dir, err := os.Getwd()
	if err != nil {
		return replica.Info{}, err
	}

	return replica.ReadInfo(dir)
}

func openBackingFile(backingFilePath string) (*backingfile.BackingFile, error) {
	if backingFilePath == "" {
		return nil, nil
	}

	if _, err := os.Stat(backingFilePath); err != nil {
		return nil, err
	}

	return backingfile.OpenBackingFile(backingFilePath)
}

func DoBackupRestore(backupURL string, toFile string, concurrentLimit int, restoreObj *replica.RestoreStatus) error {
	backupURL = util.UnescapeURL(backupURL)

	log.Infof("Start restoring from %v into snapshot %v", backupURL, toFile)

	return backupstore.RestoreDeltaBlockBackup(&backupstore.DeltaRestoreConfig{
		BackupURL:       backupURL,
		DeltaOps:        restoreObj,
		Filename:        toFile,
		ConcurrentLimit: int32(concurrentLimit),
	})
}

func DoBackupRestoreIncrementally(url string, deltaFile string, lastRestored string, concurrentLimit int, restoreObj *replica.RestoreStatus) error {
	backupURL := util.UnescapeURL(url)

	log.Infof("Start incremental restoring from %v into delta file %v", backupURL, deltaFile)

	return backupstore.RestoreDeltaBlockBackupIncrementally(&backupstore.DeltaRestoreConfig{
		BackupURL:       backupURL,
		DeltaOps:        restoreObj,
		LastBackupName:  lastRestored,
		Filename:        deltaFile,
		ConcurrentLimit: int32(concurrentLimit),
	})
}

func CreateNewSnapshotMetafile(file string) error {
	f, err := os.Create(file + ".tmp")
	if err != nil {
		return err
	}
	defer f.Close()

	content := "{\"Parent\":\"\"}\n"
	if _, err := f.Write([]byte(content)); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(file+".tmp", file)
}
