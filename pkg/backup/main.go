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

type BackupParameters struct {
	BackupName           string
	VolumeName           string
	SnapshotName         string
	DestURL              string
	BackingImageName     string
	BackingImageChecksum string
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

func DoBackupCreate(backupParams *BackupParameters) (string, *replica.BackupStatus, error) {
	var err error

	if backupParams.VolumeName == "" || backupParams.SnapshotName == "" || backupParams.DestURL == "" {
		return "", nil, fmt.Errorf("missing input parameter")
	}

	if !util.ValidVolumeName(backupParams.VolumeName) {
		return "", nil, fmt.Errorf("invalid volume name %v for backup %v", backupParams.VolumeName, backupParams.BackupName)
	}

	var labelMap map[string]string
	if backupParams.Labels != nil {
		labelMap, err = util.ParseLabels(backupParams.Labels)
		if err != nil {
			return "", nil, errors.Wrapf(err, "cannot parse backup labels for backup %v", backupParams.BackupName)
		}
	}

	volumeInfo, err := getVolumeInfoFromVolumeMeta()
	if err != nil {
		return "", nil, err
	}

	backingFile, err := openBackingFile(volumeInfo.BackingFilePath)
	if err != nil {
		return "", nil, err
	}

	replicaBackup := replica.NewBackup(backingFile)

	volume := &backupstore.Volume{
		Name:                 backupParams.VolumeName,
		Size:                 volumeInfo.Size,
		Labels:               labelMap,
		BackingImageName:     backupParams.BackingImageName,
		BackingImageChecksum: backupParams.BackingImageChecksum,
		CreatedTime:          util.Now(),
	}

	snapshot := &backupstore.Snapshot{
		Name:        backupParams.SnapshotName,
		CreatedTime: util.Now(),
	}

	log.Infof("Starting backup for %v, snapshot %v, dest %v", volume, snapshot, backupParams.DestURL)

	backupID, isIncremental, err := backupstore.CreateDeltaBlockBackup(&backupstore.DeltaBackupConfig{
		BackupName: backupParams.BackupName,
		Volume:     volume,
		Snapshot:   snapshot,
		DestURL:    backupParams.DestURL,
		DeltaOps:   replicaBackup,
		Labels:     labelMap,
	})
	if err != nil {
		return "", nil, err
	}

	replicaBackup.IsIncremental = isIncremental

	return backupID, replicaBackup, nil
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

func DoBackupRestore(backupURL string, toFile string, restoreObj *replica.RestoreStatus) error {
	backupURL = util.UnescapeURL(backupURL)

	log.Infof("Start restoring from %v into snapshot %v", backupURL, toFile)

	return backupstore.RestoreDeltaBlockBackup(&backupstore.DeltaRestoreConfig{
		BackupURL: backupURL,
		DeltaOps:  restoreObj,
		Filename:  toFile,
	})
}

func DoBackupRestoreIncrementally(url string, deltaFile string, lastRestored string, restoreObj *replica.RestoreStatus) error {
	backupURL := util.UnescapeURL(url)

	log.Infof("Start incremental restoring from %v into delta file %v", backupURL, deltaFile)

	return backupstore.RestoreDeltaBlockBackupIncrementally(&backupstore.DeltaRestoreConfig{
		BackupURL:      backupURL,
		DeltaOps:       restoreObj,
		LastBackupName: lastRestored,
		Filename:       deltaFile,
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
