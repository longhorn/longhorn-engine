package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/backupstore"

	"github.com/longhorn/longhorn-engine/replica"
	"github.com/longhorn/longhorn-engine/util"
)

var (
	VERSION = "0.0.0"
	log     = logrus.WithFields(logrus.Fields{"pkg": "backup"})
)

func cleanup() {
	if r := recover(); r != nil {
		ResponseLogAndError(r)
		os.Exit(1)
	}
}

func Main() {
	defer cleanup()

	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetOutput(os.Stderr)
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Cannot get running directory: %s", err)
	}
	log.Debugf("Currently running at %v, assume as volume dir", dir)

	app := cli.NewApp()
	app.Version = VERSION
	app.Commands = []cli.Command{}
	app.Run(os.Args)
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
	return fmt.Errorf("Cannot find valid required parameter: %v", name)
}

func DoBackupCreate(volumeName string, snapshotName string, destURL string, labels []string) (string, *replica.Backup, error) {
	var (
		err         error
		backingFile *replica.BackingFile
		labelMap    map[string]string
	)

	if volumeName == "" || snapshotName == "" || destURL == "" {
		return "", nil, fmt.Errorf("missing input parameter")
	}

	if !util.ValidVolumeName(volumeName) {
		return "", nil, fmt.Errorf("Invalid volume name %v for backup", volumeName)
	}

	if labels != nil {
		labelMap, err = util.ParseLabels(labels)
		if err != nil {
			return "", nil, fmt.Errorf("cannot parse backup labels")
		}
	}

	dir, err := os.Getwd()
	if err != nil {
		return "", nil, err
	}

	volumeInfo, err := replica.ReadInfo(dir)
	if err != nil {
		return "", nil, err
	}
	if volumeInfo.BackingFileName != "" {
		backingFileName := volumeInfo.BackingFileName
		if _, err := os.Stat(backingFileName); err != nil {
			return "", nil, err
		}

		backingFile, err = openBackingFile(backingFileName)
		if err != nil {
			return "", nil, err
		}
	}
	replicaBackup := replica.NewBackup(backingFile)

	volume := &backupstore.Volume{
		Name:        volumeName,
		Size:        volumeInfo.Size,
		CreatedTime: util.Now(),
	}
	snapshot := &backupstore.Snapshot{
		Name:        snapshotName,
		CreatedTime: util.Now(),
	}

	log.Debugf("Starting backup for %v, snapshot %v, dest %v", volume, snapshot, destURL)
	config := &backupstore.DeltaBackupConfig{
		Volume:   volume,
		Snapshot: snapshot,
		DestURL:  destURL,
		DeltaOps: replicaBackup,
		Labels:   labelMap,
	}

	backupID, err := backupstore.CreateDeltaBlockBackup(config)
	if err != nil {
		return "", nil, err
	}

	return backupID, replicaBackup, nil
}

func DoBackupRestore(backupURL string, restoreObj *replica.Restore, toFile string) error {
	if backupURL == "" {
		return RequiredMissingError("backup URL")
	}
	backupURL = util.UnescapeURL(backupURL)

	if toFile == "" {
		return RequiredMissingError("snapshot")
	}

	log.Debugf("Starting restore from %v into snapshot %v", backupURL, toFile)

	config := &backupstore.DeltaRestoreConfig{
		BackupURL: backupURL,
		DeltaOps:  restoreObj,
		Filename:  toFile,
	}

	err := backupstore.RestoreDeltaBlockBackup(config)
	if err != nil {
		return err
	}

	return nil
}

func DoBackupRestoreIncrementally(url string, deltaFile string, lastRestored string, restoreObj *replica.Restore) error {
	if url == "" {
		return RequiredMissingError("backup URL")
	}
	backupURL := util.UnescapeURL(url)

	if deltaFile == "" {
		return RequiredMissingError("delta file")
	}

	if lastRestored == "" {
		return RequiredMissingError("last-restored")
	}

	// check delta file
	if _, err := os.Stat(deltaFile); err == nil {
		logrus.Warnf("delta file %s for incremental restoring exists, will remove and re-create it", deltaFile)
		err = os.Remove(deltaFile)
		if err != nil {
			return err
		}
	}

	config := &backupstore.DeltaRestoreConfig{
		BackupURL:      backupURL,
		DeltaOps:       restoreObj,
		LastBackupName: lastRestored,
		Filename:       deltaFile,
	}

	if err := backupstore.RestoreDeltaBlockBackupIncrementally(config); err != nil {
		return err
	}

	return nil
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
