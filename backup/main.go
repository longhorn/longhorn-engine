package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/rancher/backupstore"

	"github.com/longhorn/longhorn-engine/replica"
	"github.com/longhorn/longhorn-engine/util"
)

var (
	VERSION = "0.0.0"
	log     = logrus.WithFields(logrus.Fields{"pkg": "backup"})

	backupCreateCmd = cli.Command{
		Name:  "create",
		Usage: "create a backup in backupstore: create <snapshot>",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "dest",
				Usage: "destination of backup if driver supports, would be url like s3://bucket@region/path/ or vfs:///path/",
			},
			cli.StringFlag{
				Name:  "volume",
				Usage: "volume name",
			},
			cli.StringSliceFlag{
				Name:  "label",
				Usage: "specify labels for backup, in the format of `--label key1=value1 --label key2=value2`",
			},
		},
		Action: cmdBackupCreate,
	}

	backupRestoreCmd = cli.Command{
		Name:  "restore",
		Usage: "restore a backup to file: restore <backup>",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "to",
				Usage: "destination file of restoring, will be created if not exists",
			},
			cli.BoolFlag{
				Name:  "incrementally, I",
				Usage: "Whether do incremental restore",
			},
			cli.StringFlag{
				Name:  "last-restored",
				Usage: "last restored backup name, the backup should exist in the backupstore",
			},
		},
		Action: cmdBackupRestore,
	}
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
	app.Commands = []cli.Command{
		backupCreateCmd,
		backupRestoreCmd,
	}
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

func cmdBackupCreate(c *cli.Context) {
	if err := doBackupCreate(c); err != nil {
		panic(err)
	}
}

func doBackupCreate(c *cli.Context) error {
	var (
		err         error
		backingFile *replica.BackingFile
		labelMap    map[string]string
	)

	if c.NArg() == 0 {
		return RequiredMissingError("snapshot name")
	}
	snapshotName := c.Args()[0]
	if snapshotName == "" {
		return RequiredMissingError("snapshot name")
	}

	destURL := c.String("dest")
	if destURL == "" {
		return RequiredMissingError("dest")
	}

	volumeName := c.String("volume")
	if volumeName == "" {
		return RequiredMissingError("volume")
	}
	if !util.ValidVolumeName(volumeName) {
		return fmt.Errorf("Invalid volume name %v for backup", volumeName)
	}
	labels := c.StringSlice("label")
	if labels != nil {
		labelMap, err = util.ParseLabels(labels)
		if err != nil {
			return errors.Wrap(err, "cannot parse backup labels")
		}
	}

	dir, err := os.Getwd()
	if err != nil {
		return err
	}

	volumeInfo, err := replica.ReadInfo(dir)
	if err != nil {
		return err
	}
	if volumeInfo.BackingFileName != "" {
		backingFileName := volumeInfo.BackingFileName
		if _, err := os.Stat(backingFileName); err != nil {
			return err
		}

		backingFile, err = openBackingFile(backingFileName)
		if err != nil {
			return err
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
	backupURL, err := backupstore.CreateDeltaBlockBackup(config)
	if err != nil {
		return err
	}
	fmt.Println(backupURL)
	return nil
}

func cmdBackupRestore(c *cli.Context) {
	if c.Bool("incrementally") {
		if err := doBackupRestoreIncrementally(c); err != nil {
			panic(err)
		}
	} else {
		if err := doBackupRestore(c); err != nil {
			panic(err)
		}
	}
}

func doBackupRestore(c *cli.Context) error {
	if c.NArg() == 0 {
		return RequiredMissingError("backup URL")
	}
	backupURL := c.Args()[0]
	if backupURL == "" {
		return RequiredMissingError("backup URL")
	}
	backupURL = util.UnescapeURL(backupURL)

	toFile := c.String("to")
	if toFile == "" {
		return RequiredMissingError("to")
	}

	if err := backupstore.RestoreDeltaBlockBackup(backupURL, toFile); err != nil {
		return err
	}

	if err := createNewSnapshotMetafile(toFile + ".meta"); err != nil {
		return err
	}
	return nil
}

func doBackupRestoreIncrementally(c *cli.Context) error {
	if c.NArg() == 0 {
		return RequiredMissingError("backup URL")
	}
	backupURL := c.Args()[0]
	if backupURL == "" {
		return RequiredMissingError("backup URL")
	}
	backupURL = util.UnescapeURL(backupURL)

	deltaFile := c.String("to")
	if deltaFile == "" {
		return RequiredMissingError("to")
	}

	lastRestored := c.String("last-restored")
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

	if err := backupstore.RestoreDeltaBlockBackupIncrementally(backupURL, deltaFile, lastRestored); err != nil {
		return err
	}

	return nil
}

func createNewSnapshotMetafile(file string) error {
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
