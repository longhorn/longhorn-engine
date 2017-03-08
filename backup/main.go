package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"

	"github.com/rancher/longhorn/replica"
	"github.com/yasker/backupstore"
)

const (
	DRIVERNAME = "longhorn"

	BackupstoreBase = "longhorn-backupstore"
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
		},
		Action: cmdBackupRestore,
	}

	backupDeleteCmd = cli.Command{
		Name:   "delete",
		Usage:  "delete a backup in backupstore: delete <backup>",
		Action: cmdBackupDelete,
	}

	backupListCmd = cli.Command{
		Name:  "list",
		Usage: "list backups in backupstore: list <dest>",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "volume",
				Usage: "volume name",
			},
		},
		Action: cmdBackupList,
	}

	backupInspectCmd = cli.Command{
		Name:   "inspect",
		Usage:  "inspect a backup: inspect <backup>",
		Action: cmdBackupInspect,
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

	backupstore.SetBackupstoreBase(BackupstoreBase)

	app := cli.NewApp()
	app.Version = VERSION
	app.Commands = []cli.Command{
		backupCreateCmd,
		backupRestoreCmd,
		backupDeleteCmd,
		backupListCmd,
		backupInspectCmd,
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
		Driver:      DRIVERNAME,
		Size:        volumeInfo.Size,
		CreatedTime: Now(),
	}
	snapshot := &backupstore.Snapshot{
		Name:        snapshotName,
		CreatedTime: Now(),
	}

	log.Debugf("Starting backup for %v, snapshot %v, dest %v", volume, snapshot, destURL)
	backupURL, err := backupstore.CreateDeltaBlockBackup(volume, snapshot, destURL, replicaBackup)
	if err != nil {
		return err
	}
	fmt.Println(backupURL)
	return nil
}

func cmdBackupDelete(c *cli.Context) {
	if err := doBackupDelete(c); err != nil {
		panic(err)
	}
}

func doBackupDelete(c *cli.Context) error {
	if c.NArg() == 0 {
		return RequiredMissingError("backup URL")
	}
	backupURL := c.Args()[0]
	if backupURL == "" {
		return RequiredMissingError("backup URL")
	}
	backupURL = UnescapeURL(backupURL)

	if err := backupstore.DeleteDeltaBlockBackup(backupURL); err != nil {
		return err
	}
	return nil
}

func cmdBackupRestore(c *cli.Context) {
	if err := doBackupRestore(c); err != nil {
		panic(err)
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
	backupURL = UnescapeURL(backupURL)

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

func cmdBackupList(c *cli.Context) {
	if err := doBackupList(c); err != nil {
		panic(err)
	}
}

func doBackupList(c *cli.Context) error {
	var err error

	if c.NArg() == 0 {
		return RequiredMissingError("dest URL")
	}
	destURL := c.Args()[0]
	if destURL == "" {
		return RequiredMissingError("dest URL")
	}

	volumeName := c.String("volume")

	list, err := backupstore.List(volumeName, destURL, DRIVERNAME)
	if err != nil {
		return err
	}
	fmt.Println(list)
	return nil
}

func cmdBackupInspect(c *cli.Context) {
	if err := doBackupInspect(c); err != nil {
		panic(err)
	}
}

func doBackupInspect(c *cli.Context) error {
	var err error

	if c.NArg() == 0 {
		return RequiredMissingError("backup URL")
	}
	backupURL := c.Args()[0]
	if backupURL == "" {
		return RequiredMissingError("backup URL")
	}
	backupURL = UnescapeURL(backupURL)

	info, err := backupstore.GetBackupInfo(backupURL)
	if err != nil {
		return err
	}
	data, err := ResponseOutput(info)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func UnescapeURL(url string) string {
	// Deal with escape in url inputed from bash
	result := strings.Replace(url, "\\u0026", "&", 1)
	result = strings.Replace(result, "u0026", "&", 1)
	return result
}

func Now() string {
	return time.Now().UTC().Format(time.RFC3339)
}
