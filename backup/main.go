package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"

	"github.com/rancher/convoy/objectstore"
	"github.com/rancher/convoy/util"
	"github.com/rancher/longhorn/replica"
)

const (
	DRIVERNAME = "longhorn"
)

var (
	VERSION = "0.0.0"
	log     = logrus.WithFields(logrus.Fields{"pkg": "backup"})

	backupCreateCmd = cli.Command{
		Name:  "create",
		Usage: "create a backup in objectstore: create <snapshot>",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "dest",
				Usage: "destination of backup if driver supports, would be url like s3://bucket@region/path/ or vfs:///path/",
			},
			cli.StringFlag{
				Name:  "volume",
				Usage: "volume path, the base name will be used as volume name",
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
		Usage:  "delete a backup in objectstore: delete <backup>",
		Action: cmdBackupDelete,
	}

	backupListCmd = cli.Command{
		Name:  "list",
		Usage: "list backups in objectstore: list <dest>",
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

// ResponseLogAndError would log the error before call ResponseError()
func ResponseLogAndError(v interface{}) {
	if e, ok := v.(*logrus.Entry); ok {
		e.Error(e.Message)
		oldFormatter := e.Logger.Formatter
		logrus.SetFormatter(&logrus.JSONFormatter{})
		s, err := e.String()
		logrus.SetFormatter(oldFormatter)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		// Cosmetic since " would be escaped
		fmt.Println(strings.Replace(s, "\"", "'", -1))
	} else {
		e, isErr := v.(error)
		_, isRuntimeErr := e.(runtime.Error)
		if isErr && !isRuntimeErr {
			logrus.Errorf(fmt.Sprint(e))
			fmt.Println(fmt.Sprint(e))
		} else {
			logrus.Errorf("Caught FATAL error: %s", v)
			debug.PrintStack()
			fmt.Printf("Caught FATAL error: %s\n", v)
		}
	}
}

func cleanup() {
	if r := recover(); r != nil {
		ResponseLogAndError(r)
		os.Exit(1)
	}
}

func main() {
	defer cleanup()

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

func getName(c *cli.Context, key string, required bool) (string, error) {
	var err error
	var name string
	if key == "" {
		name = c.Args().First()
	} else {
		name, err = util.GetFlag(c, key, required, err)
		if err != nil {
			return "", err
		}
	}
	if name == "" && !required {
		return "", nil
	}

	if err := util.CheckName(name); err != nil {
		return "", err
	}
	return name, nil
}

func ResponseOutput(v interface{}) ([]byte, error) {
	j, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return nil, err
	}
	return j, nil
}

func cmdBackupCreate(c *cli.Context) {
	if err := doBackupCreate(c); err != nil {
		panic(err)
	}
}

func doBackupCreate(c *cli.Context) error {
	var err error

	destURL, err := util.GetFlag(c, "dest", true, err)
	if err != nil {
		return err
	}

	snapshotName, err := getName(c, "", true)
	if err != nil {
		return err
	}

	volumePath, err := util.GetFlag(c, "volume", true, err)
	if err != nil {
		return err
	}

	// Switch to one level upper than volume working directory
	volumeName := path.Base(volumePath)
	volumeDir := path.Dir(volumePath)
	log.Info("volume dir, name", volumeDir, volumeName)
	if volumeDir != "" {
		if err := os.Chdir(volumeDir); err != nil {
			return err
		}
	}

	volumeInfo, err := replica.ReadInfo(volumeName)
	if err != nil {
		return err
	}
	replicaBackup := replica.NewBackup(volumeInfo.BackingFile)

	volume := &objectstore.Volume{
		Name:        volumeName,
		Driver:      DRIVERNAME,
		Size:        volumeInfo.Size,
		CreatedTime: util.Now(),
	}
	snapshot := &objectstore.Snapshot{
		Name:        snapshotName,
		CreatedTime: util.Now(),
	}

	log.Debugf("Starting backup for %v, snapshot %v, dest %v", volume, snapshot, destURL)
	backupURL, err := objectstore.CreateDeltaBlockBackup(volume, snapshot, destURL, replicaBackup)
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
	var err error
	backupURL, err := util.GetFlag(c, "", true, err)
	if err != nil {
		return err
	}
	backupURL = util.UnescapeURL(backupURL)

	if err := objectstore.DeleteDeltaBlockBackup(backupURL); err != nil {
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
	var err error
	backupURL, err := util.GetFlag(c, "", true, err)
	if err != nil {
		return err
	}
	backupURL = util.UnescapeURL(backupURL)

	toFile, err := util.GetFlag(c, "to", true, err)
	if err != nil {
		return err
	}

	if err := objectstore.RestoreDeltaBlockBackup(backupURL, toFile); err != nil {
		return err
	}
	return nil
}

func cmdBackupList(c *cli.Context) {
	if err := doBackupList(c); err != nil {
		panic(err)
	}
}

func doBackupList(c *cli.Context) error {
	var err error

	destURL, err := util.GetFlag(c, "", true, err)
	volumeName, err := util.GetName(c, "volume", false, err)
	if err != nil {
		return err

	}
	list, err := objectstore.List(volumeName, destURL, DRIVERNAME)
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

	backupURL, err := util.GetFlag(c, "", true, err)
	if err != nil {
		return err
	}
	backupURL = util.UnescapeURL(backupURL)

	info, err := objectstore.GetBackupInfo(backupURL)
	if err != nil {
		return nil
	}
	fmt.Println(info)

	return nil
}
