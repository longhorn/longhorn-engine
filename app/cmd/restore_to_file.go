package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"gopkg.in/cheggaaa/pb.v2"

	iutil "github.com/longhorn/go-iscsi-helper/util"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/longhorn-engine/pkg/replica"
	"github.com/longhorn/longhorn-engine/pkg/util"
)

const (
	DefaultOutputFormat   = "qcow2"
	DefaultOutputFileName = "volume"
	QEMUImageBinary       = "qemu-img"
	BackupFilePath        = "backup.img"
	BackupFileConverted   = "backup.img.converted"
	BackingFileCopy       = "backing.img.cp"

	PeriodicRefreshIntervalInSeconds = 2
)

var SupportedImageFormats = []string{
	"qcow2",
	"raw",
}

func RestoreToFileCmd() cli.Command {
	return cli.Command{
		Name:  "restore-to-file",
		Usage: "restore a backup to a raw image or a qcow2 image: restore-to-file <backupURL> --backing-file <backing-file-path> --output-file <output-file> --output-format <output-format>",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "backing-file",
				Usage: "filepath or dirpath containing exactly one qcow2 backing file",
			},
			cli.StringFlag{
				Name:  "output-file",
				Usage: "filepath to write the resulting image to",
			},
			cli.StringFlag{
				Name:  "output-format",
				Usage: "format of output file image to produce",
				Value: DefaultOutputFormat,
			},
			cli.IntFlag{
				Name:  "concurrent-limit",
				Value: 5,
				Usage: "Concurrent backup worker threads",
			},
		},
		Action: func(c *cli.Context) {
			logrus.Infof("Running restore to file command: backup-url=%s backing-file=%s output-file=%s output-format=%s",
				c.Args().First(), c.String("backing-file"), c.String("output-file"), c.String("output-format"))
			if err := restoreToFile(c); err != nil {
				logrus.WithError(err).Fatalf("Error running restore to file command")
			}
			logrus.Infof("Done running restore to file command. Produced image: %s",
				c.String("output-file"))
		},
	}
}

func restore(url string, concurrentLimit int) error {
	backupURL := util.UnescapeURL(url)
	requestedBackupName, _, _, err := backupstore.DecodeBackupURL(backupURL)
	if err != nil {
		return err
	}
	restoreObj := replica.NewRestore(BackupFilePath, "", backupURL, requestedBackupName)
	config := &backupstore.DeltaRestoreConfig{
		BackupURL:       backupURL,
		DeltaOps:        restoreObj,
		Filename:        BackupFilePath,
		ConcurrentLimit: int32(concurrentLimit),
	}

	if err := backupstore.RestoreDeltaBlockBackup(config); err != nil {
		return err
	}

	bar := pb.StartNew(100)
	periodicChecker := time.NewTicker(PeriodicRefreshIntervalInSeconds * time.Second)

	for range periodicChecker.C {
		restoreObj.Lock()
		restoreProgress := restoreObj.Progress
		restoreError := restoreObj.Error
		restoreObj.Unlock()

		if restoreProgress == 100 {
			bar.Set(restoreProgress)
			bar.Finish()
			periodicChecker.Stop()
			return nil
		}
		if restoreError != "" {
			bar.Finish()
			periodicChecker.Stop()
			return fmt.Errorf("%v", restoreError)
		}
		bar.Set(restoreProgress)
	}
	return nil
}

func restoreToFile(c *cli.Context) error {
	outputFormat := c.String("output-format")
	if !outputFormatSupported(outputFormat) {
		return fmt.Errorf("unsupported output image format: %s", outputFormat)
	}

	backupURL := c.Args().First()
	if backupURL == "" {
		return fmt.Errorf("missing the first argument, it should be backup-url")
	}

	concurrentLimit := c.Int("concurrent-limit")

	outputFile := c.String("output-file")
	if outputFile == "" {
		outputFile = DefaultOutputFileName + "." + outputFormat
	}
	outputFilePath, err := filepath.Abs(outputFile)
	if err != nil {
		return errors.Wrap(err, "error confirming output file path")
	}
	logrus.Infof("Output file path=%s", outputFilePath)

	defer CleanupTempFiles(outputFile, BackupFilePath, BackupFileConverted, BackingFileCopy)

	logrus.Infof("Start to restore %s to %s", backupURL, BackupFilePath)
	if err := restore(backupURL, concurrentLimit); err != nil {
		return err
	}
	logrus.Infof("Done restoring %s to %s", backupURL, BackupFilePath)

	backingFileOrDir := c.String("backing-file")
	if backingFileOrDir == "" {
		if err := ConvertImage(BackupFilePath, outputFile, outputFormat); err != nil {
			return err
		}
	} else {
		logrus.Infof("Start to prepare and check backing file")
		backingFilepath, err := util.ResolveBackingFilepath(backingFileOrDir)
		if err != nil {
			return err
		}
		if err := CheckBackingFileFormat(backingFilepath); err != nil {
			return err
		}
		if err := CopyFile(backingFilepath, BackingFileCopy); err != nil {
			return err
		}
		logrus.Infof("Done preparing and checking backing file: %s", backingFilepath)
		if err := ConvertImage(BackupFilePath, BackupFileConverted, DefaultOutputFormat); err != nil {
			return err
		}
		if err := MergeSnapshotsToBackingFile(BackupFileConverted, BackingFileCopy); err != nil {
			return err
		}
		if err := ConvertImage(BackingFileCopy, outputFile, outputFormat); err != nil {
			return err
		}
	}

	return nil
}

func outputFormatSupported(desiredFormat string) bool {
	for _, supportedFormat := range SupportedImageFormats {
		if desiredFormat == supportedFormat {
			return true
		}
	}
	return false
}

func CheckBackingFileFormat(backingFilePath string) error {
	output, err := iutil.ExecuteWithoutTimeout(QEMUImageBinary, []string{"info", backingFilePath})
	if err != nil {
		return errors.Wrapf(err, "failed CheckBackingFileFormat %s", backingFilePath)
	}
	if !strings.Contains(output, "file format: qcow2") {
		return fmt.Errorf("the format of backing file is not qcow2. backing file info=%s", output)
	}
	return nil
}

func CopyFile(backingFilepath, outputFile string) error {
	if _, err := iutil.ExecuteWithoutTimeout("cp", []string{backingFilepath, outputFile}); err != nil {
		return err
	}
	return nil
}

func CleanupTempFiles(outputFile string, files ...string) {
	outputFilePath, err := filepath.Abs(outputFile)
	if err != nil {
		logrus.WithError(err).Errorf("Failed to find absolute path for output file=%s", outputFile)
		return
	}
	for _, file := range files {
		filePath, err := filepath.Abs(file)
		if err != nil {
			logrus.WithError(err).Errorf("Failed to find absolute path for tmp file=%s", file)
			continue
		}
		if filePath == outputFilePath {
			continue
		}
		if err := os.Remove(filePath); err != nil {
			logrus.WithError(err).Errorf("Failed to remove tmp file=%s", file)
			continue
		}
	}
	return
}

func ConvertImage(srcFilepath, dstFilepath, format string) error {
	logrus.Infof("Start ConvertImage (%s) %s -> %s", format, srcFilepath, dstFilepath)
	_, err := iutil.ExecuteWithoutTimeout(QEMUImageBinary, []string{"convert", "-O", format, srcFilepath, dstFilepath})
	if err != nil {
		return errors.Wrapf(err, "failed convertImage (%s) %s -> %s", format, srcFilepath, dstFilepath)
	}
	logrus.Infof("Done ConvertImage (%s) %s -> %s", format, srcFilepath, dstFilepath)
	return nil
}

func MergeSnapshotsToBackingFile(snapFilepath, backingFilepath string) error {
	logrus.Infof("Start MergeSnapshotsToBackingFile %s -> %s", snapFilepath, backingFilepath)
	if err := rebaseSnapshot(snapFilepath, backingFilepath); err != nil {
		return errors.Wrapf(err, "failed MergeSnapshotsToBackingFile %s -> %s", snapFilepath, backingFilepath)
	}
	logrus.Infof("Done MergeSnapshotsToBackingFile %s -> %s", snapFilepath, backingFilepath)
	return commitSnapshot(snapFilepath)
}

func rebaseSnapshot(snapFilepath, backingFilepath string) error {
	logrus.Infof("Start rebaseSnapshot %s -> %s", snapFilepath, backingFilepath)
	_, err := iutil.ExecuteWithoutTimeout(QEMUImageBinary, []string{"rebase", "-u", "-b", backingFilepath, "-F", DefaultOutputFormat, snapFilepath})
	if err != nil {
		return errors.Wrapf(err, "failed rebaseSnapshot %s -> %s", snapFilepath, backingFilepath)
	}
	logrus.Infof("Done rebaseSnapshot %s -> %s", snapFilepath, backingFilepath)
	return nil
}

// qemu-img commit will allows you to merge from a 'top' image (snapFilepath)
// into a lower-level 'base' image (backingFilepath).
func commitSnapshot(snapFilepath string) error {
	logrus.Infof("Start commitSnapshot %s", snapFilepath)
	_, err := iutil.ExecuteWithoutTimeout(QEMUImageBinary, []string{"commit", snapFilepath})
	if err != nil {
		return errors.Wrapf(err, "failed commitSnapshot %s", snapFilepath)
	}
	logrus.Infof("Done commitSnapshot %s", snapFilepath)
	return nil
}
