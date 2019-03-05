package app

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/rancher/backupstore"
	"github.com/rancher/longhorn-engine/util"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	DefaultImageFormat  = "qcow2"
	QEMUImageBinary     = "qemu-img"
	BackupFilePath      = "backup.img"
	BackupFileConverted = "backup.img.converted"
	BackingFileCopy     = "backing.img.cp"
)

var SupportedImageFormats = []string{
	"qcow2",
	"raw",
}

func RestoreToCmd() cli.Command {
	return cli.Command{
		Name: "restore-to",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "backup-url",
				Usage: "backup URL to be published",
			},
			cli.StringFlag{
				Name:  "backing-file",
				Usage: "filepath or dirpath containing exactly one qcow2 backing file",
			},
			cli.StringFlag{
				Name:  "output-file",
				Usage: "filepath to write the resulting image to",
				Value: "volume." + DefaultImageFormat,
			},
			cli.StringFlag{
				Name:  "image-format",
				Usage: "format of image to produce",
				Value: DefaultImageFormat,
			},
		},
		Action: func(c *cli.Context) {
			logrus.Infof("Running restore to file command: backup-url=%s  output-file=%s  format=%s",
				c.String("backup-url"), c.String("output-file"), c.String("image-format"))
			if err := restoreTo(c); err != nil {
				logrus.Fatalf("Error running restore to file command: %v", err)
			}
			defer logrus.Infof("Done running restore to file command. Produced image: %s",
				c.String("output-file"))
		},
	}
}

func imageFormatSupported(desiredFormat string) bool {
	for _, supportedFormat := range SupportedImageFormats {
		if desiredFormat == supportedFormat {
			return true
		}
	}
	return false
}

func restoreTo(c *cli.Context) error {
	imageFormat := c.String("image-format")
	if !imageFormatSupported(imageFormat) {
		return fmt.Errorf("unsupported image format: %s", imageFormat)
	}

	backupURL := c.String("backup-url")
	if backupURL == "" {
		return fmt.Errorf("backup-url must be provided")
	}

	if err := backupstore.RestoreDeltaBlockBackup(backupURL, BackupFilePath); err != nil {
		return err
	}

	outputFile := c.String("output-file")
	backingFileOrDir := c.String("backing-file")
	if backingFileOrDir == "" {
		if err := ConvertImage(BackupFilePath, outputFile, imageFormat); err != nil {
			return err
		}
	} else {
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
		if err := ConvertImage(BackupFilePath, BackupFileConverted, DefaultImageFormat); err != nil {
			return err
		}
		if err := MergeSnapshots(BackupFileConverted, BackingFileCopy); err != nil {
			return err
		}
		if err := ConvertImage(BackingFileCopy, outputFile, imageFormat); err != nil {
			return err
		}
	}
	defer CleanupTempFiles(outputFile, BackupFilePath, BackupFileConverted, BackingFileCopy)

	return nil
}

func CheckBackingFileFormat(backingFilePath string) error {
	output, err := exec.Command(QEMUImageBinary, "info", backingFilePath).Output()
	if err != nil {
		return err
	}
	outStr := string(output)
	if !strings.Contains(outStr, "file format: qcow2") {
		return fmt.Errorf("the format of backing file is not qcow2. backing file info=%s", outStr)
	}
	return nil
}

func CopyFile(backingFilepath, outputFile string) error {
	if err := exec.Command("cp", backingFilepath, outputFile).Run(); err != nil {
		return err
	}
	return nil
}

func CleanupTempFiles(outputFile string, files ...string) error {
	outputFilePath, err := filepath.Abs(outputFile)
	if err != nil {
		logrus.Errorf("find absolute path for output file=%s failed: %v", outputFile, err)
		return err
	}
	for _, file := range files {
		filePath, err := filepath.Abs(file)
		if err != nil {
			logrus.Errorf("find absolute path for tmp file=%s failed: %v", file, err)
			return err
		}
		if filePath == outputFilePath {
			continue
		}
		if err := os.Remove(filePath); err != nil {
			return err
		}
	}
	return nil
}

func ConvertImage(srcFilepath, dstFilepath, format string) error {
	defer logrus.Debugf("ConvertImage (%s) %s -> %s", format, srcFilepath, dstFilepath)
	cmd := exec.Command(QEMUImageBinary, "convert", "-O", format, srcFilepath, dstFilepath)
	if err := cmd.Run(); err != nil {
		logrus.Errorf("convertImage (%s) %s -> %s failed: %v", format, srcFilepath, dstFilepath, err)
		return err
	}
	return nil
}

func MergeSnapshots(snapFilepath, backingFilepath string) error {
	defer logrus.Debugf("MergeSnapshots %s -> %s", snapFilepath, backingFilepath)

	if err := rebaseSnapshot(snapFilepath, backingFilepath); err != nil {
		return err
	}
	return commitSnapshot(snapFilepath)
}

func rebaseSnapshot(snapFilepath, backingFilepath string) error {
	output, err := exec.Command(QEMUImageBinary, "rebase", "-u", "-b", backingFilepath, snapFilepath).Output()
	if err != nil {
		logrus.Errorf("rebase snapshot %s -> %s failed: %v", snapFilepath, backingFilepath, err)
		return err
	}
	logrus.Debugf("rebaseSnapshot %s -> %s. %v", string(output))
	return nil
}

// qemu-img commit will allows you to merge from a 'top' image (snapFilepath)
// into a lower-level 'base' image (backingFilepath).
func commitSnapshot(snapFilepath string) error {
	output, err := exec.Command(QEMUImageBinary, "commit", snapFilepath).Output()
	if err != nil {
		logrus.Errorf("commitSnapshot %s failed: %v", snapFilepath, err)
		return err
	}
	logrus.Debugf("commitSnapshot %s. %v", snapFilepath, string(output))
	return nil
}
