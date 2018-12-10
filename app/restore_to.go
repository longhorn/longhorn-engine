package app

import (
	"fmt"
	"os/exec"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/backupstore"
	"github.com/rancher/longhorn-engine/util"
	"github.com/urfave/cli"
)

const (
	DefaultImageFormat = "qcow2"
	QEMUImageBinary    = "qemu-img"
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
				Name:  "backing-file",
				Usage: "filepath or dirpath containing exactly one qcow2 backing file",
			},
			cli.StringFlag{
				Name:  "backup-url",
				Usage: "backup URL to be published",
			},
			cli.StringFlag{
				Name:  "image-format",
				Usage: "format of image to produce",
				Value: DefaultImageFormat,
			},
			cli.StringFlag{
				Name:  "output-file",
				Usage: "filepath to write the resulting image to",
				Value: "volume." + DefaultImageFormat,
			},
		},
		Action: func(c *cli.Context) {
			if err := restoreTo(c); err != nil {
				logrus.Fatalf("Error running restore to file command: %v", err)
			}
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

	backupFilepath := "backup.img"
	if err := backupstore.RestoreDeltaBlockBackup(backupURL, backupFilepath); err != nil {
		return err
	}

	outputFile := c.String("output-file")
	backingFileOrDir := c.String("backing-file")
	if backingFileOrDir == "" {
		if err := ConvertImage(backupFilepath, outputFile, imageFormat); err != nil {
			return err
		}
	} else {
		backingFilepath, err := util.ResolveBackingFilepath(c.String("backing-file"))
		if err != nil {
			return err
		}
		if err := ConvertImage(backupFilepath, outputFile, DefaultImageFormat); err != nil {
			return err
		}
		if err := CommitSnapshotToBackingFile(outputFile, backingFilepath); err != nil {
			return err
		}
		if imageFormat != DefaultImageFormat {
			if err := ConvertImage(backingFilepath, outputFile, imageFormat); err != nil {
				return err
			}
		} else {
			if err := exec.Command("cp", "-rf", backingFilepath, outputFile).Run(); err != nil {
				return err
			}
		}
	}
	logrus.Infof("Produced image: %s", outputFile)

	return nil
}

func ConvertImage(srcFilepath, dstFilepath, format string) error {
	defer logrus.Debugf("ConvertImage (%s) %s -> %s", format, srcFilepath, dstFilepath)

	return exec.Command(QEMUImageBinary, "convert", "-O", format, srcFilepath, dstFilepath).Run()
}

func CommitSnapshotToBackingFile(snapFilepath, backingFilepath string) error {
	defer logrus.Debugf("CommitSnapshotToBackingFile %s -> %s", snapFilepath, backingFilepath)

	if err := rebaseSnapshot(snapFilepath, backingFilepath); err != nil {
		return err
	}
	return commitSnapshot(snapFilepath)
}

func rebaseSnapshot(snapFilepath, backingFilepath string) error {
	return exec.Command(QEMUImageBinary, "rebase", "-u", "-b", backingFilepath, snapFilepath).Run()
}

func commitSnapshot(snapFilepath string) error {
	return exec.Command(QEMUImageBinary, "commit", snapFilepath).Run()
}
