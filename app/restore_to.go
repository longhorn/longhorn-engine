package app

import (
	"fmt"
	"os/exec"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn-engine/replica"
	"github.com/rancher/longhorn-engine/util"
	"github.com/urfave/cli"
)

const DefaultImageFormat = "qcow2"

var SupportedImageFormats = []string{
	"rbd",
	"host_cdrom",
	"blkdebug",
	"qcow",
	"host_device",
	"vpc",
	"qcow2",
	"cloop",
	"vdi",
	"sheepdog",
	"qed",
	"nbd",
	"tftp",
	"vvfat",
	"ftp",
	"ftps",
	"https",
	"dmg",
	"http",
	"vmdk",
	"iscsi",
	"parallels",
	"raw",
	"bochs",
	"quorum",
	"null-aio",
	"null-co",
	"vhdx",
	"blkverify",
	"file",
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
				Value: "/image/base.qcow2",
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
	if c.NArg() != 1 {
		return fmt.Errorf("directory name is required")
	}
	dir := c.Args()[0]

	backingFile, err := openBackingFile(c.String("backing-file"))
	if err != nil {
		return err
	}
	s := replica.NewServer(dir, backingFile, 512)

	backupURL := c.String("backup-url")
	if backupURL == "" {
		return fmt.Errorf("backup-url must be provided")
	}

	snapshotName := "coalesced"
	if err := s.Restore(backupURL, snapshotName); err != nil {
		return err
	}
	coalescedFile := s.GetSnapshotFilepath(snapshotName)

	imageFormat := c.String("image-format")
	if !imageFormatSupported(imageFormat) {
		return fmt.Errorf("unsupported image format: %s", imageFormat)
	}

	outputFile := c.String("output-file")
	if backingFile == nil {
		if err := s.ConvertImage(coalescedFile, outputFile, imageFormat); err != nil {
			return err
		}
	} else {
		backingFilepath, err := util.ResolveBackingFilepath(c.String("backing-file"))
		if err != nil {
			return err
		}
		if err := s.ConvertImage(coalescedFile, outputFile, DefaultImageFormat); err != nil {
			return err
		}
		if err := s.CommitSnapshotToBackingFile(outputFile, backingFilepath); err != nil {
			return err
		}
		if imageFormat != DefaultImageFormat {
			if err := s.ConvertImage(backingFilepath, outputFile, imageFormat); err != nil {
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
