package rest

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"

	"github.com/Sirupsen/logrus"

	"github.com/rancher/go-rancher/api"
)

func (s *server) ConfigureBackupTarget(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	target := &BackupTarget{}
	if err := apiContext.Read(target); err != nil {
		return err
	}

	if err := s.createNFSMount(target); err != nil {
		return fmt.Errorf("Unable to create nfs mount for %#v. Error %v", target, err)
	}

	return nil
}

func (s *server) createNFSMount(target *BackupTarget) error {
	mountDir := constructMountDir(target)

	grep := exec.Command("grep", mountDir, "/proc/mounts")
	if err := grep.Run(); err == nil {
		logrus.Infof("Found mount %v.", mountDir)
		return nil
	}

	if err := os.MkdirAll(mountDir, 0770); err != nil {
		return err
	}

	remoteTarget := fmt.Sprintf("%v:%v", target.NFSConfig.Server, target.NFSConfig.Share)
	parentPid := strconv.Itoa(os.Getppid())

	var mount *exec.Cmd
	if target.NFSConfig.MountOptions == "" {
		mount = exec.Command("nsenter", "-t", parentPid, "-n", "mount", "-t", "nfs", remoteTarget, mountDir)
	} else {
		mount = exec.Command("nsenter", "-t", parentPid, "-n", "mount", "-t", "nfs", "-o", target.NFSConfig.MountOptions, remoteTarget, mountDir)
	}

	mount.Stdout = os.Stdout
	mount.Stderr = os.Stderr

	logrus.Infof("Running %v", mount.Args)
	return mount.Run()
}

func constructMountDir(target *BackupTarget) string {
	return fmt.Sprintf("/var/lib/rancher/longhorn/backups/%s/%s", target.Name, target.UUID)
}
