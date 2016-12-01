package util

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/yasker/go-iscsi-helper/iscsi"
	iutil "github.com/yasker/go-iscsi-helper/util"
)

type ScsiDevice struct {
	Target      string
	Device      string
	BackingFile string
	BSType      string
	BSOpts      string
}

func NewScsiDevice(name, backingFile, bsType, bsOpts string) (*ScsiDevice, error) {
	dev := &ScsiDevice{
		Target:      GetTargetName(name),
		BackingFile: backingFile,
		BSType:      bsType,
		BSOpts:      bsOpts,
	}
	return dev, nil
}

func GetTargetName(name string) string {
	return "iqn.2014-09.com.rancher:" + name
}

func GetLocalIP() (string, error) {
	ips, err := iutil.GetLocalIPs()
	if err != nil {
		return "", err
	}
	return ips[0], nil
}

func StartScsi(dev *ScsiDevice) error {
	ne, err := iutil.NewNamespaceExecutor("/host/proc/1/ns/")
	if err != nil {
		return err
	}

	if err := iscsi.CheckForInitiatorExistence(ne); err != nil {
		return err
	}

	localIP, err := GetLocalIP()
	if err != nil {
		return err
	}

	// Setup target
	if err := iscsi.StartDaemon(false); err != nil {
		return err
	}
	if err := iscsi.CreateTarget(TargetID, dev.Target); err != nil {
		return err
	}
	if err := iscsi.AddLun(TargetID, TargetLunID, dev.BackingFile, dev.BSType, dev.BSOpts); err != nil {
		return err
	}
	if err := iscsi.BindInitiator(TargetID, "ALL"); err != nil {
		return err
	}

	// Setup initiator
	err = nil
	for i := 0; i < RetryCounts; i++ {
		err = iscsi.DiscoverTarget(localIP, dev.Target, ne)
		if iscsi.IsTargetDiscovered(localIP, dev.Target, ne) {
			break
		}

		logrus.Warnf("FAIL to discover %v", err)
		// This is a trick to recover from the case. Remove the
		// empty entries in /etc/iscsi/nodes/<target_name>. If one of the entry
		// is empty it will triggered the issue.
		if err := iscsi.CleanupScsiNodes(dev.Target, ne); err != nil {
			logrus.Warnf("Fail to cleanup nodes for %v: %v", dev.Target, err)
		} else {
			logrus.Warnf("Nodes cleaned up for %v", dev.Target)
		}

		time.Sleep(time.Duration(RetryInterval) * time.Second)
	}
	if err := iscsi.LoginTarget(localIP, dev.Target, ne); err != nil {
		return err
	}
	if dev.Device, err = iscsi.GetDevice(localIP, dev.Target, TargetLunID, ne); err != nil {
		return err
	}

	deviceFound := false
	for i := 0; i < RetryCounts; i++ {
		if st, err := os.Stat(dev.Device); err == nil && (st.Mode()&os.ModeDevice != 0) {
			deviceFound = true
			break
		}
		time.Sleep(time.Duration(RetryInterval) * time.Second)
	}
	if !deviceFound {
		return fmt.Errorf("Failed to wait for device %s to show up", dev.Device)
	}
	return nil
}

func StopScsi(volumeName string) error {
	target := GetTargetName(volumeName)
	if err := LogoutTarget(target); err != nil {
		return fmt.Errorf("Fail to logout target: %v", err)
	}
	if err := DeleteTarget(target); err != nil {
		return fmt.Errorf("Fail to delete target: %v", err)
	}
	return nil
}

func LogoutTarget(target string) error {
	ne, err := iutil.NewNamespaceExecutor("/host/proc/1/ns/")
	if err != nil {
		return err
	}
	ip, err := GetLocalIP()
	if err != nil {
		return err
	}

	if err := iscsi.CheckForInitiatorExistence(ne); err != nil {
		return err
	}
	if iscsi.IsTargetLoggedIn(ip, target, ne) {
		var err error
		loggingOut := false

		logrus.Infof("Shutdown SCSI device for %v:%v", ip, target)
		for i := 0; i < RetryCounts; i++ {
			err = iscsi.LogoutTarget(ip, target, ne)
			// Ignore Not Found error
			if err == nil || strings.Contains(err.Error(), "exit status 21") {
				err = nil
				break
			}
			// The timeout for response may return in the future,
			// check session to know if it's logged out or not
			if strings.Contains(err.Error(), "Timeout executing: ") {
				loggingOut = true
				break
			}
			time.Sleep(time.Duration(RetryInterval) * time.Second)
		}
		// Wait for device to logout
		if loggingOut {
			logrus.Infof("Logout SCSI device timeout, waiting for logout complete")
			for i := 0; i < RetryCounts; i++ {
				if !iscsi.IsTargetLoggedIn(ip, target, ne) {
					err = nil
					break
				}
				time.Sleep(time.Duration(RetryInterval) * time.Second)
			}
		}
		if err != nil {
			return fmt.Errorf("Failed to logout target: %v", err)
		}
		/*
		 * Immediately delete target after logout may result in error:
		 *
		 * "Could not execute operation on all records: encountered
		 * iSCSI database failure" in iscsiadm
		 *
		 * This happenes especially there are other iscsiadm db
		 * operations go on at the same time.
		 * Retry to workaround this issue. Also treat "exit status
		 * 21"(no record found) as valid result
		 */
		for i := 0; i < RetryCounts; i++ {
			if !iscsi.IsTargetDiscovered(ip, target, ne) {
				err = nil
				break
			}

			err = iscsi.DeleteDiscoveredTarget(ip, target, ne)
			// Ignore Not Found error
			if err == nil || strings.Contains(err.Error(), "exit status 21") {
				err = nil
				break
			}
			time.Sleep(time.Duration(RetryInterval) * time.Second)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func DeleteTarget(target string) error {
	if tid, err := iscsi.GetTargetTid(target); err == nil && tid != -1 {
		if tid != TargetID {
			logrus.Fatalf("BUG: Invalid TID %v found for %v", tid, target)
		}
		logrus.Infof("Shutdown SCSI target %v", target)
		if err := iscsi.UnbindInitiator(TargetID, "ALL"); err != nil {
			return err
		}
		if err := iscsi.DeleteLun(TargetID, TargetLunID); err != nil {
			return err
		}
		if err := iscsi.DeleteTarget(TargetID); err != nil {
			return err
		}
	}
	return nil
}
