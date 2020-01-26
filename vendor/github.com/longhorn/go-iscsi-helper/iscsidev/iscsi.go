package iscsidev

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/yasker/nsfilelock"

	"github.com/longhorn/go-iscsi-helper/iscsi"
	"github.com/longhorn/go-iscsi-helper/util"
)

var (
	LockFile    = "/var/run/longhorn-iscsi.lock"
	LockTimeout = 120 * time.Second

	TargetLunID = 1

	RetryCounts           = 5
	RetryIntervalSCSI     = 3 * time.Second
	RetryIntervalTargetID = 500 * time.Millisecond

	HostProc = "/host/proc"
)

type Device struct {
	Target      string
	Device      string
	BackingFile string
	BSType      string
	BSOpts      string

	targetID int
}

func NewDevice(name, backingFile, bsType, bsOpts string) (*Device, error) {
	dev := &Device{
		Target:      GetTargetName(name),
		BackingFile: backingFile,
		BSType:      bsType,
		BSOpts:      bsOpts,
	}
	return dev, nil
}

func Volume2ISCSIName(name string) string {
	return strings.Replace(name, "_", ":", -1)
}

func GetTargetName(name string) string {
	return "iqn.2014-09.com.rancher:" + Volume2ISCSIName(name)
}

func (dev *Device) CreateTarget() error {
	// Start tgtd daemon if it's not already running
	if err := iscsi.StartDaemon(false); err != nil {
		return err
	}

	for i := 0; i < RetryCounts; i++ {
		tid, err := iscsi.FindNextAvailableTargetID()
		if err != nil {
			return err
		}
		logrus.Infof("go-iscsi-helper: found available target id %v", tid)
		err = iscsi.CreateTarget(tid, dev.Target)
		if err == nil {
			dev.targetID = tid
			break
		}
		logrus.Infof("go-iscsi-helper: failed to use target id %v, retrying with a new target ID: err %v", tid, err)
		time.Sleep(RetryIntervalTargetID)
		continue
	}

	if err := iscsi.AddLun(dev.targetID, TargetLunID, dev.BackingFile, dev.BSType, dev.BSOpts); err != nil {
		return err
	}
	if err := iscsi.BindInitiator(dev.targetID, "ALL"); err != nil {
		return err
	}
	return nil
}

func (dev *Device) StartInitator() error {
	lock := nsfilelock.NewLockWithTimeout(util.GetHostNamespacePath(HostProc), LockFile, LockTimeout)
	if err := lock.Lock(); err != nil {
		return fmt.Errorf("Fail to lock: %v", err)
	}
	defer lock.Unlock()

	ne, err := util.NewNamespaceExecutor(util.GetHostNamespacePath(HostProc))
	if err != nil {
		return err
	}

	if err := iscsi.CheckForInitiatorExistence(ne); err != nil {
		return err
	}

	localIP, err := util.GetIPToHost()
	if err != nil {
		return err
	}

	// Setup initiator
	err = nil
	for i := 0; i < RetryCounts; i++ {
		err = iscsi.DiscoverTarget(localIP, dev.Target, ne)
		if iscsi.IsTargetDiscovered(localIP, dev.Target, ne) {
			break
		}

		logrus.Warnf("FAIL to discover due to %v", err)
		// This is a trick to recover from the case. Remove the
		// empty entries in /etc/iscsi/nodes/<target_name>. If one of the entry
		// is empty it will triggered the issue.
		if err := iscsi.CleanupScsiNodes(dev.Target, ne); err != nil {
			logrus.Warnf("Fail to cleanup nodes for %v: %v", dev.Target, err)
		} else {
			logrus.Warnf("Nodes cleaned up for %v", dev.Target)
		}

		time.Sleep(RetryIntervalSCSI)
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
		time.Sleep(RetryIntervalSCSI)
	}
	if !deviceFound {
		return fmt.Errorf("Failed to wait for device %s to show up", dev.Device)
	}
	return nil
}

func (dev *Device) StopInitiator() error {
	lock := nsfilelock.NewLockWithTimeout(util.GetHostNamespacePath(HostProc), LockFile, LockTimeout)
	if err := lock.Lock(); err != nil {
		return fmt.Errorf("Fail to lock: %v", err)
	}
	defer lock.Unlock()

	if err := LogoutTarget(dev.Target); err != nil {
		return fmt.Errorf("Fail to logout target: %v", err)
	}
	return nil
}

func LogoutTarget(target string) error {
	ne, err := util.NewNamespaceExecutor(util.GetHostNamespacePath(HostProc))
	if err != nil {
		return err
	}
	ip, err := util.GetIPToHost()
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
			time.Sleep(RetryIntervalSCSI)
		}
		// Wait for device to logout
		if loggingOut {
			logrus.Infof("Logout SCSI device timeout, waiting for logout complete")
			for i := 0; i < RetryCounts; i++ {
				if !iscsi.IsTargetLoggedIn(ip, target, ne) {
					err = nil
					break
				}
				time.Sleep(RetryIntervalSCSI)
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
			time.Sleep(RetryIntervalSCSI)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (dev *Device) DeleteTarget() error {
	if tid, err := iscsi.GetTargetTid(dev.Target); err == nil && tid != -1 {
		if tid != dev.targetID {
			logrus.Fatalf("BUG: Invalid TID %v found for %v", tid, dev.Target)
		}
		logrus.Infof("Shutdown SCSI target %v", dev.Target)
		if err := iscsi.UnbindInitiator(tid, "ALL"); err != nil {
			return err
		}
		if err := iscsi.DeleteLun(tid, TargetLunID); err != nil {
			return err
		}

		sessionConnectionsMap, err := iscsi.GetTargetConnections(tid)
		if err != nil {
			return err
		}
		for sid, cidList := range sessionConnectionsMap {
			for _, cid := range cidList {
				if err := iscsi.CloseConnection(tid, sid, cid); err != nil {
					return err
				}
			}
		}

		if err := iscsi.DeleteTarget(tid); err != nil {
			return err
		}
	}
	return nil
}

func (dev *Device) UpdateScsiBackingStore(bsType, bsOpts string) error {
	dev.BSType = bsType
	dev.BSOpts = bsOpts
	return nil
}

func (dev *Device) RecreateTarget() error {
	if err := dev.DeleteTarget(); err != nil {
		return err
	}
	if err := dev.CreateTarget(); err != nil {
		return err
	}
	return nil
}

func (dev *Device) ReloadInitiator() error {
	lock := nsfilelock.NewLockWithTimeout(util.GetHostNamespacePath(HostProc), LockFile, LockTimeout)
	if err := lock.Lock(); err != nil {
		return fmt.Errorf("Fail to lock: %v", err)
	}
	defer lock.Unlock()

	ne, err := util.NewNamespaceExecutor(util.GetHostNamespacePath(HostProc))
	if err != nil {
		return err
	}
	if err := iscsi.CheckForInitiatorExistence(ne); err != nil {
		return err
	}
	ip, err := util.GetIPToHost()
	if err != nil {
		return err
	}

	if err := iscsi.RescanTarget(ip, dev.Target, ne); err != nil {
		return err
	}

	return nil
}
