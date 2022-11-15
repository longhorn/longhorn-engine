package iscsidev

import (
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/nsfilelock"

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

type ScsiDeviceParameters struct {
	ScsiTimeout int64
}

type IscsiDeviceParameters struct {
	IscsiAbortTimeout int64
}

type Device struct {
	Target       string
	KernelDevice *util.KernelDevice

	ScsiDeviceParameters
	IscsiDeviceParameters

	BackingFile string
	BSType      string
	BSOpts      string

	targetID int
}

func NewDevice(name, backingFile, bsType, bsOpts string, scsiTimeout, iscsiAbortTimeout int64) (*Device, error) {
	dev := &Device{
		Target: GetTargetName(name),
		ScsiDeviceParameters: ScsiDeviceParameters{
			ScsiTimeout: scsiTimeout,
		},
		IscsiDeviceParameters: IscsiDeviceParameters{
			IscsiAbortTimeout: iscsiAbortTimeout,
		},
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
	return "iqn.2019-10.io.longhorn:" + Volume2ISCSIName(name)
}

func (dev *Device) CreateTarget() (err error) {
	// Start tgtd daemon if it's not already running
	if err := iscsi.StartDaemon(false); err != nil {
		return err
	}

	tid := 0
	for i := 0; i < RetryCounts; i++ {
		if tid, err = iscsi.FindNextAvailableTargetID(); err != nil {
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
	if err != nil {
		return err
	}

	if err := iscsi.AddLun(dev.targetID, TargetLunID, dev.BackingFile, dev.BSType, dev.BSOpts); err != nil {
		return err
	}
	// Cannot modify the parameters for the LUNs during the adding stage
	if err := iscsi.SetLunThinProvisioning(dev.targetID, TargetLunID); err != nil {
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
		return errors.Wrapf(err, "failed to lock")
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
	for i := 0; i < RetryCounts; i++ {
		err := iscsi.DiscoverTarget(localIP, dev.Target, ne)
		if iscsi.IsTargetDiscovered(localIP, dev.Target, ne) {
			break
		}

		logrus.Warnf("Failed to discover due to %v", err)
		// This is a trick to recover from the case. Remove the
		// empty entries in /etc/iscsi/nodes/<target_name>. If one of the entry
		// is empty it will triggered the issue.
		if err := iscsi.CleanupScsiNodes(dev.Target, ne); err != nil {
			logrus.Warnf("Failed to clean up nodes for %v: %v", dev.Target, err)
		} else {
			logrus.Warnf("Nodes cleaned up for %v", dev.Target)
		}

		time.Sleep(RetryIntervalSCSI)
	}
	if err := iscsi.UpdateIscsiDeviceAbortTimeout(dev.Target, dev.IscsiAbortTimeout, ne); err != nil {
		return err
	}
	if err := iscsi.LoginTarget(localIP, dev.Target, ne); err != nil {
		return err
	}
	if dev.KernelDevice, err = iscsi.GetDevice(localIP, dev.Target, TargetLunID, ne); err != nil {
		return err
	}
	if err := iscsi.UpdateScsiDeviceTimeout(dev.KernelDevice.Name, dev.ScsiTimeout, ne); err != nil {
		return err
	}

	return nil
}

func (dev *Device) StopInitiator() error {
	lock := nsfilelock.NewLockWithTimeout(util.GetHostNamespacePath(HostProc), LockFile, LockTimeout)
	if err := lock.Lock(); err != nil {
		return errors.Wrapf(err, "failed to lock")
	}
	defer lock.Unlock()

	if err := LogoutTarget(dev.Target); err != nil {
		return errors.Wrapf(err, "failed to logout target")
	}
	return nil
}

func LogoutTarget(target string) error {
	ne, err := util.NewNamespaceExecutor(util.GetHostNamespacePath(HostProc))
	if err != nil {
		return err
	}

	if err := iscsi.CheckForInitiatorExistence(ne); err != nil {
		return err
	}
	if iscsi.IsTargetLoggedIn("", target, ne) {
		var err error
		loggingOut := false

		logrus.Infof("Shutdown SCSI device for target %v", target)
		for i := 0; i < RetryCounts; i++ {
			// New IP may be different from the IP in the previous record.
			// https://github.com/longhorn/longhorn/issues/1920
			err = iscsi.LogoutTarget("", target, ne)
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
				if !iscsi.IsTargetLoggedIn("", target, ne) {
					err = nil
					break
				}
				time.Sleep(RetryIntervalSCSI)
			}
		}
		if err != nil {
			return errors.Wrapf(err, "failed to logout target")
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
			if !iscsi.IsTargetDiscovered("", target, ne) {
				err = nil
				break
			}

			err = iscsi.DeleteDiscoveredTarget("", target, ne)
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
		if tid != dev.targetID && dev.targetID != 0 {
			logrus.Errorf("BUG: Invalid TID %v found for %v, was %v", tid, dev.Target, dev.targetID)
		}
		logrus.Infof("Shutdown SCSI target %v", dev.Target)
		if err := iscsi.UnbindInitiator(tid, "ALL"); err != nil {
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

		if err := iscsi.DeleteLun(tid, TargetLunID); err != nil {
			return err
		}

		if err := iscsi.DeleteTarget(tid); err != nil {
			return err
		}
	}
	return nil
}
