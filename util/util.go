package util

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/handlers"
	"github.com/satori/go.uuid"
	"github.com/yasker/go-iscsi-helper/iscsi"
	iutil "github.com/yasker/go-iscsi-helper/util"
	"golang.org/x/sys/unix"
)

var (
	parsePattern = regexp.MustCompile(`(.*):(\d+)`)

	TargetID    = 1
	TargetLunID = 1

	RetryCounts   = 5
	RetryInterval = 3
)

func ParseAddresses(name string) (string, string, string, error) {
	matches := parsePattern.FindStringSubmatch(name)
	if matches == nil {
		return "", "", "", fmt.Errorf("Invalid address %s does not match pattern: %v", name, parsePattern)
	}

	host := matches[1]
	port, _ := strconv.Atoi(matches[2])

	return fmt.Sprintf("%s:%d", host, port),
		fmt.Sprintf("%s:%d", host, port+1),
		fmt.Sprintf("%s:%d", host, port+2), nil
}

func UUID() string {
	return uuid.NewV4().String()
}

func Filter(list []string, check func(string) bool) []string {
	result := make([]string, 0, len(list))
	for _, i := range list {
		if check(i) {
			result = append(result, i)
		}
	}
	return result
}

func Contains(arr []string, val string) bool {
	for _, a := range arr {
		if a == val {
			return true
		}
	}
	return false
}

type filteredLoggingHandler struct {
	filteredPaths  map[string]struct{}
	handler        http.Handler
	loggingHandler http.Handler
}

func FilteredLoggingHandler(filteredPaths map[string]struct{}, writer io.Writer, router http.Handler) http.Handler {
	return filteredLoggingHandler{
		filteredPaths:  filteredPaths,
		handler:        router,
		loggingHandler: handlers.LoggingHandler(writer, router),
	}
}

func (h filteredLoggingHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		if _, exists := h.filteredPaths[req.URL.Path]; exists {
			h.handler.ServeHTTP(w, req)
			return
		}
	}
	h.loggingHandler.ServeHTTP(w, req)
}

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

		logrus.Infof("Shutdown SCSI device for %v:%v", ip, target)
		if err := iscsi.LogoutTarget(ip, target, ne); err != nil {
			return err
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

func DuplicateDevice(src, dest string) error {
	stat := unix.Stat_t{}
	if err := unix.Stat(src, &stat); err != nil {
		return fmt.Errorf("Cannot find %s: %v", src, err)
	}
	major := int(stat.Rdev / 256)
	minor := int(stat.Rdev % 256)
	if err := mknod(dest, major, minor); err != nil {
		return fmt.Errorf("Cannot duplicate device %s to %s", src, dest)
	}
	return nil
}

func mknod(device string, major, minor int) error {
	var fileMode os.FileMode = 0600
	fileMode |= unix.S_IFBLK
	dev := int((major << 8) | (minor & 0xff) | ((minor & 0xfff00) << 12))

	logrus.Infof("Creating device %s %d:%d", device, major, minor)
	return unix.Mknod(device, uint32(fileMode), dev)
}

func RemoveDevice(dev string) error {
	if _, err := os.Stat(dev); err == nil {
		if err := remove(dev); err != nil {
			return fmt.Errorf("Failed to removing device %s, %v", dev, err)
		}
	}
	return nil
}

func removeAsync(path string, done chan<- error) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		logrus.Errorf("Unable to remove: %v", path)
		done <- err
	}
	done <- nil
}

func remove(path string) error {
	done := make(chan error)
	go removeAsync(path, done)
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("Timeout trying to delete %s.", path)
	}
}
