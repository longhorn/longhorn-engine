package iscsi

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/rancher/convoy/util"
)

var (
	tgtBinary = "tgtadm"
)

// CreateTarget will create a iSCSI target using the name specified. If name is
// unspecified, a name will be generated. Notice the name must comply with iSCSI
// name format.
func CreateTarget(tid int, name string) error {
	opts := []string{
		"--lld", "iscsi",
		"--op", "new",
		"--mode", "target",
		"--tid", strconv.Itoa(tid),
		"-T", name,
	}
	_, err := util.Execute(tgtBinary, opts)
	if err != nil {
		return err
	}
	return nil
}

// DeleteTarget will remove a iSCSI target specified by tid
func DeleteTarget(tid int) error {
	opts := []string{
		"--lld", "iscsi",
		"--op", "delete",
		"--mode", "target",
		"--tid", strconv.Itoa(tid),
	}
	_, err := util.Execute(tgtBinary, opts)
	if err != nil {
		return err
	}
	return nil
}

// AddLunBackedByFile will add a LUN in an existing target, which backing by
// specified file.
func AddLunBackedByFile(tid int, lun int, backingFile string) error {
	opts := []string{
		"--lld", "iscsi",
		"--op", "new",
		"--mode", "logicalunit",
		"--tid", strconv.Itoa(tid),
		"--lun", strconv.Itoa(lun),
		"-b", backingFile,
	}
	_, err := util.Execute(tgtBinary, opts)
	if err != nil {
		return err
	}
	return nil
}

// AddLunBackedByAIOFile will add a LUN in an existing target, which backing by
// specified file, using AIO backing-store
func AddLunBackedByAIOFile(tid int, lun int, backingFile string) error {
	if !CheckTargetForBackingStore("aio") {
		return fmt.Errorf("Backing-store aio is not supported")
	}
	opts := []string{
		"--lld", "iscsi",
		"--op", "new",
		"--mode", "logicalunit",
		"--tid", strconv.Itoa(tid),
		"--lun", strconv.Itoa(lun),
		"-b", backingFile,
		"--bstype", "aio",
	}
	_, err := util.Execute(tgtBinary, opts)
	if err != nil {
		return err
	}
	return nil
}

// DeleteLun will remove a LUN from an target
func DeleteLun(tid int, lun int) error {
	opts := []string{
		"--lld", "iscsi",
		"--op", "delete",
		"--mode", "logicalunit",
		"--tid", strconv.Itoa(tid),
		"--lun", strconv.Itoa(lun),
	}
	_, err := util.Execute(tgtBinary, opts)
	if err != nil {
		return err
	}
	return nil
}

// BindInitiator will add permission to allow certain initiator(s) to connect to
// certain target. "ALL" is a special initiator which is the wildcard
func BindInitiator(tid int, initiator string) error {
	opts := []string{
		"--lld", "iscsi",
		"--op", "bind",
		"--mode", "target",
		"--tid", strconv.Itoa(tid),
		"-I", initiator,
	}
	_, err := util.Execute(tgtBinary, opts)
	if err != nil {
		return err
	}
	return nil
}

// UnbindInitiator will remove permission to allow certain initiator(s) to connect to
// certain target.
func UnbindInitiator(tid int, initiator string) error {
	opts := []string{
		"--lld", "iscsi",
		"--op", "unbind",
		"--mode", "target",
		"--tid", strconv.Itoa(tid),
		"-I", initiator,
	}
	_, err := util.Execute(tgtBinary, opts)
	if err != nil {
		return err
	}
	return nil
}

// StartDaemon will start tgtd daemon, prepare for further commands
func StartDaemon() error {
	_, err := util.Execute("tgtd", []string{})
	if err != nil {
		return err
	}
	return nil
}

func CheckTargetForBackingStore(name string) bool {
	opts := []string{
		"--lld", "iscsi",
		"--op", "show",
		"--mode", "system",
	}
	output, err := util.Execute(tgtBinary, opts)
	if err != nil {
		return false
	}
	return strings.Contains(output, " "+name)
}
