package tcmu

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn/types"
)

const (
	devPath           = "/dev/longhorn/"
	configDir         = "/sys/kernel/config/target/core/user_42"
	scsiDir           = "/sys/kernel/config/target/loopback"
	wwnPrefix         = "naa.6001405"
	teardownRetryWait = 1
	teardownAttempts  = 2
)

func New() types.Frontend {
	return &Tcmu{}
}

type Tcmu struct {
	volume string
	isUp   bool
}

func (t *Tcmu) Startup(name string, size, sectorSize int64, rw types.ReaderWriterAt) error {
	t.volume = name

	t.Shutdown()

	if err := PreEnableTcmu(name, size, sectorSize); err != nil {
		return err
	}

	if err := start(name, rw); err != nil {
		return err
	}

	if err := PostEnableTcmu(name); err != nil {
		return err
	}
	t.isUp = true
	return nil
}

func (t *Tcmu) Shutdown() error {
	if err := TeardownTcmu(t.volume); err != nil {
		return err
	}
	t.isUp = false
	return nil
}

func (t *Tcmu) State() types.State {
	if t.isUp {
		return types.StateUp
	}
	return types.StateDown
}

func PreEnableTcmu(volume string, size, sectorSize int64) error {
	err := writeLines(path.Join(configDir, volume, "control"), []string{
		fmt.Sprintf("dev_size=%d", size),
		fmt.Sprintf("dev_config=%s", GetDevConfig(volume)),
		fmt.Sprintf("hw_block_size=%d", sectorSize),
		"async=1",
	})
	if err != nil {
		return err
	}

	return writeLines(path.Join(configDir, volume, "enable"), []string{
		"1",
	})
}

func PostEnableTcmu(volume string) error {
	prefix, nexusWnn := getScsiPrefixAndWnn(volume)

	err := writeLines(path.Join(prefix, "nexus"), []string{
		nexusWnn,
	})
	if err != nil {
		return err
	}

	lunPath := getLunPath(prefix)
	logrus.Infof("Creating directory: %s", lunPath)
	if err := os.MkdirAll(lunPath, 0755); err != nil && !os.IsExist(err) {
		return err
	}

	logrus.Infof("Linking: %s => %s", path.Join(lunPath, volume), path.Join(configDir, volume))
	if err := os.Symlink(path.Join(configDir, volume), path.Join(lunPath, volume)); err != nil {
		return err
	}

	return createDevice(volume)
}

func createDevice(volume string) error {
	os.MkdirAll(devPath, 0700)

	dev := devPath + volume

	if _, err := os.Stat(dev); err == nil {
		return fmt.Errorf("Device %s already exists, can not create", dev)
	}

	tgt, _ := getScsiPrefixAndWnn(volume)

	address, err := ioutil.ReadFile(path.Join(tgt, "address"))
	if err != nil {
		return err
	}

	found := false
	matches := []string{}
	path := fmt.Sprintf("/sys/bus/scsi/devices/%s*/block/*/dev", strings.TrimSpace(string(address)))
	for i := 0; i < 30; i++ {
		var err error
		matches, err = filepath.Glob(path)
		if len(matches) > 0 && err == nil {
			found = true
			break
		}

		logrus.Infof("Waiting for %s", path)
		time.Sleep(1 * time.Second)
	}

	if !found {
		return fmt.Errorf("Failed to find %s", path)
	}

	if len(matches) == 0 {
		return fmt.Errorf("Failed to find %s", path)
	}

	if len(matches) > 1 {
		return fmt.Errorf("Too many matches for %s, found %d", path, len(matches))
	}

	majorMinor, err := ioutil.ReadFile(matches[0])
	if err != nil {
		return err
	}

	parts := strings.Split(strings.TrimSpace(string(majorMinor)), ":")
	if len(parts) != 2 {
		return fmt.Errorf("Invalid major:minor string %s", string(majorMinor))
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return err
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return err
	}

	logrus.Infof("Creating device %s %d:%d", dev, major, minor)
	return mknod(dev, major, minor)
}

func mknod(device string, major, minor int) error {
	var fileMode os.FileMode = 0600
	fileMode |= syscall.S_IFBLK
	dev := int((major << 8) | (minor & 0xff) | ((minor & 0xfff00) << 12))

	return syscall.Mknod(device, uint32(fileMode), dev)
}

func GetDevConfig(volume string) string {
	return fmt.Sprintf("longhorn//%s", volume)
}

func getScsiPrefixAndWnn(volume string) (string, string) {
	suffix := genSuffix(volume)
	loopbackWnn := wwnPrefix + "r0" + suffix
	nexusWnn := wwnPrefix + "r1" + suffix
	return path.Join(scsiDir, loopbackWnn, "tpgt_1"), nexusWnn
}

func getLunPath(prefix string) string {
	return path.Join(prefix, "lun", "lun_0")
}

func TeardownTcmu(volume string) error {
	var err error
	for i := 1; i <= teardownAttempts; i++ {
		logrus.Info("Starting TCMU teardown.")
		if err := teardown(volume); err != nil {
			if i < teardownAttempts {
				logrus.Infof("Error occurred during TCMU teardown. Attempting again after %v second sleep. Error: %v", teardownRetryWait, err)
				time.Sleep(time.Second * teardownRetryWait)
				continue
			} else {
				break
			}
		}
		stop()
		if err := finishTeardown(volume); err != nil {
			if i < teardownAttempts {
				logrus.Infof("Error occurred during TCMU teardown. Attempting again after %v second sleep. Error: %v", teardownRetryWait, err)
				time.Sleep(time.Second * teardownRetryWait)
				continue
			} else {
				break
			}

		}
		logrus.Info("TCMU teardown successful.")
		break
	}
	return err
}

func teardown(volume string) error {
	dev := devPath + volume
	tpgtPath, _ := getScsiPrefixAndWnn(volume)
	lunPath := getLunPath(tpgtPath)

	/*
		We're removing:
		/sys/kernel/config/target/loopback/naa.<id>/tpgt_1/lun/lun_0/<volume name>
		/sys/kernel/config/target/loopback/naa.<id>/tpgt_1/lun/lun_0
		/sys/kernel/config/target/loopback/naa.<id>/tpgt_1
		/sys/kernel/config/target/loopback/naa.<id>
	*/
	pathsToRemove := []string{
		path.Join(lunPath, volume),
		lunPath,
		tpgtPath,
		path.Dir(tpgtPath),
	}

	for _, p := range pathsToRemove {
		err := remove(p)
		if err != nil {
			return err
		}
	}

	// Should be cleaned up automatically, but if it isn't remove it
	if _, err := os.Stat(dev); err == nil {
		err := remove(dev)
		if err != nil {
			return err
		}
	}

	return nil
}

func finishTeardown(volume string) error {
	/*
		We're removing:
		/sys/kernel/config/target/core/user_42/<volume name>
	*/
	pathsToRemove := []string{
		path.Join(configDir, volume),
	}

	for _, p := range pathsToRemove {
		err := remove(p)
		if err != nil {
			return err
		}
	}

	return nil
}
func removeAsync(path string, done chan<- error) {
	logrus.Infof("Removing: %s", path)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		logrus.Errorf("Unable to remove: %v", path)
		done <- err
	}
	logrus.Debugf("Removed: %s", path)
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

func writeLines(target string, lines []string) error {
	dir := path.Dir(target)
	if stat, err := os.Stat(dir); os.IsNotExist(err) {
		logrus.Infof("Creating directory: %s", dir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	} else if !stat.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}

	for _, line := range lines {
		content := []byte(line + "\n")
		logrus.Infof("Setting %s: %s", target, line)
		if err := ioutil.WriteFile(target, content, 0755); err != nil {
			logrus.Errorf("Failed to write %s to %s: %v", line, target, err)
			return err
		}
	}

	return nil
}

func genSuffix(volume string) string {
	digest := md5.New()
	digest.Write([]byte(volume))
	return hex.EncodeToString(digest.Sum([]byte{}))[:8]
}
