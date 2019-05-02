package util

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/handlers"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

var (
	MaximumVolumeNameSize = 64
	parsePattern          = regexp.MustCompile(`(.*):(\d+)`)
	validVolumeName       = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]+$`)
	validLabelValue       = regexp.MustCompile(`^[a-zA-Z0-9_.\-/:]+$`)

	cmdTimeout = time.Minute // one minute by default
)

const (
	BlockSizeLinux = 512
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
		loggingHandler: handlers.CombinedLoggingHandler(writer, router),
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

func DuplicateDevice(src, dest string) error {
	stat := unix.Stat_t{}
	if err := unix.Stat(src, &stat); err != nil {
		return fmt.Errorf("Cannot duplicate device because cannot find %s: %v", src, err)
	}
	major := int(stat.Rdev / 256)
	minor := int(stat.Rdev % 256)
	if err := mknod(dest, major, minor); err != nil {
		return fmt.Errorf("Cannot duplicate device %s to %s", src, dest)
	}
	if err := os.Chmod(dest, 0660); err != nil {
		return fmt.Errorf("Couldn't change permission of the device %s: %s", dest, err)
	}
	return nil
}

func mknod(device string, major, minor int) error {
	var fileMode os.FileMode = 0660
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
		return fmt.Errorf("timeout trying to delete %s", path)
	}
}

func ValidVolumeName(name string) bool {
	if len(name) > MaximumVolumeNameSize {
		return false
	}
	return validVolumeName.MatchString(name)
}

func ValidLabelValue(name string) bool {
	return validLabelValue.MatchString(name)
}

func Volume2ISCSIName(name string) string {
	return strings.Replace(name, "_", ":", -1)
}

func Now() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func GetFileActualSize(file string) int64 {
	var st syscall.Stat_t
	if err := syscall.Stat(file, &st); err != nil {
		logrus.Errorf("Fail to get size of file %v", file)
		return -1
	}
	return st.Blocks * BlockSizeLinux
}

func ParseLabels(labels []string) (map[string]string, error) {
	result := map[string]string{}
	for _, label := range labels {
		kv := strings.Split(label, "=")
		if len(kv) != 2 {
			return nil, fmt.Errorf("Invalid label not in <key>=<value> format %v", label)
		}
		key := kv[0]
		value := kv[1]
		//Well, we should rename that ValidVolumeName
		if !ValidVolumeName(key) {
			return nil, fmt.Errorf("Invalid key %v for label %v", key, label)
		}
		if !ValidLabelValue(value) {
			return nil, fmt.Errorf("Invalid value %v for label %v", value, label)
		}
		result[key] = value
	}
	return result, nil
}

func UnescapeURL(url string) string {
	// Deal with escape in url inputed from bash
	result := strings.Replace(url, "\\u0026", "&", 1)
	result = strings.Replace(result, "u0026", "&", 1)
	result = strings.TrimLeft(result, "\"'")
	result = strings.TrimRight(result, "\"'")
	return result
}

func Execute(binary string, args ...string) (string, error) {
	return ExecuteWithTimeout(cmdTimeout, binary, args...)
}

func ExecuteWithTimeout(timeout time.Duration, binary string, args ...string) (string, error) {
	var err error
	cmd := exec.Command(binary, args...)
	done := make(chan struct{})

	var output, stderr bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &stderr

	go func() {
		err = cmd.Run()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil {
				logrus.Warnf("Problem killing process pid=%v: %s", cmd.Process.Pid, err)
			}

		}
		return "", fmt.Errorf("Timeout executing: %v %v, output %s, stderr, %s, error %v",
			binary, args, output.String(), stderr.String(), err)
	}

	if err != nil {
		return "", fmt.Errorf("Failed to execute: %v %v, output %s, stderr, %s, error %v",
			binary, args, output.String(), stderr.String(), err)
	}
	return output.String(), nil
}

func ExecuteWithoutTimeout(binary string, args ...string) (string, error) {
	var err error
	var output, stderr bytes.Buffer

	cmd := exec.Command(binary, args...)
	cmd.Stdout = &output
	cmd.Stderr = &stderr

	if err = cmd.Run(); err != nil {
		return "", fmt.Errorf("Failed to execute: %v %v, output %s, stderr, %s, error %v",
			binary, args, output.String(), stderr.String(), err)
	}
	return output.String(), nil
}

func CheckBackupType(backupTarget string) (string, error) {
	u, err := url.Parse(backupTarget)
	if err != nil {
		return "", err
	}

	return u.Scheme, nil
}

func ResolveBackingFilepath(fileOrDirpath string) (string, error) {
	fileOrDir, err := os.Open(fileOrDirpath)
	if err != nil {
		return "", err
	}
	defer fileOrDir.Close()

	fileOrDirInfo, err := fileOrDir.Stat()
	if err != nil {
		return "", err
	}

	if fileOrDirInfo.IsDir() {
		files, err := fileOrDir.Readdir(-1)
		if err != nil {
			return "", err
		}
		if len(files) != 1 {
			return "", fmt.Errorf("expected exactly one file, found %d files/subdirectories", len(files))
		}
		if files[0].IsDir() {
			return "", fmt.Errorf("expected exactly one file, found a subdirectory")
		}
		return filepath.Join(fileOrDirpath, files[0].Name()), nil
	}

	return fileOrDirpath, nil
}
