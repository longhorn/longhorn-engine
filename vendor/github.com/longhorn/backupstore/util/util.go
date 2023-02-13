package util

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"
	"time"

	fs "github.com/google/fscrypt/filesystem"
	"github.com/google/uuid"
	lz4 "github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/util/wait"
	mount "k8s.io/mount-utils"
)

const (
	PreservedChecksumLength = 64
)

var (
	cmdTimeout = time.Minute // one minute by default

	forceCleanupMountTimeout = 30 * time.Second
)

// NopCloser wraps an io.Witer as io.WriteCloser
// with noop Close
type NopCloser struct {
	io.Writer
}

func (NopCloser) Close() error { return nil }

func GenerateName(prefix string) string {
	suffix := strings.Replace(NewUUID(), "-", "", -1)
	return prefix + "-" + suffix[:16]
}

func NewUUID() string {
	return uuid.New().String()
}

func GetChecksum(data []byte) string {
	checksumBytes := sha512.Sum512(data)
	checksum := hex.EncodeToString(checksumBytes[:])[:PreservedChecksumLength]
	return checksum
}

func GetFileChecksum(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func CompressData(method string, data []byte) (io.ReadSeeker, error) {
	if method == "none" {
		return bytes.NewReader(data), nil
	}

	var buffer bytes.Buffer

	w, err := newCompressionWriter(method, &buffer)
	if err != nil {
		return nil, err
	}

	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, err
	}
	w.Close()
	return bytes.NewReader(buffer.Bytes()), nil
}

func DecompressAndVerify(method string, src io.Reader, checksum string) (io.Reader, error) {
	r, err := newDecompressionReader(method, src)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	block, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if GetChecksum(block) != checksum {
		return nil, fmt.Errorf("checksum verification failed for block")
	}
	return bytes.NewReader(block), nil
}

func newCompressionWriter(method string, buffer io.Writer) (io.WriteCloser, error) {
	switch method {
	case "gzip":
		return gzip.NewWriter(buffer), nil
	case "lz4":
		return lz4.NewWriter(buffer), nil
	default:
		return nil, fmt.Errorf("unsupported compression method: %v", method)
	}
}

func newDecompressionReader(method string, r io.Reader) (io.ReadCloser, error) {
	switch method {
	case "none":
		return ioutil.NopCloser(r), nil
	case "gzip":
		return gzip.NewReader(r)
	case "lz4":
		return ioutil.NopCloser(lz4.NewReader(r)), nil
	default:
		return nil, fmt.Errorf("unsupported decompression method: %v", method)
	}
}

func Now() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func UnorderedEqual(x, y []string) bool {
	if len(x) != len(y) {
		return false
	}
	known := make(map[string]struct{})
	for _, value := range x {
		known[value] = struct{}{}
	}
	for _, value := range y {
		if _, present := known[value]; !present {
			return false
		}
	}
	return true
}

func Filter(elements []string, predicate func(string) bool) []string {
	var filtered []string
	for _, elem := range elements {
		if predicate(elem) {
			filtered = append(filtered, elem)
		}
	}
	return filtered
}

func ExtractNames(names []string, prefix, suffix string) []string {
	result := []string{}
	for _, f := range names {
		// Remove additional slash if exists
		f = strings.TrimLeft(f, "/")

		// missing prefix or suffix
		if !strings.HasPrefix(f, prefix) || !strings.HasSuffix(f, suffix) {
			continue
		}

		f = strings.TrimPrefix(f, prefix)
		f = strings.TrimSuffix(f, suffix)
		if !ValidateName(f) {
			logrus.Errorf("Invalid name %v was processed to extract name with prefix %v suffix %v",
				f, prefix, suffix)
			continue
		}
		result = append(result, f)
	}
	return result
}

func ValidateName(name string) bool {
	validName := regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]+$`)
	return validName.MatchString(name)
}

func Execute(binary string, args []string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cmdTimeout)
	defer cancel()
	return execute(ctx, binary, args)
}

func ExecuteWithCustomTimeout(binary string, args []string, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return execute(ctx, binary, args)
}

func execute(ctx context.Context, binary string, args []string) (string, error) {
	var output []byte
	var err error

	cmd := exec.CommandContext(ctx, binary, args...)
	done := make(chan struct{})

	go func() {
		output, err = cmd.CombinedOutput()
		close(done)
	}()

	select {
	case <-done:
		break
	case <-ctx.Done():
		return "", fmt.Errorf("timeout executing: %v %v, output %v, error %v", binary, args, string(output), err)
	}

	if err != nil {
		return "", fmt.Errorf("failed to execute: %v %v, output %v, error %v", binary, args, string(output), err)
	}

	return string(output), nil
}

func UnescapeURL(url string) string {
	// Deal with escape in url inputted from bash
	result := strings.Replace(url, "\\u0026", "&", 1)
	result = strings.Replace(result, "u0026", "&", 1)
	return result
}

func IsMounted(mountPoint string) bool {
	output, err := Execute("mount", []string{})
	if err != nil {
		return false
	}
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if strings.Contains(line, " "+mountPoint+" ") {
			return true
		}
	}
	return false
}

func cleanupMount(mountDir string, mounter mount.Interface, log logrus.FieldLogger) error {
	forceUnmounter, ok := mounter.(mount.MounterForceUnmounter)
	if ok {
		log.Infof("Trying to force clean up mount point %v", mountDir)
		return mount.CleanupMountWithForce(mountDir, forceUnmounter, false, forceCleanupMountTimeout)
	}

	log.Infof("Trying to clean up mount point %v", mountDir)
	return mount.CleanupMountPoint(mountDir, forceUnmounter, false)
}

func EnsureMountPoint(Kind, mountPoint string, mounter mount.Interface, log logrus.FieldLogger) (mounted bool, err error) {
	defer func() {
		if !mounted && err == nil {
			if mkdirErr := os.MkdirAll(mountPoint, 0700); mkdirErr != nil {
				err = errors.Wrapf(err, "cannot create mount directory %v", mountPoint)
			}
		}
	}()

	mounted, err = mounter.IsMountPoint(mountPoint)
	if err != nil {
		if strings.Contains(err.Error(), syscall.ENOENT.Error()) {
			return false, nil
		}
	}

	IsCorruptedMnt := mount.IsCorruptedMnt(err)
	if !IsCorruptedMnt {
		logrus.Warnf("Mount point %v is trying reading dir to make sure it is healthy", mountPoint)
		if _, readErr := ioutil.ReadDir(mountPoint); readErr != nil {
			logrus.WithError(readErr).Warnf("Mount point %v was identified as corrupt by ReadDir", mountPoint)
			IsCorruptedMnt = true
		}
	}

	if IsCorruptedMnt {
		log.Warnf("Failed to check mount point %v (mounted=%v)", mountPoint, mounted)
		if mntErr := cleanupMount(mountPoint, mounter, log); mntErr != nil {
			return true, errors.Wrapf(mntErr, "failed to clean up corrupted mount point %v", mountPoint)
		}
		mounted = false
	}

	if !mounted {
		return false, nil
	}

	mnt, err := fs.GetMount(mountPoint)
	if err != nil {
		return true, errors.Wrapf(err, "failed to get mount for %v", mountPoint)
	}

	if strings.Contains(mnt.FilesystemType, Kind) {
		return true, nil
	}

	log.Warnf("Cleaning up the mount point %v because the fstype %v is changed to %v", mountPoint, mnt.FilesystemType, Kind)

	if mntErr := cleanupMount(mountPoint, mounter, log); mntErr != nil {
		return true, errors.Wrapf(mntErr, "failed to clean up mount point %v (%v) for %v protocol", mnt.FilesystemType, mountPoint, Kind)
	}

	return false, nil
}

func MountWithTimeout(mounter mount.Interface, source string, target string, fstype string,
	options []string, sensitiveOptions []string, interval, timeout time.Duration) error {
	mountComplete := false
	err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		err := mounter.MountSensitiveWithoutSystemd(source, target, fstype, options, sensitiveOptions)
		mountComplete = true
		return true, err
	})
	if !mountComplete {
		return errors.Wrapf(err, "mounting %v share %v on %v timed out", fstype, source, target)
	}
	return nil
}
