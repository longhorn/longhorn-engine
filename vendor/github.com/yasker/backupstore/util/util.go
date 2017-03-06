package util

import (
	"bytes"
	"compress/gzip"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/satori/go.uuid"
)

const (
	PRESERVED_CHECKSUM_LENGTH = 64
)

var (
	cmdTimeout time.Duration = time.Minute // one minute by default
)

func GenerateName(prefix string) string {
	suffix := strings.Replace(NewUUID(), "-", "", -1)
	return prefix + "-" + suffix[:16]
}

func NewUUID() string {
	return uuid.NewV4().String()
}

func GetChecksum(data []byte) string {
	checksumBytes := sha512.Sum512(data)
	checksum := hex.EncodeToString(checksumBytes[:])[:PRESERVED_CHECKSUM_LENGTH]
	return checksum
}

func CompressData(data []byte) (io.ReadSeeker, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, err
	}
	w.Close()
	return bytes.NewReader(b.Bytes()), nil
}

func DecompressAndVerify(src io.Reader, checksum string) (io.Reader, error) {
	r, err := gzip.NewReader(src)
	if err != nil {
		return nil, err
	}
	block, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if GetChecksum(block) != checksum {
		return nil, fmt.Errorf("Checksum verification failed for block!")
	}
	return bytes.NewReader(block), nil
}

func Now() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func ExtractNames(names []string, prefix, suffix string) ([]string, error) {
	result := []string{}
	for i := range names {
		f := names[i]
		// Remove additional slash if exists
		f = strings.TrimLeft(f, "/")
		f = strings.TrimPrefix(f, prefix)
		f = strings.TrimSuffix(f, suffix)
		if !ValidateName(f) {
			return nil, fmt.Errorf("Invalid name %v was processed to extract name with prefix %v surfix %v", names[i], prefix, suffix)
		}
		result = append(result, f)
	}
	return result, nil
}

func ValidateName(name string) bool {
	validName := regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]+$`)
	return validName.MatchString(name)
}

func Execute(binary string, args []string) (string, error) {
	var output []byte
	var err error
	cmd := exec.Command(binary, args...)
	done := make(chan struct{})

	go func() {
		output, err = cmd.CombinedOutput()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(cmdTimeout):
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil {
				logrus.Warnf("Problem killing process pid=%v: %s", cmd.Process.Pid, err)
			}

		}
		return "", fmt.Errorf("Timeout executing: %v %v, output %v, error %v", binary, args, string(output), err)
	}

	if err != nil {
		return "", fmt.Errorf("Failed to execute: %v %v, output %v, error %v", binary, args, string(output), err)
	}
	return string(output), nil
}
