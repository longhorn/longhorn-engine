package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	DefaulCmdTimeout = time.Minute // one minute by default

	GRPCHealthProbe = "/usr/local/bin/grpc_health_probe"
)

func Execute(binary string, args ...string) (string, error) {
	return ExecuteWithTimeout(DefaulCmdTimeout, binary, args...)
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

func PrintJSON(obj interface{}) error {
	output, err := json.MarshalIndent(obj, "", "\t")
	if err != nil {
		return err
	}

	fmt.Println(string(output))
	return nil
}

func GetURL(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

func RemoveFile(file string) error {
	if _, err := os.Stat(file); os.IsNotExist(err) {
		// file doesn't exist
		return nil
	}

	if _, err := Execute("rm", file); err != nil {
		return fmt.Errorf("fail to remove file %v: %v", file, err)
	}

	return nil
}

func GRPCServiceReadinessProbe(address string) bool {
	if _, err := Execute(GRPCHealthProbe, "-addr", address); err != nil {
		return false
	}
	return true
}
