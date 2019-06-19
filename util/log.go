package util

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
)

const (
	LogComponentField = "component"
)

type LonghornFormatter struct {
	*logrus.TextFormatter

	LogsDir string
}

func SetUpLogger(logsDir string) error {
	_, err := os.Stat(logsDir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if os.IsNotExist(err) {
		if err := os.Mkdir(logsDir, 0755); err != nil {
			return err
		}
	}
	logsDir, err = filepath.Abs(logsDir)
	if err != nil {
		return err
	}
	testFile := filepath.Join(logsDir, "test")
	if _, err := os.OpenFile(testFile, os.O_WRONLY|os.O_CREATE, 0644); os.IsPermission(err) {
		return err
	}
	logrus.Infof("Storing process logs at path: %v", logsDir)
	logrus.SetFormatter(LonghornFormatter{
		TextFormatter: &logrus.TextFormatter{
			DisableColors: true,
		},
		LogsDir: logsDir,
	})
	return nil
}

func (l LonghornFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	logMsg := &bytes.Buffer{}
	component, ok := entry.Data[LogComponentField]
	if !ok {
		component = "longhorn-engine-launcher"
	}
	component, ok = component.(string)
	if !ok {
		return nil, errors.New("field component must be a string")
	}
	logMsg.WriteString("[" + component.(string) + "] ")
	if component == "longhorn-engine-launcher" {
		msg, err := l.TextFormatter.Format(entry)
		if err != nil {
			return nil, err
		}
		logMsg.Write(msg)
	} else {
		logMsg.WriteString(entry.Message)
		logMsg.WriteString("\n")
	}

	return logMsg.Bytes(), nil
}
