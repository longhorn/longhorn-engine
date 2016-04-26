package status

import (
	"net/http"

	"github.com/Sirupsen/logrus"
)

func writeOK(rw http.ResponseWriter) {
	logrus.Debugf("Reporting OK.")
	rw.Write([]byte("OK"))
}

func writeError(rw http.ResponseWriter, err error) {
	writeErrorString(rw, err.Error())
}

func writeErrorString(rw http.ResponseWriter, msg string) {
	if rw != nil {
		logrus.Infof("Reporting unhealthy status: %v", msg)
		rw.WriteHeader(http.StatusServiceUnavailable)
		rw.Write([]byte(msg))
	} else {
		logrus.Warn("Not reporting a status. ResponseWriter is null.")
	}
}
