package util

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"

	"github.com/gorilla/handlers"
	"github.com/satori/go.uuid"
)

var (
	parsePattern = regexp.MustCompile(`(.*):(\d+)`)
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

func DuplicateDevice(src, dest string) error {
	if st, err := os.Stat(src); err != nil {
		return fmt.Errorf("Cannot duplicate device because cannot find %s: %v", src, err)
	} else if st.Mode()&os.ModeDevice == 0 {
		return fmt.Errorf("Device %s is not a block device", src)
	}
	if err := os.Symlink(src, dest); err != nil {
		return fmt.Errorf("Cannot duplicate device %s to %s: %v", src, dest, err)
	}
	return nil
}

func RemoveDevice(dev string) error {
	if _, err := os.Stat(dev); err == nil {
		if err := os.Remove(dev); err != nil {
			return fmt.Errorf("Failed to removing device %s, %v", dev, err)
		}
	}
	return nil
}
