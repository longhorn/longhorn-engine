package rest

import (
	"net/http"

	"github.com/rancher/sparse-tools/stats"
)

//ListStats flushes stats accumulated since previous flush
func (s *Server) ListStats(rw http.ResponseWriter, req *http.Request) error {
	stats.Print()
	return nil
}
