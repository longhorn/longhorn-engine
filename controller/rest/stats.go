package rest

import (
	"net/http"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/sparse-tools/stats"
)

//ListStats flushes stats accumulated since previous flush
func (s *Server) ListStats(rw http.ResponseWriter, req *http.Request) error {
	var input StatsInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}
	stats.PrintLimited(input.Limit)
	return nil
}
