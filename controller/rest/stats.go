package rest

import (
	"net/http"

	"github.com/rancher/go-rancher/api"
	journal "github.com/rancher/sparse-tools/stats"
)

//ListJournal flushes operation journal (replica read/write, ping, etc.) accumulated since previous flush
func (s *Server) ListJournal(rw http.ResponseWriter, req *http.Request) error {
	var input JournalInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}
	journal.PrintLimited(input.Limit)
	return nil
}
