package rest

import (
	"fmt"
	"net/http"

	"github.com/yasker/go-websocket-toolbox/broadcaster"

	"github.com/longhorn/longhorn-engine/types"
)

type Metrics struct {
	ReadBandwidth  uint64 `json:"readBandwidth,string"`
	WriteBandwidth uint64 `json:"writeBandwidth,string"`
	ReadLatency    uint64 `json:"readLatency,string"`
	WriteLatency   uint64 `json:"writeLatency,string"`
	IOPS           uint64 `json:"iops,string"`
}

func (s *Server) processEventMetrics(e *broadcaster.Event, r *http.Request) (interface{}, error) {
	metrics, ok := e.Data.(*types.Metrics)
	if !ok {
		return nil, fmt.Errorf("not types.Metrics in the event")
	}
	output := &Metrics{}
	if metrics.IOPS.Read != 0 {
		output.ReadBandwidth = metrics.Bandwidth.Read
		output.ReadLatency = metrics.TotalLatency.Read / metrics.IOPS.Read
	}
	if metrics.IOPS.Write != 0 {
		output.WriteBandwidth = metrics.Bandwidth.Write
		output.WriteLatency = metrics.TotalLatency.Write / metrics.IOPS.Write
	}
	output.IOPS = metrics.IOPS.Read + metrics.IOPS.Write
	return output, nil
}
