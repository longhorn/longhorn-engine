package health

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/longhorn/longhorn-engine-launcher/engine"
	"github.com/longhorn/longhorn-engine-launcher/instance"
	"github.com/longhorn/longhorn-engine-launcher/process"
)

type CheckServer struct {
	em *engine.Manager
	pl *process.Manager
	im *instance.Manager
}

func NewHealthCheckServer(em *engine.Manager, pl *process.Manager, im *instance.Manager) *CheckServer {
	return &CheckServer{
		em: em,
		pl: pl,
		im: im,
	}
}

func (hc *CheckServer) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if hc.em != nil && hc.pl != nil && hc.im != nil {
		return &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_SERVING,
		}, nil
	}

	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_NOT_SERVING,
	}, fmt.Errorf("Engine Manager or Process Manager or Instance Manager is not running")
}

func (hc *CheckServer) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	for {
		if hc.em != nil && hc.pl != nil && hc.im != nil {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_SERVING,
			}); err != nil {
				logrus.Errorf("Failed to send health check result %v for gRPC process management server: %v",
					healthpb.HealthCheckResponse_SERVING, err)
			}
		} else {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_NOT_SERVING,
			}); err != nil {
				logrus.Errorf("Failed to send health check result %v for gRPC process management server: %v",
					healthpb.HealthCheckResponse_NOT_SERVING, err)
			}

		}
		time.Sleep(time.Second)
	}
}
