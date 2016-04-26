package status

import (
	"fmt"
	"net/http"

	md "github.com/rancher/go-rancher-metadata/metadata"
	"github.com/rancher/longhorn/client"

	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn/agent/controller"
)

type ReplicaStatus struct {
	controller          *client.ControllerClient
	replica             *client.ReplicaClient
	metadata            *md.Client
	address             string
	controllerLastError string
}

func NewReplicaStatus() (*ReplicaStatus, error) {
	metadata, err := md.NewClientAndWait(controller.MetadataURL)
	if err != nil {
		return nil, err
	}
	self, err := metadata.GetSelfContainer()
	if err != nil {
		return nil, err
	}
	addr := controller.ReplicaAddress(self.PrimaryIp, 9502)

	controllerClient := client.NewControllerClient("http://controller:9501/v1")
	replicaClient, err := client.NewReplicaClient("http://localhost:9502/v1")
	if err != nil {
		return nil, err
	}

	return &ReplicaStatus{
		controller: controllerClient,
		replica:    replicaClient,
		address:    addr,
	}, nil
}

func (s *ReplicaStatus) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	// Checking against the replica is easy: just ensure that the API is responding.
	_, err := s.replica.GetReplica()
	if err != nil {
		writeError(rw, err)
		return
	}

	if ok, msg := s.checkReplicaStatusInController(rw); !ok {
		writeErrorString(rw, msg)
		return
	}

	writeOK(rw)
}

func (s *ReplicaStatus) checkReplicaStatusInController(rw http.ResponseWriter) (bool, string) {
	replicas, err := s.controller.ListReplicas()
	if err != nil {
		logrus.Warnf("Couldn't get replicas from controller. Reporting cached status.")
		return s.reportCacheControllerResponse()
	}
	for _, replica := range replicas {
		if replica.Address == s.address {
			if strings.EqualFold(replica.Mode, "err") {
				return s.cacheControllerResponse(false, fmt.Sprintf("Replica %v is in error mode.", s.address))
			}
			return s.cacheControllerResponse(true, "")
		}
	}

	return s.cacheControllerResponse(false, fmt.Sprintf("Replica %v is not in the controller's list of replicas. Current list: %v", s.address, replicas))
}

func (s *ReplicaStatus) reportCacheControllerResponse() (bool, string) {
	healthy := len(s.controllerLastError) == 0
	return healthy, s.controllerLastError
}

func (s *ReplicaStatus) cacheControllerResponse(ok bool, msg string) (bool, string) {
	if ok {
		s.controllerLastError = ""
	} else {
		s.controllerLastError = msg + " (cached response)"
	}
	return ok, msg
}
