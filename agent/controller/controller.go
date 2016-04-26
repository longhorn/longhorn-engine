package controller

import (
	"fmt"
	"os/exec"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/rancher/go-rancher-metadata/metadata"
	lclient "github.com/rancher/longhorn/client"
	"github.com/rancher/longhorn/controller/rest"
	"io/ioutil"
	"net/http"
	"os"
)

const (
	defaultVolumeSize = "10737418240" // 10 gb
	MetadataURL       = "http://rancher-metadata/2015-12-19"
)

type replica struct {
	client      *metadata.Client
	host        string
	port        int
	healthState string
	size        string
}

func ReplicaAddress(host string, port int) string {
	return fmt.Sprintf("tcp://%s:%d", host, port)
}

type Controller struct {
	client *lclient.ControllerClient
}

func New() *Controller {
	client := lclient.NewControllerClient("http://localhost:9501")
	return &Controller{
		client: client,
	}
}

func (c *Controller) Close() error {
	logrus.Infof("Shutting down Longhorn.")
	return nil
}

func (c *Controller) Start() error {
	logrus.Infof("Starting Longhorn.")

	volume, err := c.client.GetVolume()
	if err != nil {
		return fmt.Errorf("Error while getting volume: %v", err)
	}

	if volume.ReplicaCount == 0 {
		if err = c.getReplicasAndStart(); err != nil {
			return err
		}
	} else {
		logrus.Infof("Volume is started with %v replicas.", volume.ReplicaCount)
	}

	return c.refresh()
}

func (c *Controller) getReplicasAndStart() error {
	var fromMetadata map[string]replica
	var scale int
	for {
		var err error
		if scale, fromMetadata, err = c.replicasFromMetadata(); err != nil {
			return err
		} else if len(fromMetadata) < scale {
			logrus.Infof("Waiting for replicas. Current %v, expected: %v", len(fromMetadata), scale)
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}

	startingReplicas := map[string]replica{}
	dirtyReplicas := map[string]replica{}
	for address, replFromMD := range fromMetadata {
		replicaClient, err := lclient.NewReplicaClient(address)
		if err != nil {
			logrus.Errorf("Error getting client for replica %v. Removing from list of startup replicas. Error: %v", address, err)
			continue
		}

		replica, err := replicaClient.GetReplica()
		if replica.Dirty {
			logrus.Infof("Removing dirty replica %v from startup.", address)
			dirtyReplicas[address] = replFromMD
		} else {
			startingReplicas[address] = replFromMD
		}
	}

	if len(startingReplicas) == 0 && len(dirtyReplicas) > 0 {
		// just start with a single dirty replica
		for k, v := range dirtyReplicas {
			logrus.Infof("Couldn't find any clean replicas. Using dirty replica %v.", k)
			startingReplicas[k] = v
			break
		}
	}

	addresses := []string{}
	for address, repl := range startingReplicas {
		if err := c.ensureOpen(repl); err != nil {
			logrus.Errorf("Replica %v is not open. Removing it from startup list. Error while waiting for open: %v", address, err)
			continue
		}
		addresses = append(addresses, address)
	}

	if len(addresses) == 0 {
		return fmt.Errorf("Couldn't find any valid replicas to start with. Original replica set from metadta: %v", fromMetadata)
	}

	logrus.Infof("Starting controller with replicas: %v.", addresses)
	if err := c.client.Start(addresses...); err != nil {
		return fmt.Errorf("Error starting controller: %v", err)
	}

	return nil
}

func (c *Controller) refresh() error {
	for {
		if err := c.syncReplicas(); err != nil {
			logrus.Errorf("Failed to sync replicas: %v", err)
		}
		time.Sleep(5 * time.Second)
	}
}

func (c *Controller) syncReplicas() (retErr error) {
	logrus.Debugf("Syncing replicas.")

	replicasInController, err := c.client.ListReplicas()
	if err != nil {
		return fmt.Errorf("Error listing replicas in controller: %v", err)
	}
	fromController := map[string]rest.Replica{}
	for _, r := range replicasInController {
		fromController[r.Address] = r
	}

	_, fromMetadata, err := c.replicasFromMetadata()
	if err != nil {
		return fmt.Errorf("Error listing replicas in metadata: %v", err)
	}

	// Remove replicas from controller if they aren't in metadata
	if len(replicasInController) > 1 {
		for address := range fromController {
			if _, ok := fromMetadata[address]; !ok {
				logrus.Infof("Removing replica %v", address)
				if _, err := c.client.DeleteReplica(address); err != nil {
					return fmt.Errorf("Error removing replica %v: %v", address, err)
				}
				return nil // Just remove one replica per cycle
			}
		}
	}

	// Add replicas
	for address, r := range fromMetadata {
		if _, ok := fromController[address]; !ok {
			if err := c.addReplica(r); err != nil {
				return fmt.Errorf("Error adding replica %v: %v", address, err)
			}
		}
	}

	return nil
}

func (c *Controller) addReplica(r replica) error {
	logrus.Infof("Adding replica %v", r.host)
	err := c.ensureOpen(r)
	if err != nil {
		return err
	}

	cmd := exec.Command("longhorn", "add", ReplicaAddress(r.host, r.port))
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	return cmd.Run()
}

func (c *Controller) ensureOpen(r replica) error {
	client, err := lclient.NewReplicaClient(ReplicaAddress(r.host, r.port))
	if err != nil {
		return err
	}

	replica, err := client.GetReplica()
	if err != nil {
		return err
	}

	if replica.State != "open" {
		if err := client.OpenReplica(r.size); err != nil {
			return fmt.Errorf("Error opening replica %v: %v", r.host, err)
		}
	}

	return nil
}

func backoff(maxDuration time.Duration, timeoutMessage string, f func() (bool, error)) error {
	startTime := time.Now()
	waitTime := 150 * time.Millisecond
	maxWaitTime := 2 * time.Second
	for {
		if time.Now().Sub(startTime) > maxDuration {
			return fmt.Errorf(timeoutMessage)
		}

		if done, err := f(); err != nil {
			return err
		} else if done {
			return nil
		}

		time.Sleep(waitTime)

		waitTime *= 2
		if waitTime > maxWaitTime {
			waitTime = maxWaitTime
		}
	}
}

func (c *Controller) replicasFromMetadata() (int, map[string]replica, error) {
	client, err := metadata.NewClientAndWait(MetadataURL)
	if err != nil {
		return 0, nil, err
	}
	service, err := client.GetSelfServiceByName("replica")
	if err != nil {
		return 0, nil, err
	}

	// Unmarshalling the metadata as json is forcing it to a bad format
	resp, err := http.Get(MetadataURL + "/self/service/metadata/longhorn/volume_size")
	if err != nil {
		return 0, nil, err
	}

	size := ""
	if resp.StatusCode == 200 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return 0, nil, err
		}
		size = string(body)
	}

	if size == "" {
		size = defaultVolumeSize
	}

	containers := map[string]metadata.Container{}
	for _, container := range service.Containers {
		if c, ok := containers[container.Name]; !ok {
			containers[container.Name] = container
		} else if container.CreateIndex > c.CreateIndex {
			containers[container.Name] = container
		}
	}

	result := map[string]replica{}
	for _, container := range containers {
		r := replica{
			healthState: container.HealthState,
			host:        container.PrimaryIp,
			port:        9502,
			size:        size,
		}
		result[ReplicaAddress(r.host, r.port)] = r
	}

	return service.Scale, result, nil
}
