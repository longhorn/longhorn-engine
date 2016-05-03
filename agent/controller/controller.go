package controller

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/rancher/go-rancher-metadata/metadata"

	lclient "github.com/rancher/longhorn/client"
	"github.com/rancher/longhorn/controller/rest"
)

const (
	defaultVolumeSize = "10737418240" // 10 gb
	MetadataURL       = "http://rancher-metadata/2015-12-19"
)

type replica struct {
	client      *lclient.ReplicaClient
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
	var replicaMetadata map[string]*replica
	var scale int
	for {
		var err error
		if scale, replicaMetadata, err = c.replicaMetadataAndClient(); err != nil {
			return err
		} else if len(replicaMetadata) < scale {
			logrus.Infof("Waiting for replicas. Current %v, expected: %v", len(replicaMetadata), scale)
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}

	initializingReplicas := map[string]*replica{}
	closedCleanReplicas := map[string]*replica{}
	closedDirtyReplicas := map[string]*replica{}
	openCleanReplicas := map[string]*replica{}
	openDirtyReplicas := map[string]*replica{}
	rebuildingClosedReplicas := map[string]*replica{}
	rebuildingOpenReplicas := map[string]*replica{}
	otherReplicas := map[string]*replica{}

	for address, replicaMd := range replicaMetadata {
		replica, err := replicaMd.client.GetReplica()
		if err != nil {
			logrus.Errorf("Error getting replica %v. Removing from list of start replcias. Error: %v", address, err)
			continue
		}

		if replica.State == "initial" {
			initializingReplicas[address] = replicaMd

		} else if replica.Rebuilding && replica.State == "closed" {
			rebuildingClosedReplicas[address] = replicaMd

		} else if replica.Rebuilding {
			rebuildingOpenReplicas[address] = replicaMd

		} else if replica.State == "closed" && replica.Dirty {
			closedDirtyReplicas[address] = replicaMd

		} else if replica.State == "closed" {
			closedCleanReplicas[address] = replicaMd

		} else if replica.State == "open" {
			openCleanReplicas[address] = replicaMd

		} else if replica.State == "dirty" {
			openDirtyReplicas[address] = replicaMd

		} else {
			otherReplicas[address] = replicaMd

		}
	}
	logrus.Infof("Initializing replicas: %v", initializingReplicas)
	logrus.Infof("Closed and clean replicas: %v", closedCleanReplicas)
	logrus.Infof("Closed and dirty replicas: %v", closedDirtyReplicas)
	logrus.Infof("Open and dirty replicas: %v", openDirtyReplicas)
	logrus.Infof("Open and clean replicas: %v", openCleanReplicas)
	logrus.Infof("Rebuilding and closed replicas: %v", rebuildingClosedReplicas)
	logrus.Infof("Rebuilding and open replicas: %v", rebuildingOpenReplicas)
	logrus.Infof("Other replicas (likely in error state)L %v", otherReplicas)

	// Closed and clean. Start with all replicas.
	attemptedStart, err := c.startWithAll(closedCleanReplicas, false)
	if attemptedStart {
		return err
	}

	// Closed and dirty. Start with one.
	attemptedStart, err = c.startWithOne(closedDirtyReplicas, false)
	if attemptedStart {
		return err
	}

	// Open and dirty. Close and start with one.
	attemptedStart, err = c.startWithOne(openDirtyReplicas, true)
	if attemptedStart {
		return err
	}

	// Open and clean. Close and start with one (because they could become dirty before we close).
	attemptedStart, err = c.startWithOne(openCleanReplicas, true)
	if attemptedStart {
		return err
	}

	// Rebuilding and closed. Start with one.
	attemptedStart, err = c.startWithOne(rebuildingClosedReplicas, false)
	if attemptedStart {
		return err
	}

	// Rebuilding and open. Close and start with one.
	attemptedStart, err = c.startWithOne(rebuildingOpenReplicas, true)
	if attemptedStart {
		return err
	}

	// Initial. Start with all
	attemptedStart, err = c.startWithAll(initializingReplicas, true)
	if attemptedStart {
		return err
	}

	return fmt.Errorf("Couldn't find any valid replicas to start with. Original replicas from metadata: %v", replicaMetadata)
}

func (c *Controller) startWithAll(replicas map[string]*replica, create bool) (bool, error) {
	addresses := []string{}
	for address, replica := range replicas {
		if create {
			logrus.Infof("Create replica %v", address)
			if err := replica.client.Create(replica.size); err != nil {
				logrus.Errorf("Error creating replica %v: %v. It won't be used to start controller.", address, err)
				continue
			}
		}
		addresses = append(addresses, address)
	}
	if len(addresses) > 0 {
		logrus.Infof("Starting controller with replicas: %v.", addresses)
		return true, c.client.Start(addresses...)
	}
	return false, nil
}

// Start the controller with a single replica from the provided map. If the map is bigger than one, will try with each replica.
// Return bool indicates if the controller attempted to start.
func (c *Controller) startWithOne(replicas map[string]*replica, close bool) (bool, error) {
	returnErrors := []error{}
	for addr, replica := range replicas {
		if close {
			logrus.Infof("Closing replica %v", addr)
			if err := replica.client.Close(); err != nil {
				logrus.Errorf("Error closing replica %v: %v. It won't be used to start controller.", addr, err)
				continue
			}
		}

		logrus.Infof("Starting controller with replica: %v.", addr)
		if err := c.client.Start(addr); err != nil {
			returnErrors = append(returnErrors, fmt.Errorf("%v: %v", addr, err))
		} else {
			return true, nil
		}
	}

	var err error
	if len(returnErrors) > 0 {
		err = fmt.Errorf("Enountered %v errors trying to start controller. Errors: %v", len(returnErrors), returnErrors)
	}
	return err != nil, err
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

	_, fromMetadata, err := c.replicaMetadataAndClient()
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

func (c *Controller) addReplica(r *replica) error {
	replica, err := r.client.GetReplica()
	if err != nil {
		return fmt.Errorf("Error getting replica %v before adding: %v", r.host, err)
	}

	if replica.State == "initial" {
		err := r.client.Create(r.size)
		if err != nil {
			return fmt.Errorf("Error opening replica %v before adding: %v", r.host, err)
		}
	} else if _, ok := replica.Actions["close"]; ok {
		err := r.client.Close()
		if err != nil {
			return fmt.Errorf("Error closing replica %v before adding: %v", r.host, err)
		}
	}

	cmd := exec.Command("longhorn", "add", ReplicaAddress(r.host, r.port))
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	return cmd.Run()
}

func (c *Controller) replicaMetadataAndClient() (int, map[string]*replica, error) {
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

	result := map[string]*replica{}
	for _, container := range containers {
		r := &replica{
			healthState: container.HealthState,
			host:        container.PrimaryIp,
			port:        9502,
			size:        size,
		}

		address := ReplicaAddress(r.host, r.port)
		replicaClient, err := lclient.NewReplicaClient(address)
		if err != nil {
			return 0, nil, fmt.Errorf("Error getting client for replica %v: %v", address, err)
		}
		r.client = replicaClient
		result[address] = r
	}

	return service.Scale, result, nil
}
