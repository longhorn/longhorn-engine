package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"

	"github.com/rancher/go-rancher-metadata/metadata"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/longhorn/agent/controller"
	"github.com/rancher/longhorn/agent/replica/rest"
)

// TODO Add logic to purge old entries from these maps
var restoreMutex = &sync.RWMutex{}
var restoreMap = make(map[string]*status)

var backupMutex = &sync.RWMutex{}
var backupMap = make(map[string]*status)

func (s *Server) CreateBackup(rw http.ResponseWriter, req *http.Request) error {
	logrus.Infof("Creating backup")

	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	snapshot, err := s.getSnapshot(apiContext, id)
	if err != nil {
		return err
	}

	if snapshot == nil {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	var input backupInput
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	if input.UUID == "" {
		rw.WriteHeader(http.StatusBadRequest)
		return nil
	}

	if status := checkStatus(input.UUID, backupMap, backupMutex); status != nil {
		return apiContext.WriteResource(status)
	}

	if err := prepareBackupTarget(input.BackupTarget); err != nil {
		return err
	}

	destination := fmt.Sprintf("vfs:///var/lib/rancher/longhorn/backups/%v/%v", input.BackupTarget.Name, input.BackupTarget.UUID)
	status, err := backup(input.UUID, snapshot.Id, destination)
	if err != nil {
		return err
	}

	return apiContext.WriteResource(status)
}

func (s *Server) RemoveBackup(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	var input locationInput
	if err := apiContext.Read(&input); err != nil {
		return err
	}
	logrus.Infof("Removing backup %#v", input)

	if input.Location == "" {
		rw.WriteHeader(http.StatusBadRequest)
		return nil
	}

	if err := prepareBackupTarget(input.BackupTarget); err != nil {
		return err
	}

	exists, err := backupExists(input)
	if err != nil {
		return fmt.Errorf("Error while determining if backup exists: %v", err)
	}

	if !exists {
		rw.WriteHeader(http.StatusNoContent)
		return nil
	}

	cmd := exec.Command("longhorn", "backup", "rm", input.Location)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	logrus.Infof("Running %v", cmd.Args)
	if err := cmd.Run(); err != nil {
		return err
	}

	rw.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) RestoreFromBackup(rw http.ResponseWriter, req *http.Request) error {
	logrus.Infof("Restoring from backup")

	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	if id != "1" {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	var input locationInput
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	if input.Location == "" || input.UUID == "" {
		rw.WriteHeader(http.StatusBadRequest)
		return nil
	}

	if status := checkStatus(input.UUID, restoreMap, restoreMutex); status != nil {
		return apiContext.WriteResource(status)
	}

	if err := prepareBackupTarget(input.BackupTarget); err != nil {
		return err
	}

	restoreStatus, err := restore(input.UUID, input.Location)
	if err != nil {
		return err
	}

	return apiContext.WriteResource(restoreStatus)
}

func restore(uuid, location string) (*status, error) {
	cmd := exec.Command("longhorn", "backup", "restore", location)
	return doStatusBackedCommand(uuid, "restorestatus", cmd, restoreMap, restoreMutex)
}

func backup(uuid, snapshot, destination string) (*status, error) {
	cmd := exec.Command("longhorn", "backup", "create", snapshot, "--dest", destination)
	return doStatusBackedCommand(uuid, "backupstatus", cmd, backupMap, backupMutex)
}

func doStatusBackedCommand(id, resourceType string, command *exec.Cmd, statusMap map[string]*status, statusMutex *sync.RWMutex) (*status, error) {
	output := new(bytes.Buffer)
	command.Stdout = output
	command.Stderr = os.Stderr
	err := command.Start()
	if err != nil {
		return &status{}, err
	}

	statusMutex.Lock()
	defer statusMutex.Unlock()
	status := newStatus(id, "running", "", resourceType)
	statusMap[id] = status

	go func(id string, c *exec.Cmd) {
		var message string
		var state string

		err := c.Wait()
		if err != nil {
			state = "error"
			message = fmt.Sprintf("Error: %v", err)
		} else {
			state = "done"
			message = output.String()
		}

		statusMutex.Lock()
		defer statusMutex.Unlock()
		status, ok := statusMap[id]
		if !ok {
			status = newStatus(id, "", "", resourceType)
		}

		status.State = state
		status.Message = message
		restoreMap[id] = status
	}(id, command)

	return status, nil
}

func (s *Server) GetBackupStatus(rw http.ResponseWriter, req *http.Request) error {
	logrus.Infof("Getting backup status")
	return getStatus(backupMap, backupMutex, rw, req)
}

func (s *Server) GetRestoreStatus(rw http.ResponseWriter, req *http.Request) error {
	logrus.Infof("Getting restore status")
	return getStatus(restoreMap, restoreMutex, rw, req)
}

func checkStatus(id string, statusMap map[string]*status, statusMutex *sync.RWMutex) *status {
	statusMutex.RLock()
	defer statusMutex.RUnlock()
	return statusMap[id]
}

func getStatus(statusMap map[string]*status, statusMutex *sync.RWMutex, rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["id"]

	statusMutex.RLock()
	defer statusMutex.RUnlock()

	status, ok := statusMap[id]
	if !ok {
		rw.WriteHeader(http.StatusNotFound)
		return nil
	}

	return apiContext.WriteResource(status)
}

func backupExists(location locationInput) (bool, error) {
	// TODO Properly implement once we have a way of knowing if a backup exists
	return true, nil
}

func prepareBackupTarget(target rest.BackupTarget) error {
	replicas, err := replicRestEndpoints()
	if err != nil {
		return fmt.Errorf("Error getting replica endpoints: %v", err)
	}

	b, err := json.Marshal(target)
	if err != nil {
		return err
	}

	// This could be optimized with some goroutines and a waitgroup
	for _, r := range replicas {
		url := r + "/backuptargets"
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(b))
		if err != nil {
			return err
		}

		if resp.StatusCode >= 300 {
			content, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("Bad response preparing mount for %v: %v %v - %s", r, resp.StatusCode, resp.Status, content)
		}
		resp.Body.Close()
	}

	return nil
}

func replicRestEndpoints() ([]string, error) {
	client, err := metadata.NewClientAndWait(controller.MetadataURL)
	if err != nil {
		return nil, err
	}
	service, err := client.GetSelfServiceByName("replica")
	if err != nil {
		return nil, err
	}

	containers := map[string]metadata.Container{}
	for _, container := range service.Containers {
		if c, ok := containers[container.Name]; !ok {
			containers[container.Name] = container
		} else if container.CreateIndex > c.CreateIndex {
			containers[container.Name] = container
		}
	}

	result := []string{}
	for _, container := range containers {
		endpoint := fmt.Sprintf("http://%s/v1", container.PrimaryIp)
		result = append(result, endpoint)
	}

	return result, nil
}
