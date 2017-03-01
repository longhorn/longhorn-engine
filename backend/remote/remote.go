package remote

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn/replica/rest"
	"github.com/rancher/longhorn/rpc"
	"github.com/rancher/longhorn/types"
	"github.com/rancher/longhorn/util"
)

var (
	pingInveral   = 2 * time.Second
	timeout       = 30 * time.Second
	requestBuffer = 1024
)

func New() types.BackendFactory {
	return &Factory{}
}

type Factory struct {
}

type Remote struct {
	types.ReaderWriterAt
	name        string
	pingURL     string
	replicaURL  string
	httpClient  *http.Client
	closeChan   chan struct{}
	monitorChan types.MonitorChannel
}

func (r *Remote) Close() error {
	logrus.Infof("Closing: %s", r.name)
	r.StopMonitoring()
	return r.doAction("close", nil)
}

func (r *Remote) open() error {
	logrus.Infof("Opening: %s", r.name)
	return r.doAction("open", nil)
}

func (r *Remote) Snapshot(name string, userCreated bool) error {
	logrus.Infof("Snapshot: %s %s UserCreated %v", r.name, name, userCreated)
	return r.doAction("snapshot",
		&map[string]interface{}{"name": name, "usercreated": userCreated})
}

func (r *Remote) doAction(action string, obj interface{}) error {
	body := io.Reader(nil)
	if obj != nil {
		buffer := &bytes.Buffer{}
		if err := json.NewEncoder(buffer).Encode(obj); err != nil {
			return err
		}
		body = buffer
	}

	req, err := http.NewRequest("POST", r.replicaURL+"?action="+action, body)
	if err != nil {
		return err
	}

	if obj != nil {
		req.Header.Add("Content-Type", "application/json")
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Bad status: %d %s", resp.StatusCode, resp.Status)
	}

	return nil
}

func (r *Remote) Size() (int64, error) {
	replica, err := r.info()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(replica.Size, 10, 0)
}

func (r *Remote) SectorSize() (int64, error) {
	replica, err := r.info()
	if err != nil {
		return 0, err
	}
	return replica.SectorSize, nil
}

func (r *Remote) RemainSnapshots() (int, error) {
	replica, err := r.info()
	if err != nil {
		return 0, err
	}
	if replica.State != "open" && replica.State != "dirty" && replica.State != "rebuilding" {
		return 0, fmt.Errorf("Invalid state %v for counting snapshots", replica.State)
	}
	return replica.RemainSnapshots, nil
}

func (r *Remote) GetRevisionCounter() (int64, error) {
	replica, err := r.info()
	if err != nil {
		return 0, err
	}
	if replica.State != "open" && replica.State != "dirty" {
		return 0, fmt.Errorf("Invalid state %v for getting revision counter", replica.State)
	}
	return replica.RevisionCounter, nil
}

func (r *Remote) SetRevisionCounter(counter int64) error {
	logrus.Infof("Set revision counter of %s to : %v", r.name, counter)
	return r.doAction("setrevisioncounter", &map[string]int64{"counter": counter})
}

func (r *Remote) info() (rest.Replica, error) {
	var replica rest.Replica
	req, err := http.NewRequest("GET", r.replicaURL, nil)
	if err != nil {
		return replica, err
	}

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return replica, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return replica, fmt.Errorf("Bad status: %d %s", resp.StatusCode, resp.Status)
	}

	err = json.NewDecoder(resp.Body).Decode(&replica)
	return replica, err
}

func (rf *Factory) Create(address string) (types.Backend, error) {
	logrus.Infof("Connecting to remote: %s", address)

	controlAddress, dataAddress, _, err := util.ParseAddresses(address)
	if err != nil {
		return nil, err
	}

	r := &Remote{
		name:       address,
		replicaURL: fmt.Sprintf("http://%s/v1/replicas/1", controlAddress),
		pingURL:    fmt.Sprintf("http://%s/ping", controlAddress),
		httpClient: &http.Client{
			Timeout: timeout,
		},
		// We don't want sender to wait for receiver, because receiver may
		// has been already notified
		closeChan:   make(chan struct{}, 5),
		monitorChan: make(types.MonitorChannel, 5),
	}

	replica, err := r.info()
	if err != nil {
		return nil, err
	}

	if replica.State != "closed" {
		return nil, fmt.Errorf("Replica must be closed, Can not add in state: %s", replica.State)
	}

	conn, err := net.Dial("tcp", dataAddress)
	if err != nil {
		return nil, err
	}

	rpc := rpc.NewClient(conn)
	r.ReaderWriterAt = rpc

	if err := r.open(); err != nil {
		return nil, err
	}

	go r.monitorPing(rpc)

	return r, nil
}

func (r *Remote) monitorPing(client *rpc.Client) {
	ticker := time.NewTicker(pingInveral)
	defer ticker.Stop()

	for {
		select {
		case <-r.closeChan:
			r.monitorChan <- nil
			return
		case <-ticker.C:
			if err := client.Ping(); err != nil {
				client.SetError(err)
				r.monitorChan <- err
				return
			}
		}
	}
}

func (r *Remote) GetMonitorChannel() types.MonitorChannel {
	return r.monitorChan
}

func (r *Remote) StopMonitoring() {
	r.closeChan <- struct{}{}
}
