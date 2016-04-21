package remote

import (
	"encoding/json"
	"fmt"
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
	name       string
	replicaURL string
	httpClient *http.Client
}

func (r *Remote) Close() error {
	logrus.Infof("Closing: %s", r.name)
	return r.doAction("close")
}

func (r *Remote) Snapshot() error {
	logrus.Infof("Snapshot: %s", r.name)
	return r.doAction("snapshot")
}

func (r *Remote) doAction(action string) error {
	req, err := http.NewRequest("POST", r.replicaURL+"?action="+action, nil)
	if err != nil {
		return err
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
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}

	replica, err := r.info()
	if err != nil {
		return nil, err
	}

	if replica.State != rest.StateOpen {
		return nil, fmt.Errorf("Can not add replica in state: %s", replica.State)
	}

	conn, err := net.Dial("tcp", dataAddress)
	if err != nil {
		return nil, err
	}

	r.ReaderWriterAt = rpc.NewClient(conn)

	return r, nil
}
