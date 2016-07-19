package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn/replica/rest"
	"github.com/rancher/longhorn/sync/agent"
)

type ReplicaClient struct {
	address    string
	syncAgent  string
	host       string
	httpClient *http.Client
}

func NewReplicaClient(address string) (*ReplicaClient, error) {
	if strings.HasPrefix(address, "tcp://") {
		address = address[6:]
	}

	if !strings.HasPrefix(address, "http") {
		address = "http://" + address
	}

	if !strings.HasSuffix(address, "/v1") {
		address += "/v1"
	}

	u, err := url.Parse(address)
	if err != nil {
		return nil, err
	}

	parts := strings.Split(u.Host, ":")
	if len(parts) < 2 {
		return nil, fmt.Errorf("Invalid address %s, must have a port in it", address)
	}

	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}

	syncAgent := strings.Replace(address, fmt.Sprintf(":%d", port), fmt.Sprintf(":%d", port+2), -1)

	timeout := time.Duration(30 * time.Second)
	client := &http.Client{
		Timeout: timeout,
	}

	return &ReplicaClient{
		host:       parts[0],
		address:    address,
		syncAgent:  syncAgent,
		httpClient: client,
	}, nil
}

func (c *ReplicaClient) Create(size string) error {
	r, err := c.GetReplica()
	if err != nil {
		return err
	}

	return c.post(r.Actions["create"], rest.CreateInput{
		Size: size,
	}, nil)
}

func (c *ReplicaClient) Revert(name string) error {
	r, err := c.GetReplica()
	if err != nil {
		return err
	}

	return c.post(r.Actions["revert"], rest.RevertInput{
		Name: name,
	}, nil)
}

func (c *ReplicaClient) Close() error {
	r, err := c.GetReplica()
	if err != nil {
		return err
	}

	return c.post(r.Actions["close"], nil, nil)
}

func (c *ReplicaClient) SetRebuilding(rebuilding bool) error {
	r, err := c.GetReplica()
	if err != nil {
		return err
	}

	return c.post(r.Actions["setrebuilding"], &rest.RebuildingInput{
		Rebuilding: rebuilding,
	}, nil)
}

func (c *ReplicaClient) RemoveDisk(disk string) error {
	r, err := c.GetReplica()
	if err != nil {
		return err
	}

	return c.post(r.Actions["removedisk"], &rest.RemoveDiskInput{
		Name: disk,
	}, nil)
}

func (c *ReplicaClient) PrepareRemoveDisk(disk string) (rest.PrepareRemoveDiskOutput, error) {
	var output rest.PrepareRemoveDiskOutput
	r, err := c.GetReplica()
	if err != nil {
		return output, err
	}

	err = c.post(r.Actions["prepareremovedisk"], &rest.PrepareRemoveDiskInput{
		Name: disk,
	}, &output)
	return output, err
}

func (c *ReplicaClient) OpenReplica() error {
	r, err := c.GetReplica()
	if err != nil {
		return err
	}

	return c.post(r.Actions["open"], nil, nil)
}

func (c *ReplicaClient) GetReplica() (rest.Replica, error) {
	var replica rest.Replica

	err := c.get(c.address+"/replicas/1", &replica)
	return replica, err
}

func (c *ReplicaClient) ReloadReplica() (rest.Replica, error) {
	var replica rest.Replica

	err := c.post(c.address+"/replicas/1?action=reload", map[string]string{}, &replica)
	return replica, err
}

func (c *ReplicaClient) LaunchReceiver(toFilePath string) (string, int, error) {
	var running agent.Process
	err := c.post(c.syncAgent+"/processes", &agent.Process{
		ProcessType: "sync",
		DestFile:    toFilePath,
	}, &running)
	if err != nil {
		return "", 0, err
	}

	return c.host, running.Port, nil
}

func (c *ReplicaClient) Coalesce(from, to string) error {
	var running agent.Process
	err := c.post(c.syncAgent+"/processes", &agent.Process{
		ProcessType: "fold",
		SrcFile:     from,
		DestFile:    to,
	}, &running)
	if err != nil {
		return err
	}

	start := 250 * time.Millisecond
	for {
		err := c.get(running.Links["self"], &running)
		if err != nil {
			return err
		}

		switch running.ExitCode {
		case -2:
			time.Sleep(start)
			start = start * 2
			if start > 1*time.Second {
				start = 1 * time.Second
			}
		case 0:
			return nil
		default:
			return fmt.Errorf("ExitCode: %d", running.ExitCode)
		}
	}
}

func (c *ReplicaClient) SendFile(from, host string, port int) error {
	var running agent.Process
	err := c.post(c.syncAgent+"/processes", &agent.Process{
		ProcessType: "sync",
		Host:        host,
		SrcFile:     from,
		Port:        port,
	}, &running)
	if err != nil {
		return err
	}

	start := 250 * time.Millisecond
	for {
		err := c.get(running.Links["self"], &running)
		if err != nil {
			return err
		}

		switch running.ExitCode {
		case -2:
			time.Sleep(start)
			start = start * 2
			if start > 1*time.Second {
				start = 1 * time.Second
			}
		case 0:
			return nil
		default:
			return fmt.Errorf("ExitCode: %d", running.ExitCode)
		}
	}
}

func (c *ReplicaClient) CreateBackup(snapshot, dest, volume string) (string, error) {
	var running agent.Process
	err := c.post(c.syncAgent+"/processes", &agent.Process{
		ProcessType: "backup",
		SrcFile:     snapshot,
		DestFile:    dest,
		Host:        volume,
	}, &running)
	if err != nil {
		return "", err
	}

	start := 250 * time.Millisecond
	for {
		err := c.get(running.Links["self"], &running)
		if err != nil {
			return "", err
		}

		switch running.ExitCode {
		case -2:
			time.Sleep(start)
			start = start * 2
			if start > 1*time.Second {
				start = 1 * time.Second
			}
		case 0:
			return running.Output, nil
		default:
			return "", fmt.Errorf("ExitCode: %d, output: %v",
				running.ExitCode, running.Output)
		}
	}
}

func (c *ReplicaClient) RmBackup(backup string) error {
	var running agent.Process
	err := c.post(c.syncAgent+"/processes", &agent.Process{
		ProcessType: "rmbackup",
		SrcFile:     backup,
	}, &running)
	if err != nil {
		return err
	}

	start := 250 * time.Millisecond
	for {
		err := c.get(running.Links["self"], &running)
		if err != nil {
			return err
		}

		switch running.ExitCode {
		case -2:
			time.Sleep(start)
			start = start * 2
			if start > 1*time.Second {
				start = 1 * time.Second
			}
		case 0:
			return nil
		default:
			return fmt.Errorf("ExitCode: %d, output: %v",
				running.ExitCode, running.Output)
		}
	}
}

func (c *ReplicaClient) RestoreBackup(backup, snapshotFile string) error {
	var running agent.Process
	err := c.post(c.syncAgent+"/processes", &agent.Process{
		ProcessType: "restore",
		SrcFile:     backup,
		DestFile:    snapshotFile,
	}, &running)
	if err != nil {
		return err
	}

	start := 250 * time.Millisecond
	for {
		err := c.get(running.Links["self"], &running)
		if err != nil {
			return err
		}

		switch running.ExitCode {
		case -2:
			time.Sleep(start)
			start = start * 2
			if start > 1*time.Second {
				start = 1 * time.Second
			}
		case 0:
			return nil
		default:
			return fmt.Errorf("ExitCode: %d, output: %v",
				running.ExitCode, running.Output)
		}
	}
}

func (c *ReplicaClient) InspectBackup(backup string) (string, error) {
	var running agent.Process
	err := c.post(c.syncAgent+"/processes", &agent.Process{
		ProcessType: "inspectbackup",
		SrcFile:     backup,
	}, &running)
	if err != nil {
		return "", err
	}

	start := 250 * time.Millisecond
	for {
		err := c.get(running.Links["self"], &running)
		if err != nil {
			return "", err
		}

		switch running.ExitCode {
		case -2:
			time.Sleep(start)
			start = start * 2
			if start > 1*time.Second {
				start = 1 * time.Second
			}
		case 0:
			return running.Output, nil
		default:
			return "", fmt.Errorf("ExitCode: %d, output: %v",
				running.ExitCode, running.Output)
		}
	}
}

func (c *ReplicaClient) get(url string, obj interface{}) error {
	if !strings.HasPrefix(url, "http") {
		url = c.address + url
	}

	resp, err := c.httpClient.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return json.NewDecoder(resp.Body).Decode(obj)
}

func (c *ReplicaClient) post(path string, req, resp interface{}) error {
	b, err := json.Marshal(req)
	if err != nil {
		return err
	}

	bodyType := "application/json"
	url := path
	if !strings.HasPrefix(url, "http") {
		url = c.address + path
	}

	logrus.Debugf("POST %s", url)

	httpResp, err := c.httpClient.Post(url, bodyType, bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode >= 300 {
		content, _ := ioutil.ReadAll(httpResp.Body)
		return fmt.Errorf("Bad response: %d %s: %s", httpResp.StatusCode, httpResp.Status, content)
	}

	if resp == nil {
		return nil
	}

	return json.NewDecoder(httpResp.Body).Decode(resp)
}

func (c *ReplicaClient) HardLink(from, to string) error {
	var running agent.Process
	err := c.post(c.syncAgent+"/processes", &agent.Process{
		ProcessType: "hardlink",
		SrcFile:     from,
		DestFile:    to,
	}, &running)
	if err != nil {
		return err
	}

	start := 250 * time.Millisecond
	for {
		err := c.get(running.Links["self"], &running)
		if err != nil {
			return err
		}

		switch running.ExitCode {
		case -2:
			time.Sleep(start)
			start = start * 2
			if start > 1*time.Second {
				start = 1 * time.Second
			}
		case 0:
			return nil
		default:
			return fmt.Errorf("ExitCode: %d", running.ExitCode)
		}
	}
}
