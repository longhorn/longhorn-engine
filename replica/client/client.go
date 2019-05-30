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

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-engine/replica/rest"
	"github.com/longhorn/longhorn-engine/sync/rpc"
)

const (
	SyncAgentServiceCommonTimeout = 1 * time.Minute
	SyncAgentServiceLongTimeout   = 24 * time.Hour
)

type ReplicaClient struct {
	address             string
	host                string
	httpClient          *http.Client
	syncAgentServiceURL string
	replicaServiceURL   string
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

	timeout := time.Duration(30 * time.Second)
	client := &http.Client{
		Timeout: timeout,
	}

	syncAgentServiceURL := strings.Replace(address, fmt.Sprintf(":%d", port), fmt.Sprintf(":%d", port+2), -1)
	syncAgentServiceURL = strings.TrimPrefix(syncAgentServiceURL, "http://")
	syncAgentServiceURL = strings.TrimSuffix(syncAgentServiceURL, "/v1")

	replicaServiceURL := strings.Replace(syncAgentServiceURL, fmt.Sprintf(":%d", port), fmt.Sprintf(":%d", port+3), -1)

	return &ReplicaClient{
		host:                parts[0],
		address:             address,
		httpClient:          client,
		syncAgentServiceURL: syncAgentServiceURL,
		replicaServiceURL:   replicaServiceURL,
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

func (c *ReplicaClient) Revert(name, created string) error {
	r, err := c.GetReplica()
	if err != nil {
		return err
	}

	return c.post(r.Actions["revert"], rest.RevertInput{
		Name:    name,
		Created: created,
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

func (c *ReplicaClient) RemoveDisk(disk string, force bool) error {
	r, err := c.GetReplica()
	if err != nil {
		return err
	}

	return c.post(r.Actions["removedisk"], &rest.RemoveDiskInput{
		Name:  disk,
		Force: force,
	}, nil)
}

func (c *ReplicaClient) ReplaceDisk(target, source string) error {
	r, err := c.GetReplica()
	if err != nil {
		return err
	}

	return c.post(r.Actions["replacedisk"], &rest.ReplaceDiskInput{
		Target: target,
		Source: source,
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

func (c *ReplicaClient) MarkDiskAsRemoved(disk string) error {
	r, err := c.GetReplica()
	if err != nil {
		return err
	}

	return c.post(r.Actions["markdiskasremoved"], &rest.MarkDiskAsRemovedInput{
		Name: disk,
	}, nil)
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

func (c *ReplicaClient) RemoveFile(file string) error {
	conn, err := grpc.Dial(c.syncAgentServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to SyncAgentService %v: %v", c.syncAgentServiceURL, err)
	}
	defer conn.Close()
	syncAgentServiceClient := rpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), SyncAgentServiceCommonTimeout)
	defer cancel()

	if _, err := syncAgentServiceClient.FileRemove(ctx, &rpc.FileRemoveRequest{
		FileName: file,
	}); err != nil {
		return fmt.Errorf("failed to remove file %v: %v", file, err)
	}

	return nil
}

func (c *ReplicaClient) RenameFile(oldFileName, newFileName string) error {
	conn, err := grpc.Dial(c.syncAgentServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to SyncAgentService %v: %v", c.syncAgentServiceURL, err)
	}
	defer conn.Close()
	syncAgentServiceClient := rpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), SyncAgentServiceCommonTimeout)
	defer cancel()

	if _, err := syncAgentServiceClient.FileRename(ctx, &rpc.FileRenameRequest{
		OldFileName: oldFileName,
		NewFileName: newFileName,
	}); err != nil {
		return fmt.Errorf("failed to rename or replace old file %v with new file %v: %v", oldFileName, newFileName, err)
	}

	return nil
}

func (c *ReplicaClient) CoalesceFile(from, to string) error {
	conn, err := grpc.Dial(c.syncAgentServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to SyncAgentService %v: %v", c.syncAgentServiceURL, err)
	}
	defer conn.Close()
	syncAgentServiceClient := rpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), SyncAgentServiceLongTimeout)
	defer cancel()

	if _, err := syncAgentServiceClient.FileCoalesce(ctx, &rpc.FileCoalesceRequest{
		FromFileName: from,
		ToFileName:   to,
	}); err != nil {
		return fmt.Errorf("failed to coalesce file %v to file %v: %v", from, to, err)
	}

	return nil
}

func (c *ReplicaClient) SendFile(from, host string, port int32) error {
	conn, err := grpc.Dial(c.syncAgentServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to SyncAgentService %v: %v", c.syncAgentServiceURL, err)
	}
	defer conn.Close()
	syncAgentServiceClient := rpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), SyncAgentServiceLongTimeout)
	defer cancel()

	if _, err := syncAgentServiceClient.FileSend(ctx, &rpc.FileSendRequest{
		FromFileName: from,
		Host:         host,
		Port:         port,
	}); err != nil {
		return fmt.Errorf("failed to send file %v to %v:%v: %v", from, host, port, err)
	}

	return nil
}

func (c *ReplicaClient) LaunchReceiver(toFilePath string) (string, int32, error) {
	conn, err := grpc.Dial(c.syncAgentServiceURL, grpc.WithInsecure())
	if err != nil {
		return "", 0, fmt.Errorf("cannot connect to SyncAgentService %v: %v", c.syncAgentServiceURL, err)
	}
	defer conn.Close()
	syncAgentServiceClient := rpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), SyncAgentServiceLongTimeout)
	defer cancel()

	reply, err := syncAgentServiceClient.ReceiverLaunch(ctx, &rpc.ReceiverLaunchRequest{
		ToFileName: toFilePath,
	})
	if err != nil {
		return "", 0, fmt.Errorf("failed to launch receiver for %v: %v", toFilePath, err)
	}

	return c.host, reply.Port, nil
}

func (c *ReplicaClient) CreateBackup(snapshot, dest, volume string, labels []string, credential map[string]string) (string, error) {
	conn, err := grpc.Dial(c.syncAgentServiceURL, grpc.WithInsecure())
	if err != nil {
		return "", fmt.Errorf("cannot connect to SyncAgentService %v: %v", c.syncAgentServiceURL, err)
	}
	defer conn.Close()
	syncAgentServiceClient := rpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), SyncAgentServiceLongTimeout)
	defer cancel()

	reply, err := syncAgentServiceClient.BackupCreate(ctx, &rpc.BackupCreateRequest{
		SnapshotFileName: snapshot,
		BackupTarget:     dest,
		VolumeName:       volume,
		Labels:           labels,
		Credential:       credential,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create backup to %v for volume %v: %v", dest, volume, err)
	}

	return reply.Backup, nil
}

func (c *ReplicaClient) RmBackup(backup string) error {
	conn, err := grpc.Dial(c.syncAgentServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to SyncAgentService %v: %v", c.syncAgentServiceURL, err)
	}
	defer conn.Close()
	syncAgentServiceClient := rpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), SyncAgentServiceCommonTimeout)
	defer cancel()

	if _, err := syncAgentServiceClient.BackupRemove(ctx, &rpc.BackupRemoveRequest{
		Backup: backup,
	}); err != nil {
		return fmt.Errorf("failed to remove backup %v: %v", backup, err)
	}

	return nil
}

func (c *ReplicaClient) RestoreBackup(backup, snapshotFile string) error {
	conn, err := grpc.Dial(c.syncAgentServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to SyncAgentService %v: %v", c.syncAgentServiceURL, err)
	}
	defer conn.Close()
	syncAgentServiceClient := rpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), SyncAgentServiceLongTimeout)
	defer cancel()

	if _, err := syncAgentServiceClient.BackupRestore(ctx, &rpc.BackupRestoreRequest{
		Backup:           backup,
		SnapshotFileName: snapshotFile,
	}); err != nil {
		return fmt.Errorf("failed to restore backup %v to snapshot %v: %v", backup, snapshotFile, err)
	}

	return nil
}

func (c *ReplicaClient) RestoreBackupIncrementally(backup, deltaFile, lastRestored string) error {
	conn, err := grpc.Dial(c.syncAgentServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to SyncAgentService %v: %v", c.syncAgentServiceURL, err)
	}
	defer conn.Close()
	syncAgentServiceClient := rpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), SyncAgentServiceLongTimeout)
	defer cancel()

	if _, err := syncAgentServiceClient.BackupRestoreIncrementally(ctx, &rpc.BackupRestoreIncrementallyRequest{
		Backup:                 backup,
		DeltaFileName:          deltaFile,
		LastRestoredBackupName: lastRestored,
	}); err != nil {
		return fmt.Errorf("failed to incrementally restore backup %v to file %v: %v", backup, deltaFile, err)
	}

	return nil
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

	logrus.Infof("POST %s", url)

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
