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

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	replicarpc "github.com/longhorn/longhorn-engine/replica/rpc"
	syncagentrpc "github.com/longhorn/longhorn-engine/sync/rpc"
	"github.com/longhorn/longhorn-engine/types"
)

const (
	GRPCServiceCommonTimeout = 1 * time.Minute
	GRPCServiceLongTimeout   = 24 * time.Hour
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

	replicaServiceURL := strings.Replace(address, fmt.Sprintf(":%d", port), fmt.Sprintf(":%d", port+3), -1)
	replicaServiceURL = strings.TrimPrefix(replicaServiceURL, "http://")
	replicaServiceURL = strings.TrimSuffix(replicaServiceURL, "/v1")

	return &ReplicaClient{
		host:                parts[0],
		address:             address,
		httpClient:          client,
		syncAgentServiceURL: syncAgentServiceURL,
		replicaServiceURL:   replicaServiceURL,
	}, nil
}

func GetDiskInfo(info *replicarpc.DiskInfo) *types.DiskInfo {
	diskInfo := &types.DiskInfo{
		Name:        info.Name,
		Parent:      info.Parent,
		Children:    info.Children,
		Removed:     info.Removed,
		UserCreated: info.UserCreated,
		Created:     info.Created,
		Size:        info.Size,
		Labels:      info.Labels,
	}

	if diskInfo.Labels == nil {
		diskInfo.Labels = map[string]string{}
	}

	return diskInfo
}

func GetReplicaInfo(r *replicarpc.Replica) *types.ReplicaInfo {
	replicaInfo := &types.ReplicaInfo{
		Dirty:           r.Dirty,
		Rebuilding:      r.Rebuilding,
		Head:            r.Head,
		Parent:          r.Parent,
		Size:            r.Size,
		SectorSize:      r.SectorSize,
		BackingFile:     r.BackingFile,
		State:           r.State,
		Chain:           r.Chain,
		Disks:           map[string]types.DiskInfo{},
		RemainSnapshots: int(r.RemainSnapshots),
		RevisionCounter: r.RevisionCounter,
	}

	for diskName, diskInfo := range r.Disks {
		replicaInfo.Disks[diskName] = *GetDiskInfo(diskInfo)
	}

	return replicaInfo
}

func (c *ReplicaClient) GetReplica() (*types.ReplicaInfo, error) {
	conn, err := grpc.Dial(c.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to ReplicaService %v: %v", c.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := replicarpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	replica, err := replicaServiceClient.ReplicaGet(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to get replica %v: %v", c.replicaServiceURL, err)
	}

	return GetReplicaInfo(replica), nil
}

func (c *ReplicaClient) OpenReplica() error {
	conn, err := grpc.Dial(c.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", c.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := replicarpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaOpen(ctx, &empty.Empty{}); err != nil {
		return fmt.Errorf("failed to open replica %v: %v", c.replicaServiceURL, err)
	}

	return nil
}

func (c *ReplicaClient) Close() error {
	conn, err := grpc.Dial(c.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", c.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := replicarpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaClose(ctx, &empty.Empty{}); err != nil {
		return fmt.Errorf("failed to close replica %v: %v", c.replicaServiceURL, err)
	}

	return nil
}

func (c *ReplicaClient) ReloadReplica() (*replicarpc.Replica, error) {
	conn, err := grpc.Dial(c.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to ReplicaService %v: %v", c.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := replicarpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	replica, err := replicaServiceClient.ReplicaReload(ctx, &empty.Empty{})
	if err != nil {
		return nil, fmt.Errorf("failed to reload replica %v: %v", c.replicaServiceURL, err)
	}

	return replica, nil
}

func (c *ReplicaClient) Revert(name, created string) error {
	conn, err := grpc.Dial(c.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", c.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := replicarpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.ReplicaRevert(ctx, &replicarpc.ReplicaRevertRequest{
		Name:    name,
		Created: created,
	}); err != nil {
		return fmt.Errorf("failed to revert replica %v: %v", c.replicaServiceURL, err)
	}

	return nil
}

func (c *ReplicaClient) RemoveDisk(disk string, force bool) error {
	conn, err := grpc.Dial(c.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", c.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := replicarpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.DiskRemove(ctx, &replicarpc.DiskRemoveRequest{
		Name:  disk,
		Force: force,
	}); err != nil {
		return fmt.Errorf("failed to remove disk %v for replica %v: %v", disk, c.replicaServiceURL, err)
	}

	return nil
}

func (c *ReplicaClient) ReplaceDisk(target, source string) error {
	conn, err := grpc.Dial(c.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", c.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := replicarpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.DiskReplace(ctx, &replicarpc.DiskReplaceRequest{
		Target: target,
		Source: source,
	}); err != nil {
		return fmt.Errorf("failed to replace disk %v with %v for replica %v: %v", target, source, c.replicaServiceURL, err)
	}

	return nil
}

func (c *ReplicaClient) PrepareRemoveDisk(disk string) ([]*replicarpc.PrepareRemoveAction, error) {
	conn, err := grpc.Dial(c.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("cannot connect to ReplicaService %v: %v", c.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := replicarpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	reply, err := replicaServiceClient.DiskPrepareRemove(ctx, &replicarpc.DiskPrepareRemoveRequest{
		Name: disk,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to prepare removing disk %v for replica %v: %v", disk, c.replicaServiceURL, err)
	}

	return reply.Operations, nil
}

func (c *ReplicaClient) MarkDiskAsRemoved(disk string) error {
	conn, err := grpc.Dial(c.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", c.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := replicarpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.DiskMarkAsRemoved(ctx, &replicarpc.DiskMarkAsRemovedRequest{
		Name: disk,
	}); err != nil {
		return fmt.Errorf("failed to mark disk %v as removed for replica %v: %v", disk, c.replicaServiceURL, err)
	}

	return nil
}

func (c *ReplicaClient) SetRebuilding(rebuilding bool) error {
	conn, err := grpc.Dial(c.replicaServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to ReplicaService %v: %v", c.replicaServiceURL, err)
	}
	defer conn.Close()
	replicaServiceClient := replicarpc.NewReplicaServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := replicaServiceClient.RebuildingSet(ctx, &replicarpc.RebuildingSetRequest{
		Rebuilding: rebuilding,
	}); err != nil {
		return fmt.Errorf("failed to set rebuilding to %v for replica %v: %v", rebuilding, c.replicaServiceURL, err)
	}

	return nil
}

func (c *ReplicaClient) RemoveFile(file string) error {
	conn, err := grpc.Dial(c.syncAgentServiceURL, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("cannot connect to SyncAgentService %v: %v", c.syncAgentServiceURL, err)
	}
	defer conn.Close()
	syncAgentServiceClient := syncagentrpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := syncAgentServiceClient.FileRemove(ctx, &syncagentrpc.FileRemoveRequest{
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
	syncAgentServiceClient := syncagentrpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := syncAgentServiceClient.FileRename(ctx, &syncagentrpc.FileRenameRequest{
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
	syncAgentServiceClient := syncagentrpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceLongTimeout)
	defer cancel()

	if _, err := syncAgentServiceClient.FileCoalesce(ctx, &syncagentrpc.FileCoalesceRequest{
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
	syncAgentServiceClient := syncagentrpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceLongTimeout)
	defer cancel()

	if _, err := syncAgentServiceClient.FileSend(ctx, &syncagentrpc.FileSendRequest{
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
	syncAgentServiceClient := syncagentrpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceLongTimeout)
	defer cancel()

	reply, err := syncAgentServiceClient.ReceiverLaunch(ctx, &syncagentrpc.ReceiverLaunchRequest{
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
	syncAgentServiceClient := syncagentrpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceLongTimeout)
	defer cancel()

	reply, err := syncAgentServiceClient.BackupCreate(ctx, &syncagentrpc.BackupCreateRequest{
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
	syncAgentServiceClient := syncagentrpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceCommonTimeout)
	defer cancel()

	if _, err := syncAgentServiceClient.BackupRemove(ctx, &syncagentrpc.BackupRemoveRequest{
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
	syncAgentServiceClient := syncagentrpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceLongTimeout)
	defer cancel()

	if _, err := syncAgentServiceClient.BackupRestore(ctx, &syncagentrpc.BackupRestoreRequest{
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
	syncAgentServiceClient := syncagentrpc.NewSyncAgentServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceLongTimeout)
	defer cancel()

	if _, err := syncAgentServiceClient.BackupRestoreIncrementally(ctx, &syncagentrpc.BackupRestoreIncrementallyRequest{
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
