package sparse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/types"
	"github.com/longhorn/sparse-tools/util"
)

const (
	httpClientTimeout      = 5
	numBlocksInBatch       = 32
	defaultSyncWorkerCount = 4
)

type DataSyncClient interface {
	open() error
	close()
	syncHoleInterval(holeInterval Interval) error
	syncDataInterval(dataInterval Interval) error
	getServerChecksum(checksumInterval Interval) ([]byte, error)
	getServerRecordedMetadata() ([]byte, error)
}

type syncClient struct {
	remote     string
	timeout    int
	sourceName string
	size       int64
	rw         ReaderWriterAt
	directIO   bool

	recordedChangeTime     string
	recordedChecksumMethod string
	recordedChecksum       string

	httpClient *http.Client
}

type ReaderWriterAt interface {
	io.ReaderAt
	io.WriterAt

	GetDataLayout(ctx context.Context) (<-chan FileInterval, <-chan error, error)
}

func newHTTPClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 100
	t.MaxConnsPerHost = 100
	t.MaxIdleConnsPerHost = 100

	return &http.Client{
		Timeout:   httpClientTimeout * time.Second,
		Transport: t,
	}
}

func newSyncClient(remote string, timeout int, sourceName string, size int64, rw ReaderWriterAt, directIO bool,
	recordedChangeTime, recordedChecksumMethod, recordedChecksum string) *syncClient {
	return &syncClient{
		remote:     remote,
		timeout:    timeout,
		sourceName: sourceName,
		size:       size,
		rw:         rw,
		directIO:   directIO,
		httpClient: newHTTPClient(),

		recordedChangeTime:     recordedChangeTime,
		recordedChecksumMethod: recordedChecksumMethod,
		recordedChecksum:       recordedChecksum,
	}
}

// SyncFile synchronizes local file to remote host
func SyncFile(localPath string, remote string, timeout int, directIO, fastSync bool) error {
	fileInfo, err := os.Stat(localPath)
	if err != nil {
		log.WithError(err).Errorf("Failed to get file info of source file %s", localPath)
		return err
	}

	fileSize := fileInfo.Size()
	if directIO && fileSize%Blocks != 0 {
		return fmt.Errorf("invalid file size %v for directIO", fileSize)
	}

	log.Infof("Syncing file %v to %v: size %v, directIO %v, fastSync %v", localPath, remote, fileSize, directIO, fastSync)

	fileIo, err := newFileIoProcessor(localPath, directIO)
	if err != nil {
		log.WithError(err).Errorf("Failed to open local source file %v", localPath)
		return err
	}
	defer fileIo.Close()

	return SyncContent(fileIo.Name(), fileIo, fileSize, remote, timeout, directIO, fastSync)
}

func newFileIoProcessor(localPath string, directIO bool) (FileIoProcessor, error) {
	if directIO {
		return NewDirectFileIoProcessor(localPath, os.O_RDONLY, 0)
	}
	return NewBufferedFileIoProcessor(localPath, os.O_RDONLY, 0)
}

func SyncContent(sourceName string, rw ReaderWriterAt, size int64, remote string, timeout int, directIO, fastSync bool) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync content for source file %v", sourceName)
	}()

	if directIO && size%Blocks != 0 {
		return fmt.Errorf("source file %v has invalid file size %v for directIO", sourceName, size)
	}

	// Get change time and checksum from checksum file
	recordedChangeTime := ""
	recordedChecksumMethod := ""
	recordedChecksum := ""
	if filepath.Ext(sourceName) == types.SnapshotDiskSuffix {
		recordedChangeTime, recordedChecksumMethod, recordedChecksum, err = getLocalDiskFileChangeTimeAndChecksum(sourceName)
		if err != nil {
			log.WithError(err).Warnf("Failed to get change time and checksum of local file %v", sourceName)
		}
	}

	client := newSyncClient(remote, timeout, sourceName, size, rw, directIO, recordedChangeTime, recordedChecksumMethod, recordedChecksum)
	defer client.close() // kill the server no matter success or not, best effort

	if fastSync && filepath.Ext(client.sourceName) == types.SnapshotDiskSuffix {
		if client.isLocalAndRemoteDiskFilesIdentical() {
			log.Infof("Skipped syncing file %v", client.sourceName)
			return nil
		}
	}

	syncStartTime := time.Now()

	err = client.syncContent()
	if err != nil {
		return err
	}

	log.Infof("Finished sync for the source file %v, size %v, elapsed %.2fs",
		sourceName, size, time.Since(syncStartTime).Seconds())

	return nil
}

func (client *syncClient) syncContent() error {
	if err := client.open(); err != nil {
		return errors.Wrap(err, "failed to open")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fileIntervalChannel, errChannel, err := client.rw.GetDataLayout(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to get data layout for file %v", client.sourceName)
	}

	errorChannels := []<-chan error{errChannel}
	for i := 0; i < defaultSyncWorkerCount; i++ {
		errorChannels = append(errorChannels, processFileIntervals(ctx, fileIntervalChannel, client.processSegment))
	}
	// the below select will exit once all error channels are closed, or a single
	// channel has run into an error, which will lead to the ctx being cancelled
	mergedErrc := mergeErrorChannels(ctx, errorChannels...)
	select {
	case err = <-mergedErrc:
		break
	}
	return err
}

func (client *syncClient) isLocalAndRemoteDiskFilesIdentical() bool {
	if client.recordedChangeTime == "" || client.recordedChecksumMethod == "" || client.recordedChecksum == "" {
		return false
	}

	remain, err := client.isLocalDiskFileRemain()
	if err != nil {
		log.WithError(err).Warnf("Failed to check if local file %v is changed", client.sourceName)
		return false
	}
	if !remain {
		return false
	}

	remoteRecordedChecksum, err := client.getRemoteDiskFileRecordedChecksum()
	if err != nil {
		log.WithError(err).Warnf("Failed to get checksum for remote file %v", client.sourceName)
		return false
	}

	return client.recordedChecksum == remoteRecordedChecksum
}

func (client *syncClient) isLocalDiskFileRemain() (bool, error) {
	currentChangeTime, err := util.GetFileChangeTime(client.sourceName)
	if err != nil {
		return false, err
	}
	return currentChangeTime == client.recordedChangeTime, nil
}

func (client *syncClient) getRemoteDiskFileRecordedChecksum() (string, error) {
	metadata, err := client.getServerRecordedMetadata()
	if err != nil {
		return "", err
	}

	var info types.SnapshotHashInfo
	if err := json.Unmarshal(metadata, &info); err != nil {
		return "", errors.Wrap(err, "failed to unmarshal hash info")
	}

	return info.Checksum, nil
}

func getLocalDiskFileChangeTimeAndChecksum(sourceName string) (recordedChangeTime, recordedChecksumMethod, recordedChecksum string, err error) {
	f, err := os.Open(sourceName + types.DiskChecksumSuffix)
	if err != nil {
		return "", "", "", errors.Wrap(err, "failed to open checksum file")
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return "", "", "", errors.Wrap(err, "failed to read checksum file")
	}

	var info types.SnapshotHashInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return "", "", "", errors.Wrap(err, "failed to unmarshal hash info")
	}

	return info.ChangeTime, info.Method, info.Checksum, nil
}

func (client *syncClient) processSegment(segment FileInterval) error {
	switch segment.Kind {
	case SparseHole:
		if err := client.syncHoleInterval(segment.Interval); err != nil {
			return errors.Wrapf(err, "failed to sync hole interval %+v", segment.Interval)
		}
	case SparseData:
		if err := client.syncDataInterval(segment.Interval); err != nil {
			return errors.Wrapf(err, "failed to sync data interval %+v", segment.Interval)
		}
	}
	return nil
}

func (client *syncClient) sendHTTPRequest(method string, action string, queries map[string]string, data []byte) (*http.Response, error) {
	httpClient := client.httpClient
	if httpClient == nil {
		httpClient = newHTTPClient()
	}

	url := fmt.Sprintf("http://%s/v1-ssync/%s", client.remote, action)

	var req *http.Request
	var err error
	if data != nil {
		req, err = http.NewRequest(method, url, bytes.NewBuffer(data))
	} else {
		req, err = http.NewRequest(method, url, nil)
	}
	if err != nil {
		return nil, err
	}

	req.Header.Add("Accept", "application/json")

	q := req.URL.Query()
	for k, v := range queries {
		q.Add(k, v)
	}
	req.URL.RawQuery = q.Encode()

	log.Tracef("method: %s, url with query string: %s, data len: %d", method, req.URL.String(), len(data))

	return httpClient.Do(req)
}

func (client *syncClient) open() error {
	var err error
	var resp *http.Response

	timeStart := time.Now()
	timeStop := timeStart.Add(time.Duration(client.timeout) * time.Second)
	queries := make(map[string]string)
	queries["begin"] = strconv.FormatInt(0, 10)
	queries["end"] = strconv.FormatInt(client.size, 10)
	queries["directIO"] = strconv.FormatBool(client.directIO)
	for timeNow := timeStart; timeNow.Before(timeStop); timeNow = time.Now() {
		resp, err = client.sendHTTPRequest("GET", "open", queries, nil)
		if err == nil {
			break
		}
		log.Warnf("Failed to open server: %s, Retrying...", client.remote)
		if timeNow != timeStart {
			// only sleep after the second attempt to speedup tests
			time.Sleep(1 * time.Second)
		}
	}

	if err != nil {
		return errors.Wrap(err, "failed to open server")
	}

	// drain the buffer and close the body
	_, _ = io.Copy(ioutil.Discard, resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to open server: status code=%v (%v)",
			resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	return nil
}

func (client *syncClient) close() {
	queries := make(map[string]string)
	queries["checksumMethod"] = client.recordedChecksumMethod
	queries["checksum"] = client.recordedChecksum
	resp, err := client.sendHTTPRequest("POST", "close", queries, nil)
	if err == nil {
		// drain the buffer and close the body
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}

func (client *syncClient) syncHoleInterval(holeInterval Interval) error {
	queries := make(map[string]string)
	queries["begin"] = strconv.FormatInt(holeInterval.Begin, 10)
	queries["end"] = strconv.FormatInt(holeInterval.End, 10)
	resp, err := client.sendHTTPRequest("POST", "sendHole", queries, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to send hole interval %+v", holeInterval)
	}

	// drain the buffer and close the body
	_, _ = io.Copy(ioutil.Discard, resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to sync hole interval %+v: status code=%v (%v)",
			holeInterval, resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	return nil
}

func (client *syncClient) getServerChecksum(checksumInterval Interval) ([]byte, error) {
	queries := make(map[string]string)
	queries["begin"] = strconv.FormatInt(checksumInterval.Begin, 10)
	queries["end"] = strconv.FormatInt(checksumInterval.End, 10)
	resp, err := client.sendHTTPRequest("GET", "getChecksum", queries, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get checksum")
	}

	// drain the buffer and close the body
	checksum, err := ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get server checksum of interval %+v: status code=%v (%v)",
			checksumInterval, resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	return checksum, err
}

func (client *syncClient) getServerRecordedMetadata() ([]byte, error) {
	queries := make(map[string]string)
	resp, err := client.sendHTTPRequest("GET", "getRecordedMetadata", queries, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get recorded metadata")
	}

	// drain the buffer and close the body
	metadata, err := ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get recorded metadata: status code=%v (%v)",
			resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	return metadata, err
}

func (client *syncClient) writeData(dataInterval Interval, data []byte) error {
	queries := make(map[string]string)
	queries["begin"] = strconv.FormatInt(dataInterval.Begin, 10)
	queries["end"] = strconv.FormatInt(dataInterval.End, 10)
	resp, err := client.sendHTTPRequest("POST", "writeData", queries, data)
	if err != nil {
		return errors.Wrap(err, "failed to write data")
	}

	// drain the buffer and close the body
	_, _ = io.Copy(ioutil.Discard, resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to write data: status code=%v (%v)",
			resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	return nil
}

func getSize(offset, size, end int64) int64 {
	if offset+size > end {
		return end - offset
	}
	return size
}

func (client *syncClient) syncDataInterval(dataInterval Interval) error {
	batch := numBlocksInBatch * Blocks

	// Process data in chunks
	for offset := dataInterval.Begin; offset < dataInterval.End; {
		size := getSize(offset, batch, dataInterval.End)

		batchInterval := Interval{offset, offset + size}

		/*
			sync the batch data interval:

			1. Launch 2 goroutines to ask server for checksum and calculate local checksum simultaneously.
			2. Wait for checksum calculation complete then compare the checksums.
			3. Send data if the checksums are different.
		*/
		var dataBuffer, serverCheckSum, localCheckSum []byte
		var serverCksumErr, cliCksumErr error
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			defer wg.Done()
			var body []byte
			if body, serverCksumErr = client.getServerChecksum(batchInterval); serverCksumErr != nil {
				log.WithError(serverCksumErr).Errorf("failed to get checksum of interval %+v from server", batchInterval)
				return
			}
			if serverCksumErr = json.Unmarshal(body, &serverCheckSum); serverCksumErr != nil {
				log.WithError(serverCksumErr).Errorf("failed to unmarshal checksum for interval %+v from server", batchInterval)
				return
			}
		}()
		go func() {
			defer wg.Done()
			// read data for checksum and sending
			if dataBuffer, cliCksumErr = ReadDataInterval(client.rw, batchInterval); cliCksumErr != nil {
				log.WithError(cliCksumErr).Errorf("failed to read data interval %+v", batchInterval)
				return
			}
			// calculate local checksum for the data batch interval
			if localCheckSum, cliCksumErr = HashData(dataBuffer); cliCksumErr != nil {
				log.WithError(cliCksumErr).Errorf("failed to hash local data interval %+v", batchInterval)
				return
			}
		}()
		wg.Wait()
		if serverCksumErr != nil || cliCksumErr != nil {
			return fmt.Errorf("failed to get checksums for client and server, server checksum error: %v, client checksum error: %v",
				serverCksumErr, cliCksumErr)
		}

		serverNeedData := true
		if len(serverCheckSum) != 0 {
			// compare server checksum with localCheckSum
			serverNeedData = !bytes.Equal(serverCheckSum, localCheckSum)
		}
		if serverNeedData {
			// send data buffer
			log.Tracef("Sending dataBuffer size: %d", len(dataBuffer))
			if err := client.writeData(batchInterval, dataBuffer); err != nil {
				log.WithError(err).Errorf("Failed to write data interval %+v", batchInterval)
				return err
			}
		}
		offset += batchInterval.Len()
	}
	return nil
}
