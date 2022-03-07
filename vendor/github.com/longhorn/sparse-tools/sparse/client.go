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
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	httpClientTimeout = 5
	numBlocksInBatch  = 32
)

type syncClient struct {
	remote   string
	timeout  int
	sourceName string
	size int64
	rw ReaderWriterAt
	directIO bool

	httpClient *http.Client
}

type ReaderWriterAt interface {
	io.ReaderAt
	io.WriterAt

	GetDataLayout (ctx context.Context) (<-chan FileInterval, <-chan error, error)
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

func newSyncClient(remote string, timeout int, sourceName string, size int64, rw ReaderWriterAt, directIO bool) *syncClient {
	return &syncClient{remote: remote, timeout: timeout, sourceName: sourceName, size:size, rw: rw, directIO: directIO, httpClient: newHTTPClient()}
}

const connectionRetries = 5

// SyncFile synchronizes local file to remote host
func SyncFile(localPath string, remote string, timeout int, directIO bool) error {
	fileInfo, err := os.Stat(localPath)
	if err != nil {
		log.Errorf("Failed to get size of source file: %s, err: %s", localPath, err)
		return err
	}
	fileSize := fileInfo.Size()
	if directIO && fileSize%Blocks != 0 {
		return fmt.Errorf("Invalid directIO file block size: %v", fileSize)
	}
	log.Infof("source file size: %d, setting up directIO: %v", fileSize, directIO)

	var fileIo FileIoProcessor
	if directIO {
		fileIo, err = NewDirectFileIoProcessor(localPath, os.O_RDONLY, 0)
	} else {
		fileIo, err = NewBufferedFileIoProcessor(localPath, os.O_RDONLY, 0)
	}
	if err != nil {
		log.Error("Failed to open local source file:", localPath)
		return err
	}
	defer fileIo.Close()

	return SyncContent(fileIo.Name(), fileIo, fileSize, remote, timeout, directIO)
}

func SyncContent(sourceName string, rw ReaderWriterAt, size int64, remote string, timeout int, directIO bool) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to SyncContent for source: %v", sourceName)
	}()

	if directIO && size%Blocks != 0 {
		return fmt.Errorf("invalid directIO file block size: %v", size)
	}

	client := newSyncClient(remote, timeout, sourceName, size, rw, directIO)
	defer client.closeServer() // kill the server no matter success or not, best effort

	syncStartTime := time.Now()

	err = client.syncContent()
	if err != nil {
		return err
	}

	log.Debugf("finished sync for the source: %v , size: %v elapsed: %.2fs",
		sourceName, size, time.Now().Sub(syncStartTime).Seconds())
	return nil
}

func (client *syncClient) syncContent() error {
	if err := client.openServer(); err != nil {
		return fmt.Errorf("openServer failed, err: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fileIntervalChannel, errChannel, err := client.rw.GetDataLayout(ctx)
	if err != nil {
		return fmt.Errorf("failed to getFileInterval for rw %v err: %v", client.sourceName, err)
	}

	const WorkerCount = 4
	errorChannels := []<-chan error{errChannel}
	for i := 0; i < WorkerCount; i++ {
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

func (client *syncClient) processSegment(segment FileInterval) error {
	if segment.Kind == SparseHole {
		if err := client.syncHoleInterval(segment.Interval); err != nil {
			return fmt.Errorf("syncHoleInterval %s failed, err: %s", segment.Interval, err)
		}
	} else if segment.Kind == SparseData {
		if err := client.syncDataInterval(segment.Interval); err != nil {
			return fmt.Errorf("syncDataInterval %s failed, err: %s", segment.Interval, err)
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

func (client *syncClient) openServer() error {
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
		return fmt.Errorf("open failed, err: %s", err)
	}

	// drain the buffer and close the body
	_, _ = io.Copy(ioutil.Discard, resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("resp.StatusCode(%d) != http.StatusOK", resp.StatusCode)
	}

	return nil
}

func (client *syncClient) closeServer() {
	queries := make(map[string]string)
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
		return fmt.Errorf("sendHole failed, err: %s", err)
	}

	// drain the buffer and close the body
	_, _ = io.Copy(ioutil.Discard, resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("resp.StatusCode(%d) != http.StatusOK", resp.StatusCode)
	}

	return nil
}

func (client *syncClient) getServerChecksum(checksumInterval Interval) ([]byte, error) {
	queries := make(map[string]string)
	queries["begin"] = strconv.FormatInt(checksumInterval.Begin, 10)
	queries["end"] = strconv.FormatInt(checksumInterval.End, 10)
	resp, err := client.sendHTTPRequest("GET", "getChecksum", queries, nil)
	if err != nil {
		return nil, fmt.Errorf("getChecksum failed, err: %s", err)
	}

	// drain the buffer and close the body
	checksum, err := ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("resp.StatusCode(%d) != http.StatusOK", resp.StatusCode)
	}

	return checksum, err
}

func (client *syncClient) writeData(dataInterval Interval, data []byte) error {
	queries := make(map[string]string)
	queries["begin"] = strconv.FormatInt(dataInterval.Begin, 10)
	queries["end"] = strconv.FormatInt(dataInterval.End, 10)
	resp, err := client.sendHTTPRequest("POST", "writeData", queries, data)
	if err != nil {
		return fmt.Errorf("writeData failed, err: %s", err)
	}

	// drain the buffer and close the body
	_, _ = io.Copy(ioutil.Discard, resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("resp.StatusCode(%d) != http.StatusOK", resp.StatusCode)
	}

	return nil
}

func (client *syncClient) syncDataInterval(dataInterval Interval) error {
	batch := numBlocksInBatch * Blocks

	// Process data in chunks
	for offset := dataInterval.Begin; offset < dataInterval.End; {
		size := batch
		if offset+size > dataInterval.End {
			size = dataInterval.End - offset
		}
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
				log.Errorf("getServerChecksum batchInterval:%s failed, err: %s", batchInterval, serverCksumErr)
				return
			}
			if serverCksumErr = json.Unmarshal(body, &serverCheckSum); serverCksumErr != nil {
				log.Errorf("json.Unmarshal serverCheckSum failed, err: %s", serverCksumErr)
				return
			}
		}()
		go func() {
			defer wg.Done()
			// read data for checksum and sending
			if dataBuffer, cliCksumErr = ReadDataInterval(client.rw, batchInterval); cliCksumErr != nil {
				log.Errorf("ReadDataInterval for batchInterval: %s failed, err: %s", batchInterval, cliCksumErr)
				return
			}
			// calculate local checksum for the data batch interval
			if localCheckSum, cliCksumErr = HashData(dataBuffer); cliCksumErr != nil {
				log.Errorf("HashData locally: %s failed, err: %s", batchInterval, cliCksumErr)
				return
			}
		}()
		wg.Wait()
		if serverCksumErr != nil || cliCksumErr != nil {
			return fmt.Errorf("failed to get checksums for client and server, server checksum error: %v, client checksum error: %v", serverCksumErr, cliCksumErr)
		}

		serverNeedData := true
		if len(serverCheckSum) != 0 {
			// compare server checksum with localCheckSum
			serverNeedData = !bytes.Equal(serverCheckSum, localCheckSum)
		}
		if serverNeedData {
			// send data buffer
			log.Tracef("sending dataBuffer size: %d", len(dataBuffer))
			if err := client.writeData(batchInterval, dataBuffer); err != nil {
				log.Errorf("writeData for batchInterval: %s failed, err: %s", batchInterval, err)
				return err
			}
		}
		offset += batchInterval.Len()
	}
	return nil
}
