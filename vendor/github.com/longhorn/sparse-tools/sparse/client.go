package sparse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	httpClientTimeout = 5
	numBlocksInBatch  = 32
)

type syncClient struct {
	remote   string
	timeout  int
	filePath string
	fileSize int64
	fileIo   FileIoProcessor
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

	client := &syncClient{remote, timeout, localPath, fileSize, fileIo}

	defer client.closeServer() // kill the server no matter success or not, best effort

	err = client.syncFileContent(fileIo, fileSize, directIO)
	if err != nil {
		log.Errorf("syncFileContent failed: %s", err)
		return err
	}

	return err
}

func (client *syncClient) syncFileContent(file FileIoProcessor, fileSize int64, directIO bool) error {
	exts, err := GetFiemapExtents(file)
	if err != nil {
		return err
	}

	err = client.openServer(directIO)
	if err != nil {
		return fmt.Errorf("openServer failed, err: %s", err)
	}

	var lastIntervalEnd int64
	var holeInterval Interval

	for _, e := range exts {
		interval := Interval{int64(e.Logical), int64(e.Logical + e.Length)}

		// report hole
		if lastIntervalEnd < interval.Begin {
			holeInterval = Interval{lastIntervalEnd, interval.Begin}
			err := client.syncHoleInterval(holeInterval)
			if err != nil {
				return fmt.Errorf("syncHoleInterval %s failed, err: %s", holeInterval, err)
			}
		}
		lastIntervalEnd = interval.End

		// report data
		err := client.syncDataInterval(file, interval)
		if err != nil {
			return fmt.Errorf("syncDataInterval %s failed, err: %s", interval, err)
		}

		if e.Flags&FIEMAP_EXTENT_LAST != 0 {

			// report last hole
			if lastIntervalEnd < fileSize {
				holeInterval := Interval{lastIntervalEnd, fileSize}

				// syncing hole interval
				err = client.syncHoleInterval(holeInterval)
				if err != nil {
					return fmt.Errorf("syncHoleInterval %s failed, err: %s", holeInterval, err)
				}
			}
		}
	}

	// special case, the whole file is a hole
	if len(exts) == 0 && fileSize != 0 {
		holeInterval = Interval{0, fileSize}
		log.Infof("The file is a hole: %s", holeInterval)

		// syncing hole interval
		err := client.syncHoleInterval(holeInterval)
		if err != nil {
			return fmt.Errorf("syncHoleInterval %s failed, err: %s", holeInterval, err)
		}
	}

	return nil
}

func (client *syncClient) sendHTTPRequest(method string, action string, queries map[string]string, data []byte) (*http.Response, error) {
	httpClient := &http.Client{Timeout: time.Duration(httpClientTimeout * time.Second)}

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

	log.Debugf("method: %s, url with query string: %s, data len: %d", method, req.URL.String(), len(data))

	return httpClient.Do(req)
}

func (client *syncClient) openServer(directIO bool) error {
	var err error
	var resp *http.Response

	timeStart := time.Now()
	timeStop := timeStart.Add(time.Duration(client.timeout) * time.Second)
	queries := make(map[string]string)
	queries["begin"] = strconv.FormatInt(0, 10)
	queries["end"] = strconv.FormatInt(client.fileSize, 10)
	queries["directIO"] = strconv.FormatBool(directIO)
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
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("resp.StatusCode(%d) != http.StatusOK", resp.StatusCode)
	}
	resp.Body.Close()

	return nil
}

func (client *syncClient) closeServer() {
	queries := make(map[string]string)
	client.sendHTTPRequest("POST", "close", queries, nil)
}

func (client *syncClient) syncHoleInterval(holeInterval Interval) error {
	queries := make(map[string]string)
	queries["begin"] = strconv.FormatInt(holeInterval.Begin, 10)
	queries["end"] = strconv.FormatInt(holeInterval.End, 10)
	resp, err := client.sendHTTPRequest("POST", "sendHole", queries, nil)
	if err != nil {
		return fmt.Errorf("sendHole failed, err: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("resp.StatusCode(%d) != http.StatusOK", resp.StatusCode)
	}
	resp.Body.Close()

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
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("resp.StatusCode(%d) != http.StatusOK", resp.StatusCode)
	}
	defer resp.Body.Close()

	return ioutil.ReadAll(resp.Body)
}

func (client *syncClient) writeData(dataInterval Interval, data []byte) error {
	queries := make(map[string]string)
	queries["begin"] = strconv.FormatInt(dataInterval.Begin, 10)
	queries["end"] = strconv.FormatInt(dataInterval.End, 10)
	resp, err := client.sendHTTPRequest("POST", "writeData", queries, data)
	if err != nil {
		return fmt.Errorf("writeData failed, err: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("resp.StatusCode(%d) != http.StatusOK", resp.StatusCode)
	}
	resp.Body.Close()

	return nil
}

func (client *syncClient) syncDataInterval(file FileIoProcessor, dataInterval Interval) error {
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
			if dataBuffer, cliCksumErr = ReadDataInterval(file, batchInterval); cliCksumErr != nil {
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
			log.Debugf("sending dataBuffer size: %d", len(dataBuffer))
			if err := client.writeData(batchInterval, dataBuffer); err != nil {
				log.Errorf("writeData for batchInterval: %s failed, err: %s", batchInterval, err)
				return err
			}
		}
		offset += batchInterval.Len()
	}
	return nil
}
