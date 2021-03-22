package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/sparse"
)

type SyncFileOperations interface {
	UpdateSyncFileProgress(size int64)
}

type SyncFileStub struct{}

func (f *SyncFileStub) UpdateSyncFileProgress(size int64) {}

func (server *SyncServer) getQueryDirectIO(request *http.Request) (bool, error) {
	queryParams := request.URL.Query()
	directIOStr := queryParams.Get("directIO")
	if directIOStr == "" {
		return false, fmt.Errorf("directIO does not exist in the queries: %+v", queryParams)
	}

	directIO, err := strconv.ParseBool(directIOStr)
	if err != nil {
		return false, fmt.Errorf("strconv.ParseBool(directIOStr) error: %s", err)
	}
	return directIO, nil
}

func (server *SyncServer) getQueryInterval(request *http.Request) (sparse.Interval, error) {
	var interval sparse.Interval
	var err error

	queryParams := request.URL.Query()
	beginStr := queryParams.Get("begin") // only one value for key begin
	endStr := queryParams.Get("end")     // only one value for key end
	if beginStr == "" || endStr == "" {
		err = fmt.Errorf("queryParams begin or end not exist")
		return interval, err
	}

	begin, err := strconv.ParseInt(beginStr, 10, 64)
	if err != nil {
		err = fmt.Errorf("strconv.ParseInt(begin) error: %s", err)
		return interval, err
	}
	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		err = fmt.Errorf("strconv.ParseInt(end) error: %s", err)
		return interval, err
	}

	return sparse.Interval{Begin: begin, End: end}, err
}

func (server *SyncServer) open(writer http.ResponseWriter, request *http.Request) {
	err := server.doOpen(request)
	if err != nil {
		log.Errorf("open failed, err: %s", err)
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Infof("Ssync server opened and ready")
}

func (server *SyncServer) doOpen(request *http.Request) error {
	// get directIO
	directIO, err := server.getQueryDirectIO(request)
	if err != nil {
		return fmt.Errorf("server.getQueryDirectIO failed, err: %s", err)
	}

	// get file size
	interval, err := server.getQueryInterval(request)
	if err != nil {
		return fmt.Errorf("server.getQueryInterval failed, err: %s", err)
	}

	if directIO && interval.End%sparse.Blocks != 0 {
		return fmt.Errorf("Invalid directIO file block size: %v", interval.End)
	}
	log.Infof("open: receiving fileSize: %d, setting up directIO: %v", interval.End, directIO)

	var fileIo sparse.FileIoProcessor
	if directIO {
		fileIo, err = sparse.NewDirectFileIoProcessor(server.filePath, os.O_RDWR, 0666, true)
	} else {
		fileIo, err = sparse.NewBufferedFileIoProcessor(server.filePath, os.O_RDWR, 0666, true)
	}
	if err != nil {
		return fmt.Errorf("open: Failed to open/create local source file, path: %s, err: %s", server.filePath, err)
	}

	err = fileIo.Truncate(interval.End)
	if err != nil {
		return fmt.Errorf("open: Failed to Truncate local source file, path: %s, err: %s", server.filePath, err)
	}

	// initialize the server file object
	server.fileIo = fileIo

	return nil
}

func (server *SyncServer) close(writer http.ResponseWriter, request *http.Request) {
	if f, ok := writer.(http.Flusher); ok {
		f.Flush()
	}

	server.fileIo.Close()
	log.Infof("Closing ssync server")

	server.srv.Close()
}

func (server *SyncServer) sendHole(writer http.ResponseWriter, request *http.Request) {
	err := server.doSendHole(request)
	if err != nil {
		log.Errorf("sendHole failed, err: %s", err)
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (server *SyncServer) doSendHole(request *http.Request) error {
	remoteHoleInterval, err := server.getQueryInterval(request)
	if err != nil {
		return fmt.Errorf("server.getQueryInterval failed, err: %s", err)
	}

	fiemap := sparse.NewFiemapFile(server.fileIo.GetFile())
	err = fiemap.PunchHole(remoteHoleInterval.Begin, remoteHoleInterval.Len())
	if err != nil {
		return fmt.Errorf("PunchHole: %s error: %s", remoteHoleInterval, err)
	}

	return nil
}

func (server *SyncServer) getChecksum(writer http.ResponseWriter, request *http.Request) {
	err := server.doGetChecksum(writer, request)
	if err != nil {
		log.Errorf("getChecksum failed, err: %s", err)
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (server *SyncServer) doGetChecksum(writer http.ResponseWriter, request *http.Request) error {
	remoteDataInterval, err := server.getQueryInterval(request)
	if err != nil {
		return fmt.Errorf("server.getQueryInterval failed, err: %s", err)
	}

	var checksum []byte

	// For the region to have valid data, it can only has one extent covering the whole region
	exts, err := sparse.GetFiemapRegionExts(server.fileIo, remoteDataInterval)
	if len(exts) == 1 && int64(exts[0].Logical) <= remoteDataInterval.Begin &&
		int64(exts[0].Logical+exts[0].Length) >= remoteDataInterval.End {

		checksum, err = sparse.HashFileInterval(server.fileIo, remoteDataInterval)
		if err != nil {
			return fmt.Errorf("HashFileInterval locally: %s failed, err: %s", remoteDataInterval, err)
		}
	}

	outgoingJSON, err := json.Marshal(checksum)
	if err != nil {
		return fmt.Errorf("json.Marshal(checksum) failed, err: %s", err)
	}

	writer.Header().Set("Content-Type", "application/json")
	fmt.Fprint(writer, string(outgoingJSON))
	server.syncFileOps.UpdateSyncFileProgress(remoteDataInterval.Len())

	return nil
}

func (server *SyncServer) writeData(writer http.ResponseWriter, request *http.Request) {
	err := server.doWriteData(request)
	if err != nil {
		log.Errorf("writeData failed, err: %s", err)
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (server *SyncServer) doWriteData(request *http.Request) error {
	remoteDataInterval, err := server.getQueryInterval(request)
	if err != nil {
		return fmt.Errorf("server.getQueryInterval failed, err: %s", err)
	}
	log.Debugf("writeData: interval: %s", remoteDataInterval)

	data, err := ioutil.ReadAll(io.LimitReader(request.Body, remoteDataInterval.End-remoteDataInterval.Begin))
	if err != nil {
		return fmt.Errorf("ioutil.ReadAll(io.LimitReader(r.Body, end-begin)) error: %s", err)
	}

	// Write file with received data into the range
	err = sparse.WriteDataInterval(server.fileIo, remoteDataInterval, data)
	if err != nil {
		return fmt.Errorf("WriteDataInterval to file error: %s", err)
	}

	return nil
}
