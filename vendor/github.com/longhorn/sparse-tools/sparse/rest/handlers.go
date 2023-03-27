package rest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/longhorn/sparse-tools/sparse"
	"github.com/longhorn/sparse-tools/types"
	"github.com/longhorn/sparse-tools/util"
)

type SyncFileOperations interface {
	UpdateSyncFileProgress(size int64)
}

type SyncFileStub struct{}

func (f *SyncFileStub) UpdateSyncFileProgress(size int64) {}

func getQueryDirectIO(request *http.Request) (bool, error) {
	queryParams := request.URL.Query()
	directIOStr := queryParams.Get("directIO")
	if directIOStr == "" {
		return false, fmt.Errorf("directIO does not exist in the queries %+v", queryParams)
	}

	directIO, err := strconv.ParseBool(directIOStr)
	if err != nil {
		return false, errors.Wrapf(err, "failed to parse directIO string %v", directIOStr)
	}
	return directIO, nil
}

func getQueryChecksumMethod(request *http.Request) string {
	queryParams := request.URL.Query()
	return queryParams.Get("checksumMethod")
}

func getQueryChecksum(request *http.Request) string {
	queryParams := request.URL.Query()
	return queryParams.Get("checksum")
}

func getQueryInterval(request *http.Request) (sparse.Interval, error) {
	var interval sparse.Interval

	queryParams := request.URL.Query()

	beginStr := queryParams.Get("begin") // only one value for key begin
	endStr := queryParams.Get("end")     // only one value for key end
	if beginStr == "" || endStr == "" {
		return interval, fmt.Errorf("queryParams begin or end does not exist")
	}

	begin, err := strconv.ParseInt(beginStr, 10, 64)
	if err != nil {
		return interval, errors.Wrapf(err, "failed to parse begin string %v", begin)
	}
	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		return interval, errors.Wrapf(err, "failed to parse end string %v", end)
	}

	return sparse.Interval{Begin: begin, End: end}, nil
}

func (server *SyncServer) open(writer http.ResponseWriter, request *http.Request) {
	err := server.doOpen(writer, request)
	if err != nil {
		log.WithError(err).Error("Failed to open Ssync server")
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Infof("Ssync server is opened and ready for receiving file %v", server.filePath)
}

func (server *SyncServer) doOpen(writer http.ResponseWriter, request *http.Request) error {
	// get directIO
	directIO, err := getQueryDirectIO(request)
	if err != nil {
		return errors.Wrap(err, "failed to query directIO")
	}

	// get file size
	interval, err := getQueryInterval(request)
	if err != nil {
		return errors.Wrap(err, "failed to query interval")
	}

	if directIO && interval.End%sparse.Blocks != 0 {
		return fmt.Errorf("invalid file size %v for directIO", interval.End)
	}
	log.Infof("Receiving %v: size %v, directIO %v", server.filePath, interval.End, directIO)

	fileIo, err := server.newFileIoProcessor(directIO)
	if err != nil {
		return errors.Wrapf(err, "failed to open/create local source file %v", server.filePath)
	}

	err = fileIo.Truncate(interval.End)
	if err != nil {
		return errors.Wrapf(err, "failed to truncate local source file %v", server.filePath)
	}

	// initialize the server file object
	server.fileIo = fileIo

	outgoingJSON, err := json.Marshal(server.fileAlreadyExists)
	if err != nil {
		return errors.Wrap(err, "failed to marshal fileAlreadyExists")
	}

	writer.Header().Set("Content-Type", "application/json")
	fmt.Fprint(writer, string(outgoingJSON))

	return nil
}

func (server *SyncServer) newFileIoProcessor(directIO bool) (sparse.FileIoProcessor, error) {
	if directIO {
		return sparse.NewDirectFileIoProcessor(server.filePath, os.O_RDWR, 0666, true)
	}
	return sparse.NewBufferedFileIoProcessor(server.filePath, os.O_RDWR, 0666, true)
}

func (server *SyncServer) close(writer http.ResponseWriter, request *http.Request) {
	checksumMethod := getQueryChecksumMethod(request)
	checksum := getQueryChecksum(request)

	if f, ok := writer.(http.Flusher); ok {
		f.Flush()
	}

	if server.fileIo != nil {
		server.fileIo.Close()
	}

	if checksumMethod != "" && checksum != "" {
		changeTime, err := util.GetFileChangeTime(server.filePath)
		if err == nil {
			info := &types.SnapshotHashInfo{
				Method:     checksumMethod,
				Checksum:   checksum,
				ChangeTime: changeTime,
			}

			err = util.SetSnapshotHashInfoToChecksumFile(server.filePath+".checksum", info)
		}

		if err != nil {
			log.WithError(err).Warnf("Failed ot set snapshot hash info to checksum file %v", server.filePath)
		}
	}

	log.Infof("Closing Ssync server")
	server.cancelFunc()
}

func (server *SyncServer) sendHole(writer http.ResponseWriter, request *http.Request) {
	err := server.doSendHole(request)
	if err != nil {
		log.WithError(err).Error("Failed to send hole")
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (server *SyncServer) doSendHole(request *http.Request) error {
	remoteHoleInterval, err := getQueryInterval(request)
	if err != nil {
		return errors.Wrap(err, "failed to query interval")
	}

	fiemap := sparse.NewFiemapFile(server.fileIo.GetFile())
	err = fiemap.PunchHole(remoteHoleInterval.Begin, remoteHoleInterval.Len())
	if err != nil {
		return errors.Wrapf(err, "failed to punch hole interval %+v", remoteHoleInterval)
	}

	return nil
}

func (server *SyncServer) getChecksum(writer http.ResponseWriter, request *http.Request) {
	err := server.doGetChecksum(writer, request)
	if err != nil {
		log.WithError(err).Error("Failed to get checksum")
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (server *SyncServer) doGetChecksum(writer http.ResponseWriter, request *http.Request) error {
	remoteDataInterval, err := getQueryInterval(request)
	if err != nil {
		return errors.Wrap(err, "failed to query interval")
	}

	var checksum []byte

	// For the region to have valid data, it can only has one extent covering the whole region
	exts, err := sparse.GetFiemapRegionExts(server.fileIo, remoteDataInterval, 2)
	if len(exts) == 1 && int64(exts[0].Logical) <= remoteDataInterval.Begin &&
		int64(exts[0].Logical+exts[0].Length) >= remoteDataInterval.End {

		checksum, err = sparse.HashFileInterval(server.fileIo, remoteDataInterval)
		if err != nil {
			return errors.Wrapf(err, "failed to hash interval %+v", remoteDataInterval)
		}
	}

	outgoingJSON, err := json.Marshal(checksum)
	if err != nil {
		return errors.Wrap(err, "failed to marshal checksum")
	}

	writer.Header().Set("Content-Type", "application/json")
	fmt.Fprint(writer, string(outgoingJSON))

	if server.fileAlreadyExists {
		server.syncFileOps.UpdateSyncFileProgress(remoteDataInterval.Len())
	}

	return nil
}

func (server *SyncServer) writeData(writer http.ResponseWriter, request *http.Request) {
	err := server.doWriteData(request)
	if err != nil {
		log.WithError(err).Error("Failed to write data")
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (server *SyncServer) doWriteData(request *http.Request) error {
	remoteDataInterval, err := getQueryInterval(request)
	if err != nil {
		return errors.Wrap(err, "failed to query interval")
	}

	log.Tracef("writeData: interval %+v", remoteDataInterval)

	data, err := io.ReadAll(io.LimitReader(request.Body, remoteDataInterval.End-remoteDataInterval.Begin))
	if err != nil {
		return errors.Wrap(err, "failed to read request")
	}

	// Write file with received data into the range
	err = sparse.WriteDataInterval(server.fileIo, remoteDataInterval, data)
	if err != nil {
		return errors.Wrapf(err, "failed to write data interval %+v", remoteDataInterval)
	}

	if !server.fileAlreadyExists {
		server.syncFileOps.UpdateSyncFileProgress(remoteDataInterval.Len())
	}

	return nil
}

func (server *SyncServer) getRecordedMetadata(writer http.ResponseWriter, request *http.Request) {
	err := server.doGetRecordedMetadata(writer, request)
	if err != nil {
		log.WithError(err).Errorf("Failed to get recorded metadata for file %v", server.filePath)

		if strings.Contains(err.Error(), syscall.ENOENT.Error()) {
			http.Error(writer, err.Error(), http.StatusNotFound)
		} else {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
	}
}

func (server *SyncServer) doGetRecordedMetadata(writer http.ResponseWriter, request *http.Request) error {
	// Recorded change time
	metadata, err := server.getRecordedMetadataFromChecksumFile()
	if err != nil {
		return err
	}
	var info types.SnapshotHashInfo
	if err := json.Unmarshal(metadata, &info); err != nil {
		return errors.Wrap(err, "failed to unmarshal hash info")
	}

	// Current change time
	currentChangeTime, err := util.GetFileChangeTime(server.filePath)
	if err != nil {
		return err
	}
	if currentChangeTime != info.ChangeTime {
		return fmt.Errorf("disk file %v is changed (current change time %v, expected change time %v)",
			server.filePath, currentChangeTime, info.ChangeTime)
	}

	writer.Header().Set("Content-Type", "application/json")
	fmt.Fprint(writer, string(metadata))

	return nil
}

func (server *SyncServer) getRecordedMetadataFromChecksumFile() ([]byte, error) {
	f, err := os.Open(server.filePath + types.DiskChecksumSuffix)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open checksum file")
	}
	defer f.Close()

	metadata, err := io.ReadAll(f)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read checksum file")
	}

	return metadata, nil
}
