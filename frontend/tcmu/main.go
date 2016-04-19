package main

/*
#cgo LDFLAGS: -L ./libs -ltcmu
#cgo CFLAGS: -I ./includes

#include <errno.h>
#include <stdlib.h>
#include <scsi/scsi.h>
#include "libtcmu.h"

extern struct tcmulib_context *tcmu_init();
extern bool tcmu_poll_master_fd(struct tcmulib_context *cxt);
extern int tcmu_wait_for_next_command(struct tcmu_device *dev);
extern void *allocate_buffer(int length);

*/
import "C"
import "unsafe"

import (
	"net"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Sirupsen/logrus"

	"github.com/yasker/longhorn/block"
	"github.com/yasker/longhorn/rpc"

	"flag"
	"os"
	"runtime/pprof"
)

var (
	ready bool = false

	log = logrus.WithFields(logrus.Fields{"pkg": "main"})

	address = "localhost:5000"

	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	workers    = 128

	sigs chan os.Signal
	done chan bool
)

type TcmuState struct {
	volume    string
	client    *rpc.Client
	conn      *net.TCPConn
	lbas      int64
	blockSize int
	mutex     *sync.Mutex
	dev       TcmuDevice
}

//export shOpen
func shOpen(dev TcmuDevice) int {
	var (
		err error
	)

	state := &TcmuState{
		mutex: &sync.Mutex{},
	}
	blockSizeStr := C.CString("hw_block_size")
	defer C.free(unsafe.Pointer(blockSizeStr))
	blockSize := int(C.tcmu_get_attribute(dev, blockSizeStr))
	if blockSize == -1 {
		log.Errorln("Cannot find valid hw_block_size")
		return -C.EINVAL
	}
	state.blockSize = blockSize

	size := int64(C.tcmu_get_device_size(dev))
	if size == -1 {
		log.Errorln("Cannot find valid disk size")
		return -C.EINVAL
	}
	state.lbas = size / int64(state.blockSize)

	cfgString := C.GoString(C.tcmu_get_dev_cfgstring(dev))
	if cfgString == "" {
		log.Errorln("Cannot find configuration string")
		return -C.EINVAL
	}
	//id := strings.TrimPrefix(cfgString, "file/")
	//TODO check volume name here

	addr, err := net.ResolveTCPAddr("tcp4", address)
	if err != nil {
		log.Fatalf("failed to resolve ", address, err)
	}
	state.conn, err = net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Fatalf("Cannot connect to replica, %v", err)
	}

	state.client = rpc.NewClient(state.conn, 5, workers)
	state.dev = dev

	go state.HandleRequest()

	log.Debugln("Device added")

	ready = true
	return 0
}

func (s *TcmuState) HandleRequest() {
	defer s.conn.Close()

	cmds := make(chan TcmuCommand, workers)
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			s.processCommands(cmds)
		}()
	}

	for true {
		C.tcmulib_processing_start(s.dev)
		cmd := C.tcmulib_get_next_command(s.dev)
		for cmd != nil {
			cmds <- cmd
			cmd = C.tcmulib_get_next_command(s.dev)
		}
		ret := C.tcmu_wait_for_next_command(s.dev)
		if ret != 0 {
			log.Errorln("Fail to wait for next command", ret)
			break
		}
	}
}

func (s *TcmuState) handleReadCommand(dev TcmuDevice, cmd TcmuCommand) int {
	offset := CmdGetLba(cmd) * int64(s.blockSize)
	length := CmdGetXferLength(cmd) * s.blockSize

	resp, err := s.client.Call(&rpc.Request{
		Header: &block.Request{
			Type:   rpc.MSG_TYPE_READ_REQUEST,
			Offset: offset,
			Length: int64(length),
		}})
	if err != nil {
		log.Errorln("read failed: ", err.Error())
		return CmdSetMediumError(cmd)
	}

	copied := CmdMemcpyIntoIovec(cmd, resp.Data, length)
	if copied != length {
		log.Errorln("read failed: unable to complete buffer copy ")
		return CmdSetMediumError(cmd)
	}
	return C.SAM_STAT_GOOD
}

func (s *TcmuState) handleWriteCommand(dev TcmuDevice, cmd TcmuCommand) int {
	offset := CmdGetLba(cmd) * int64(s.blockSize)
	length := CmdGetXferLength(cmd) * s.blockSize

	buf := make([]byte, length)
	if buf == nil {
		log.Errorln("read failed: fail to allocate buffer")
		return CmdSetMediumError(cmd)
	}
	copied := CmdMemcpyFromIovec(cmd, buf, length)
	if copied != length {
		log.Errorln("write failed: unable to complete buffer copy ")
		return CmdSetMediumError(cmd)
	}

	if _, err := s.client.Call(&rpc.Request{
		Header: &block.Request{
			Type:   rpc.MSG_TYPE_WRITE_REQUEST,
			Offset: offset,
			Length: int64(length),
		},
		Data: buf}); err != nil {
		log.Errorln("write failed: ", err.Error())
		return CmdSetMediumError(cmd)
	}

	return C.SAM_STAT_GOOD
}

func (s *TcmuState) processCommands(cmds chan TcmuCommand) {
	for cmd := range cmds {
		s.processCommand(cmd)
	}
}

func (s *TcmuState) processCommand(cmd TcmuCommand) {
	ret := s.handleCommand(cmd)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	C.tcmulib_command_complete(s.dev, cmd, C.int(ret))
	C.tcmulib_processing_complete(s.dev)
}

func (s *TcmuState) handleCommand(cmd TcmuCommand) int {
	scsiCmd := CmdGetScsiCmd(cmd)
	switch scsiCmd {
	case C.INQUIRY:
		return CmdEmulateInquiry(cmd, s.dev)
	case C.TEST_UNIT_READY:
		return CmdEmulateTestUnitReady(cmd)
	case C.SERVICE_ACTION_IN_16:
		return CmdEmulateServiceActionIn(cmd, s.lbas, s.blockSize)
	case C.MODE_SENSE, C.MODE_SENSE_10:
		return CmdEmulateModeSense(cmd)
	case C.MODE_SELECT, C.MODE_SELECT_10:
		return CmdEmulateModeSelect(cmd)
	case C.READ_6, C.READ_10, C.READ_12, C.READ_16:
		return s.handleReadCommand(s.dev, cmd)
	case C.WRITE_6, C.WRITE_10, C.WRITE_12, C.WRITE_16:
		return s.handleWriteCommand(s.dev, cmd)
	default:
		log.Errorf("unknown command 0x%x\n", scsiCmd)
	}
	return C.TCMU_NOT_HANDLED
}

//export shClose
func shClose(dev TcmuDevice) {
	log.Debugln("Device removed")
}

func handleSignal() {
	sig := <-sigs
	log.Infoln("Shutting down process, due to received signal ", sig)
	pprof.StopCPUProfile()
	done <- true
}

func main() {
	logrus.SetLevel(logrus.DebugLevel)

	flag.Parse()
	if *cpuprofile != "" {
		log.Debug("Output cpuprofile to %v", *cpuprofile)
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}

	sigs = make(chan os.Signal, 1)
	done = make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go handleSignal()

	cxt := C.tcmu_init()
	if cxt == nil {
		panic("cxt is nil")
	}

	for !ready {
		result := C.tcmu_poll_master_fd(cxt)
		log.Debugln("Poll master fd one more time, last result ", result)
	}

	log.Infoln("Waiting for process")
	<-done
	log.Infoln("Shutdown complete")
}
