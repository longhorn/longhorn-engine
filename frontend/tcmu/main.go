package tcmu

/*
#cgo LDFLAGS: -ltcmu -lnl-3 -lnl-genl-3 -lm

#include <errno.h>
#include <stdlib.h>
#include <scsi/scsi.h>
#include <libtcmu.h>
#include <scsi_defs.h>

extern struct tcmulib_context *tcmu_init();
extern bool tcmu_poll_master_fd(struct tcmulib_context *cxt);
extern int tcmu_wait_for_next_command(struct tcmu_device *dev);
extern void *allocate_buffer(int length);

*/
import "C"
import (
	"errors"
	"strings"
	"sync"
	"unsafe"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/longhorn/types"
)

var (
	log = logrus.WithFields(logrus.Fields{"pkg": "main"})

	// this is super dirty
	backend types.ReaderWriterAt
	cxt     *C.struct_tcmulib_context
	volume  string
)

type State struct {
	sync.Mutex

	volume    string
	lbas      int64
	blockSize int
	backend   types.ReaderWriterAt
}

//export devOpen
func devOpen(dev Device) int {
	state := &State{
		backend: backend,
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

	id := strings.TrimPrefix(cfgString, "longhorn//")
	if id != volume {
		log.Debugf("Ignore volume %s, which is not mine", id)
		return -C.EINVAL
	}
	state.volume = id

	go state.HandleRequest(dev)

	log.Infof("Device %s added", state.volume)
	return 0
}

func (s *State) HandleRequest(dev Device) {
	for true {
		C.tcmulib_processing_start(dev)
		cmd := C.tcmulib_get_next_command(dev)
		for cmd != nil {
			go s.processCommand(dev, cmd)
			cmd = C.tcmulib_get_next_command(dev)
		}
		ret := C.tcmu_wait_for_next_command(dev)
		if ret != 0 {
			log.Errorln("Fail to wait for next command", ret)
			break
		}
	}
}

func (s *State) handleReadCommand(dev Device, cmd Command) int {
	offset := CmdGetLba(cmd) * int64(s.blockSize)
	length := CmdGetXferLength(cmd) * s.blockSize

	buf := make([]byte, length)
	_, err := s.backend.ReadAt(buf, offset)
	if err != nil {
		log.Errorln("read failed: ", err.Error())
		return CmdSetMediumError(cmd)
	}

	copied := CmdMemcpyIntoIovec(cmd, buf, length)
	if copied != length {
		log.Errorln("read failed: unable to complete buffer copy ")
		return CmdSetMediumError(cmd)
	}
	return C.SAM_STAT_GOOD
}

func (s *State) handleWriteCommand(dev Device, cmd Command) int {
	offset := CmdGetLba(cmd) * int64(s.blockSize)
	length := CmdGetXferLength(cmd) * s.blockSize

	buf := make([]byte, length, length)
	if buf == nil {
		log.Errorln("read failed: fail to allocate buffer")
		return CmdSetMediumError(cmd)
	}
	copied := CmdMemcpyFromIovec(cmd, buf, length)
	if copied != length {
		log.Errorln("write failed: unable to complete buffer copy ")
		return CmdSetMediumError(cmd)
	}

	if _, err := s.backend.WriteAt(buf, offset); err != nil {
		log.Errorln("write failed: ", err.Error())
		return CmdSetMediumError(cmd)
	}

	return C.SAM_STAT_GOOD
}

func (s *State) processCommand(dev Device, cmd Command) {
	ret := s.handleCommand(dev, cmd)

	s.Lock()
	defer s.Unlock()

	C.tcmulib_command_complete(dev, cmd, C.int(ret))
	C.tcmulib_processing_complete(dev)
}

func (s *State) handleCommand(dev Device, cmd Command) int {
	scsiCmd := CmdGetScsiCmd(cmd)
	switch scsiCmd {
	case C.INQUIRY:
		return CmdEmulateInquiry(cmd, dev)
	case C.TEST_UNIT_READY:
		return CmdEmulateTestUnitReady(cmd)
	case C.SERVICE_ACTION_IN_16:
		return CmdEmulateServiceActionIn(cmd, s.lbas, s.blockSize)
	case C.MODE_SENSE, C.MODE_SENSE_10:
		return CmdEmulateModeSense(cmd)
	case C.MODE_SELECT, C.MODE_SELECT_10:
		return CmdEmulateModeSelect(cmd)
	case C.READ_6, C.READ_10, C.READ_12, C.READ_16:
		return s.handleReadCommand(dev, cmd)
	case C.WRITE_6, C.WRITE_10, C.WRITE_12, C.WRITE_16:
		return s.handleWriteCommand(dev, cmd)
	default:
		log.Debugf("Ignore unknown SCSI command 0x%x\n", scsiCmd)
	}
	return C.TCMU_NOT_HANDLED
}

//export devClose
func devClose(dev Device) {
	cfgString := C.GoString(C.tcmu_get_dev_cfgstring(dev))
	if cfgString == "" {
		log.Errorln("Cannot find configuration string")
		return
	}

	id := strings.TrimPrefix(cfgString, "longhorn//")
	if id != volume {
		//Ignore close other devs
		return
	}
	log.Infof("Device %s removed", volume)
}

//export devCheckConfig
func devCheckConfig(cfg *C.char, reason **C.char) bool {
	cfgString := C.GoString(cfg)
	if cfgString == "" {
		// Don't want deal with free or cause memory leak, so ignore
		// reason
		log.Errorln("Cannot find valid configuration string")
		return false
	}

	id := strings.TrimPrefix(cfgString, "longhorn//")
	if id != volume {
		//it's for others
		*reason = C.CString("Not current volume")
		log.Debugf("%s is not my volume", id)
		return false
	}
	return true
}

func start(name string, rw types.ReaderWriterAt) error {
	if cxt == nil {
		// this is super dirty
		backend = rw
		volume = name
		cxt = C.tcmu_init()
		if cxt == nil {
			return errors.New("TCMU ctx is nil")
		}

		go func() {
			for {
				result := C.tcmu_poll_master_fd(cxt)
				log.Debugln("Poll master fd one more time, last result ", result)
			}
		}()
	}

	return nil
}
