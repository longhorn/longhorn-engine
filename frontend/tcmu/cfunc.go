package tcmu

/*
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <poll.h>
#include <scsi/scsi.h>
#include <scsi_defs.h>
#include <libtcmu.h>


void errp(const char *fmt, ...)
{
	va_list va;

	va_start(va, fmt);
	vfprintf(stderr, fmt, va);
	va_end(va);
}

extern bool devCheckConfig(const char *cfgString, char **reason);
extern int devOpen(struct tcmu_device *dev);
extern void devClose(struct tcmu_device *dev);

int dev_open_cgo(struct tcmu_device *dev) {
	return devOpen(dev);
}

void dev_close_cgo(struct tcmu_device *dev) {
	devClose(dev);
}

bool dev_check_config_cgo(const char *cfgString, char **reason) {
	return devCheckConfig(cfgString, reason);
}

static struct tcmulib_handler lh_handler = {
	.name = "Longhorn TCMU handler",
	.subtype = "longhorn",
	.cfg_desc = "dev_config=longhorn//<id>",
	.added = dev_open_cgo,
	.removed = dev_close_cgo,
	.check_config = dev_check_config_cgo,
};

struct tcmulib_context *tcmu_init() {
	return tcmulib_initialize(&lh_handler, 1, errp);
}

bool tcmu_poll_master_fd(struct tcmulib_context *cxt) {
	int ret;
	struct pollfd pfd;

	pfd.fd = tcmulib_get_master_fd(cxt);
	pfd.events = POLLIN;
	pfd.revents = 0;

	ret = poll(&pfd, 1, -1);
	if (ret < 0) {
		errp("poll error out with %d", ret);
		return false;
	}

	if (pfd.revents) {
		tcmulib_master_fd_ready(cxt);
		return true;
	}
	return false;
}

int tcmu_wait_for_next_command(struct tcmu_device *dev) {
	struct pollfd pfd;

	pfd.fd = tcmu_get_dev_fd(dev);
	pfd.events = POLLIN;
	pfd.revents = 0;

	poll(&pfd, 1, -1);

	if (pfd.revents != 0 && pfd.revents != POLLIN) {
		errp("poll received unexpected revent: 0x%x\n", pfd.revents);
		return -1;
	}
	return 0;
}

uint8_t tcmucmd_get_cdb_at(struct tcmulib_cmd *cmd, int index) {
	return cmd->cdb[index];
}

void *allocate_buffer(int length) {
	return calloc(1, length);
}

*/
import "C"

import (
	"unsafe"
)

type (
	Command *C.struct_tcmulib_cmd
	Device  *C.struct_tcmu_device

	Cbuffer *C.void
)

func CmdGetScsiCmd(cmd Command) byte {
	return byte(C.tcmucmd_get_cdb_at(cmd, 0))
}

func CmdMemcpyIntoIovec(cmd Command, buf []byte, length int) int {
	if len(buf) != length {
		log.Errorln("read buffer length %v is not %v: ", len(buf), length)
		return 0
	}
	return int(C.tcmu_memcpy_into_iovec(cmd.iovec, cmd.iov_cnt, unsafe.Pointer(&buf[0]), C.size_t(length)))
}

func CmdMemcpyFromIovec(cmd Command, buf []byte, length int) int {
	if len(buf) != length {
		log.Errorln("write buffer length %v is not %v: ", len(buf), length)
		return 0
	}
	return int(C.tcmu_memcpy_from_iovec(unsafe.Pointer(&buf[0]), C.size_t(length), cmd.iovec, cmd.iov_cnt))
}

func CmdSetMediumError(cmd Command) int {
	return int(C.tcmu_set_sense_data(&cmd.sense_buf[0], C.MEDIUM_ERROR, C.ASC_READ_ERROR, nil))
}

func CmdGetLba(cmd Command) int64 {
	return int64(C.tcmu_get_lba(cmd.cdb))
}

func CmdGetXferLength(cmd Command) int {
	return int(C.tcmu_get_xfer_length(cmd.cdb))
}

func CmdEmulateInquiry(cmd Command, dev Device) int {
	return int(C.tcmu_emulate_inquiry(dev, cmd.cdb, cmd.iovec, cmd.iov_cnt, &cmd.sense_buf[0]))
}

func CmdEmulateTestUnitReady(cmd Command) int {
	return int(C.tcmu_emulate_test_unit_ready(cmd.cdb, cmd.iovec, cmd.iov_cnt, &cmd.sense_buf[0]))
}

func CmdEmulateModeSense(cmd Command) int {
	return int(C.tcmu_emulate_mode_sense(cmd.cdb, cmd.iovec, cmd.iov_cnt, &cmd.sense_buf[0]))
}

func CmdEmulateModeSelect(cmd Command) int {
	return int(C.tcmu_emulate_mode_select(cmd.cdb, cmd.iovec, cmd.iov_cnt, &cmd.sense_buf[0]))
}

func CmdEmulateServiceActionIn(cmd Command, numLbas int64, blockSize int) int {
	if C.tcmucmd_get_cdb_at(cmd, 1) == C.READ_CAPACITY_16 {
		return int(C.tcmu_emulate_read_capacity_16(C.uint64_t(numLbas),
			C.uint32_t(blockSize),
			cmd.cdb, cmd.iovec, cmd.iov_cnt, &cmd.sense_buf[0]))
	}
	return C.TCMU_NOT_HANDLED
}
