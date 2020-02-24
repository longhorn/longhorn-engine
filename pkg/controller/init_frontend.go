package controller

import (
	devtypes "github.com/longhorn/go-iscsi-helper/types"

	"github.com/longhorn/longhorn-engine/pkg/frontend/rest"
	"github.com/longhorn/longhorn-engine/pkg/frontend/socket"
	"github.com/longhorn/longhorn-engine/pkg/frontend/tgt"
	"github.com/longhorn/longhorn-engine/pkg/types"
)

var (
	Frontends = map[string]types.Frontend{}
)

func init() {
	Frontends["rest"] = rest.New()
	Frontends["socket"] = socket.New()
	Frontends[devtypes.FrontendTGTBlockDev] = tgt.New(devtypes.FrontendTGTBlockDev)
	Frontends[devtypes.FrontendTGTISCSI] = tgt.New(devtypes.FrontendTGTISCSI)
}
