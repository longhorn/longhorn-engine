package controller

import (
	"github.com/rancher/longhorn-engine/frontend/rest"
	"github.com/rancher/longhorn-engine/frontend/socket"
	"github.com/rancher/longhorn-engine/frontend/tcmu"
	"github.com/rancher/longhorn-engine/frontend/tgt"
	"github.com/rancher/longhorn-engine/types"
)

var (
	Frontends = map[string]types.Frontend{}
)

func init() {
	Frontends["rest"] = rest.New()
	Frontends["socket"] = socket.New()
	Frontends["tcmu"] = tcmu.New()
	Frontends["tgt"] = tgt.New()
}
