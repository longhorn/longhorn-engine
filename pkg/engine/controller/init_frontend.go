package controller

import (
	"github.com/longhorn/longhorn-engine/pkg/engine/frontend/rest"
	"github.com/longhorn/longhorn-engine/pkg/engine/frontend/socket"
	"github.com/longhorn/longhorn-engine/pkg/engine/frontend/tcmu"
	"github.com/longhorn/longhorn-engine/pkg/engine/frontend/tgt"
	"github.com/longhorn/longhorn-engine/pkg/engine/types"
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
