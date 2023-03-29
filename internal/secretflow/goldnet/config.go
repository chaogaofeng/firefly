package goldnet

import (
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/wsclient"
)

func (gsf *SecretFlow) InitConfig(config config.KeySet) {
	wsclient.InitConfig(config)
}
