package sffactory

import (
	"context"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly/internal/coreconfig"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/internal/secretflow/goldnet"
	sf "github.com/hyperledger/firefly/pkg/secretflow"
)

var pluginsByName = map[string]func() sf.Plugin{
	(*goldnet.SecretFlow)(nil).Name(): func() sf.Plugin { return &goldnet.SecretFlow{} },
}

func InitConfig(config config.ArraySection) {
	config.AddKnownKey(coreconfig.PluginConfigName)
	config.AddKnownKey(coreconfig.PluginConfigType)
	config.AddKnownKey(coreconfig.PluginBroadcastName)
	for name, plugin := range pluginsByName {
		plugin().InitConfig(config.SubSection(name))
	}
}

func GetPlugin(ctx context.Context, connectorName string) (sf.Plugin, error) {
	plugin, ok := pluginsByName[connectorName]
	if !ok {
		return nil, i18n.NewError(ctx, coremsgs.MsgUnknownSecretFlowsPlugin, connectorName)
	}
	return plugin(), nil
}
