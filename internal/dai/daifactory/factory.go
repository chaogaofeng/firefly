package daifactory

import (
	"context"

	"github.com/hyperledger/firefly/pkg/dai"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly/internal/coreconfig"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/internal/dai/executor"
)

var pluginsByType = map[string]func() dai.Plugin{
	(*executor.Executor)(nil).Name(): func() dai.Plugin { return &executor.Executor{} },
}

func InitConfig(config config.ArraySection) {
	config.AddKnownKey(coreconfig.PluginConfigName)
	config.AddKnownKey(coreconfig.PluginConfigType)
	for name, plugin := range pluginsByType {
		plugin().InitConfig(config.SubSection(name))
	}
}

func GetPlugin(ctx context.Context, pluginType string) (dai.Plugin, error) {
	plugin, ok := pluginsByType[pluginType]
	if !ok {
		return nil, i18n.NewError(ctx, coremsgs.MsgUnknownDaiPlugin, pluginType)
	}
	return plugin(), nil
}
