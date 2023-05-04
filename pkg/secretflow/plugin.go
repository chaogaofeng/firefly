package secretflow

import (
	"context"
	"github.com/hyperledger/firefly-common/pkg/fftypes"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly/pkg/core"
)

// Plugin is the interface implemented by each secretflow plugin
type Plugin interface {
	core.Named

	// InitConfig initializes the set of configuration options that are valid, with defaults. Called on all plugins.
	InitConfig(config config.KeySet)

	// Init initializes the plugin, with configuration
	Init(ctx context.Context, cancelCtx context.CancelFunc, name string, config config.Section) error

	// SetHandler registers a handler to receive callbacks
	// Plugin will attempt (but is not guaranteed) to deliver events only for the given namespace
	SetHandler(namespace string, handler Callbacks)

	// SetOperationHandler registers a handler to receive async operation status
	// If namespace is set, plugin will attempt to deliver only events for that namespace
	SetOperationHandler(namespace string, handler core.OperationCallbacks)

	// secretflow interface must not deliver any events until start is called
	Start() error

	// Capabilities returns capabilities - not called until after Init
	Capabilities() *Capabilities

	RunPipeline(ctx context.Context, body *fftypes.JSONObject) error
	RunPostProxy(ctx context.Context, path string, body *fftypes.JSONObject) (*fftypes.JSONObject, error)
}

// Callbacks is the interface provided to the secretflow plugin, to allow it to pass events back to firefly.
//
// Events must be delivered sequentially, such that event 2 is not delivered until the callback invoked for event 1
// has completed. However, it does not matter if these events are workload balance between the firefly core
// cluster instances of the node.
type Callbacks interface {
	TrainingJobUpdated(ctx context.Context, plugin Plugin, flowId string, compId string, status int, log string) error
}

// Capabilities is the supported featureset of the secretflow interface implemented by the plugin, with the specified config
type Capabilities struct {
}
