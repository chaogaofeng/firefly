package dai

import (
	"context"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly/pkg/core"
)

// Plugin is the interface implemented by each dai plugin
type Plugin interface {
	core.Named

	// InitConfig initializes the set of configuration options that are valid, with defaults. Called on all plugins.
	InitConfig(config config.Section)

	// Init initializes the plugin, with configuration
	Init(ctx context.Context, cancelCtx context.CancelFunc, config config.Section) error

	// SetHandler registers a handler to receive callbacks
	// Plugin will attempt (but is not guaranteed) to deliver events only for the given namespace
	SetHandler(namespace string, handler Callbacks)

	// SetOperationHandler registers a handler to receive async operation status
	// If namespace is set, plugin will attempt to deliver only events for that namespace
	SetOperationHandler(namespace string, handler core.OperationCallbacks)

	// dai interface must not deliver any events until start is called
	Start() error

	// Capabilities returns capabilities - not called until after Init
	Capabilities() *Capabilities

	// RegisterExecutorNode registers executor node
	RegisterExecutorNode() (nodeID string, err error)

	// PublishTask publishes a task, returns taskID
	PublishTask() (taskID string, err error)

	// ConfirmTask called when Executor confirms task, only task with 'Confirming' status can be started
	ConfirmTask(taskID string) (err error)

	// ConfirmTask called when Executor rejects task, only task with 'Confirming' status can be started
	RejectTask(taskID string) (err error)

	// StartTask starts task by taskID, only task with 'Ready' status can be started
	StartTask(taskID string) (err error)

	// StopTask stops task by taskID, only task with 'Processing' status can be started
	StopTask(taskID string) (err error)

	// TASK
	// TaskConfirming = "Confirming" // waiting for Executors to confirm
	// TaskReady      = "Ready"      // has been confirmed by all Executors, and ready to start
	// TaskStop      = "Stop"      // has been confirmed by all Executors, and ready to start
	// TaskProcessing = "Processing" // under process, that's during training or predicting
	// TaskFinished   = "Finished"   // task finished
	// TaskFailed     = "Failed"     // task failed
	// TaskRejected   = "Rejected"   // task rejected by one of the Executors
}

type Callbacks interface {
}

type Capabilities struct {
}

type DataForTask struct {
	Owner       []byte
	Executor    []byte
	DataID      string
	PsiLabel    string
	ConfirmedAt int64
	RejectedAt  int64
	Address     string
	IsTagPart   bool
}

type FLTask struct {
	TaskID      string
	Name        string
	Description string
	Requester   []byte
	DataSets    []*DataForTask
	//	AlgoParam   *common.TaskParams
	Status      string
	ErrMessage  string
	Result      string
	PublishTime int64
	StartTime   int64
	EndTime     int64
}
