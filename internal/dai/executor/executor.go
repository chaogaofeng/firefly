package executor

import (
	"context"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/hyperledger/firefly/pkg/core"
	"github.com/hyperledger/firefly/pkg/dai"
)

type Executor struct {
	ctx          context.Context
	cancelCtx    context.CancelFunc
	capabilities *dai.Capabilities
}

func (p *Executor) Name() string {
	return "executor"
}

func (p *Executor) Init(ctx context.Context, cancelCtx context.CancelFunc, config config.Section) (err error) {
	p.ctx = log.WithLogField(ctx, "proto", "exector")
	p.cancelCtx = cancelCtx
	p.capabilities = &dai.Capabilities{}

	return nil
}

// SetHandler registers a handler to receive callbacks
// Plugin will attempt (but is not guaranteed) to deliver events only for the given namespace
func (p *Executor) SetHandler(namespace string, handler dai.Callbacks) {

}

// SetOperationHandler registers a handler to receive async operation status
// If namespace is set, plugin will attempt to deliver only events for that namespace
func (p *Executor) SetOperationHandler(namespace string, handler core.OperationCallbacks) {

}

func (p *Executor) Start() error {
	return nil
}

func (p *Executor) Capabilities() *dai.Capabilities {
	return p.capabilities
}

// RegisterExecutorNode registers executor node
func (p *Executor) RegisterExecutorNode() (nodeID string, err error) {
	return "", nil
}

// PublishTask publishes a task, returns taskID
func (p *Executor) PublishTask() (taskID string, err error) {
	return "", nil
}

// ConfirmTask called when Executor confirms task, only task with 'Confirming' status can be started
func (p *Executor) ConfirmTask(taskID string) (err error) {
	return nil
}

// ConfirmTask called when Executor rejects task, only task with 'Confirming' status can be started
func (p *Executor) RejectTask(taskID string) (err error) {
	return nil
}

// StartTask starts task by taskID, only task with 'Ready' status can be started
func (p *Executor) StartTask(taskID string) (err error) {
	return nil
}

// StopTask stops task by taskID, only task with 'Processing' status can be started
func (p *Executor) StopTask(taskID string) (err error) {
	return nil
}
