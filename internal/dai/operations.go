package dai

import (
	"context"
	"encoding/json"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
)

func addExecutorInputs(op *core.Operation, executor *core.Executor) (err error) {
	var executorJSON []byte
	if executorJSON, err = json.Marshal(executor); err == nil {
		err = json.Unmarshal(executorJSON, &op.Input)
	}
	return err
}

func retrieveExecutorInputs(ctx context.Context, op *core.Operation) (*core.Executor, error) {
	var executor core.Executor
	s := op.Input.String()
	if err := json.Unmarshal([]byte(s), &executor); err != nil {
		return nil, i18n.WrapError(ctx, err, i18n.MsgJSONObjectParseFailed, s)
	}
	return &executor, nil
}

func opExecutor(op *core.Operation, executor *core.Executor) *core.PreparedOperation {
	return &core.PreparedOperation{
		ID:        op.ID,
		Namespace: op.Namespace,
		Plugin:    op.Plugin,
		Type:      op.Type,
		Data:      executorData{Executor: executor},
	}
}

type executorData struct {
	Executor *core.Executor `json:"executor"`
}

func addTaskInputs(op *core.Operation, task *core.Task) (err error) {
	var taskJSON []byte
	if taskJSON, err = json.Marshal(task); err == nil {
		err = json.Unmarshal(taskJSON, &op.Input)
	}
	return err
}

func retrieveTaskInputs(ctx context.Context, op *core.Operation) (*core.Task, error) {
	var task core.Task
	s := op.Input.String()
	if err := json.Unmarshal([]byte(s), &task); err != nil {
		return nil, i18n.WrapError(ctx, err, i18n.MsgJSONObjectParseFailed, s)
	}
	return &task, nil
}

func opTask(op *core.Operation, task *core.Task) *core.PreparedOperation {
	return &core.PreparedOperation{
		ID:        op.ID,
		Namespace: op.Namespace,
		Plugin:    op.Plugin,
		Type:      op.Type,
		Data:      taskData{Task: task},
	}
}

type taskData struct {
	Task *core.Task `json:"task"`
}

func (dm *daiManager) PrepareOperation(ctx context.Context, op *core.Operation) (*core.PreparedOperation, error) {
	switch op.Type {
	case core.TransactionTypeDaiExecutor:
		executor, err := retrieveExecutorInputs(ctx, op)
		if err != nil {
			return nil, err
		}
		return opExecutor(op, executor), nil

	case core.TransactionTypeDaiTask:
		task, err := retrieveTaskInputs(ctx, op)
		if err != nil {
			return nil, err
		}
		return opTask(op, task), nil

	default:
		return nil, i18n.NewError(ctx, coremsgs.MsgOperationNotSupported, op.Type)
	}
}

func (dm *daiManager) RunOperation(ctx context.Context, op *core.PreparedOperation) (outputs fftypes.JSONObject, complete bool, err error) {
	dataJson, _ := json.Marshal(op.Data)
	switch op.Data.(type) {
	case executorData:
		contract := dm.namespace.Contracts.Active
		return nil, false, dm.blockchain.SubmitNetworkAction(ctx, op.NamespacedIDString(), dm.defaultKey, core.NetworkActionDaiExecutor, contract.Location, string(dataJson))
	case taskData:
		contract := dm.namespace.Contracts.Active
		return nil, false, dm.blockchain.SubmitNetworkAction(ctx, op.NamespacedIDString(), dm.defaultKey, core.NetworkActionDaiTask, contract.Location, string(dataJson))

	default:
		return nil, false, i18n.NewError(ctx, coremsgs.MsgOperationDataIncorrect, op.Data)
	}
}

func (dm *daiManager) OnOperationUpdate(ctx context.Context, op *core.Operation, update *core.OperationUpdate) error {
	//if op.Type == core.TransactionTypeDaiExecutor && update.Status == core.OpStatusSucceeded {
	//	executorData, err := retrieveExecutorInputs(ctx, op)
	//	if err != nil {
	//		return err
	//	}
	//	dm.executorsMutex.Lock()
	//	dm.executors[executorData.Name] = executorData
	//	dm.executorsMutex.Unlock()
	//}
	//
	//if op.Type == core.TransactionTypeDaiTask && update.Status == core.OpStatusSucceeded {
	//	taskData, err := retrieveTaskInputs(ctx, op)
	//	if err != nil {
	//		return err
	//	}
	//	switch taskData.Status {
	//	case TaskProcessing:
	//		if err := dm.mpcStart(ctx, taskData); err != nil {
	//			return err
	//		}
	//	case TaskStop:
	//		if err := dm.mpcStop(ctx, taskData); err != nil {
	//			return err
	//		}
	//	}
	//}
	return nil
}
