package dai

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/hyperledger/firefly-common/pkg/wsclient"

	"github.com/go-resty/resty/v2"

	"github.com/hyperledger/firefly-common/pkg/log"

	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly/internal/broadcast"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/internal/dai/executor"
	"github.com/hyperledger/firefly/internal/identity"
	"github.com/hyperledger/firefly/internal/metrics"
	"github.com/hyperledger/firefly/internal/operations"
	"github.com/hyperledger/firefly/internal/privatemessaging"
	"github.com/hyperledger/firefly/internal/syncasync"
	"github.com/hyperledger/firefly/internal/txcommon"
	"github.com/hyperledger/firefly/pkg/blockchain"
	"github.com/hyperledger/firefly/pkg/core"
	"github.com/hyperledger/firefly/pkg/database"
)

const (
	ExecutorNormal int = iota
	ExecutorStop
)

const (
	TaskReady int = iota
	TaskTryStart
	TaskStarted
	TaskSuccess
	TaskFailed
	TaskTryStop
	TaskStopped
)

const (
	MPCTaskUnkown int64 = iota
	MPCTaskInit
	MPCTaskProcess
	MPCTaskSuccess
	MPCTaskFail
	MPCTaskStop
)

type Manager interface {
	core.Named

	RegisterExecutor(ctx context.Context, node *core.ExecutorInput, waitConfirm bool) (*core.Executor, error)

	GetExecutors(ctx context.Context, filter database.AndFilter) ([]*core.Executor, *database.FilterResult, error)
	GetExecutorByNameOrID(ctx context.Context, nameOrID string) (*core.Executor, error)

	PublishTask(ctx context.Context, pool *core.TaskInput, waitConfirm bool) (*core.Task, error)
	StartTask(ctx context.Context, nameOrID string, waitConfirm bool) (*core.Task, error)
	ConfirmTask(ctx context.Context, nameOrID string, waitConfirm bool) (*core.Task, error)
	StopTask(ctx context.Context, nameOrID string, waitConfirm bool) (*core.Task, error)

	GetTasks(ctx context.Context, filter database.AndFilter) ([]*core.Task, *database.FilterResult, error)
	GetTaskByNameOrID(ctx context.Context, nameOrID string) (*core.Task, error)

	ExecutorContract(ctx context.Context, location *fftypes.JSONAny, event *blockchain.Event) (err error)
	TaskContract(ctx context.Context, location *fftypes.JSONAny, event *blockchain.Event) (err error)

	// From operations.OperationHandler
	PrepareOperation(ctx context.Context, op *core.Operation) (*core.PreparedOperation, error)
	RunOperation(ctx context.Context, op *core.PreparedOperation) (outputs fftypes.JSONObject, complete bool, err error)
}

type daiManager struct {
	ctx              context.Context
	namespace        *core.Namespace
	database         database.Plugin
	blockchain       blockchain.Plugin
	txHelper         txcommon.Helper
	identity         identity.Manager
	syncasync        syncasync.Bridge
	broadcast        broadcast.Manager        // optional
	messaging        privatemessaging.Manager // optional
	metrics          metrics.Manager
	operations       operations.Manager
	defaultKey       string
	keyNormalization int

	executorsMutex sync.Mutex
	executors      map[string]wsclient.WSClient
}

func NewDaiManager(ctx context.Context, ns *core.Namespace, defaultKey string, keyNormalization string, di database.Plugin, bi blockchain.Plugin, im identity.Manager, sa syncasync.Bridge, bm broadcast.Manager, pm privatemessaging.Manager, mm metrics.Manager, om operations.Manager, txHelper txcommon.Helper) (Manager, error) {
	if di == nil || im == nil || sa == nil || bi == nil || mm == nil || om == nil {
		return nil, i18n.NewError(ctx, coremsgs.MsgInitializationNilDepError, "DaiManager")
	}
	dm := &daiManager{
		ctx:              log.WithLogField(ctx, "role", "dai-manager"),
		namespace:        ns,
		database:         di,
		blockchain:       bi,
		txHelper:         txHelper,
		identity:         im,
		syncasync:        sa,
		broadcast:        bm,
		messaging:        pm,
		defaultKey:       strings.ToLower(defaultKey),
		keyNormalization: identity.ParseKeyNormalizationConfig(keyNormalization),
		metrics:          mm,
		operations:       om,
		executors:        map[string]wsclient.WSClient{},
	}
	om.RegisterHandler(ctx, dm, []core.OpType{
		core.TransactionTypeDaiExecutor,
		core.TransactionTypeDaiTask,
	})

	if err := dm.init(ctx); err != nil {
		return nil, err
	}

	return dm, nil
}

func (dm *daiManager) init(ctx context.Context) error {
	// 更新任务的状态
	go dm.syncTaskStatus(ctx)
	go func() {
		timer := time.NewTicker(30 * time.Minute)
		for {
			select {
			case <-timer.C:
				// 更新节点状态
				dm.syncExecutorStatus(ctx)
				// 更新任务的状态
				dm.syncTaskStatus(ctx)
			}
		}
	}()
	return nil
}

func (dm *daiManager) Name() string {
	return "DaiManager"
}

func (dm *daiManager) RegisterExecutor(ctx context.Context, executorInput *core.ExecutorInput, waitConfirm bool) (*core.Executor, error) {
	if err := fftypes.ValidateFFNameFieldNoUUID(ctx, executorInput.Name, "name"); err != nil {
		return nil, err
	}
	if existing, err := dm.database.GetExecutor(ctx, dm.namespace.Name, executorInput.Name); err != nil {
		return nil, err
	} else if existing != nil {
		return nil, i18n.NewError(ctx, coremsgs.MsgDaiExecutorDuplicate, executorInput.Name)
	}
	if executorInput.ID.String() == "00000000-0000-0000-0000-000000000000" {
		executorInput.ID = fftypes.NewUUID()
	} else {
		if existing, err := dm.database.GetExecutorByID(ctx, dm.namespace.Name, executorInput.ID); err != nil {
			return nil, err
		} else if existing != nil {
			return nil, i18n.NewError(ctx, coremsgs.MsgDaiExecutorDuplicate, executorInput.ID)
		}
	}
	executorInput.Namespace = dm.namespace.Name
	executorInput.Status = ExecutorNormal

	var op *core.Operation
	err := dm.database.RunAsGroup(ctx, func(ctx context.Context) (err error) {
		txid, err := dm.txHelper.SubmitNewTransaction(ctx, core.TransactionTypeDaiExecutor, "")
		if err != nil {
			return err
		}

		executorInput.TX.ID = txid
		executorInput.TX.Type = core.TransactionTypeDaiExecutor

		op = core.NewOperation(
			(*executor.Executor)(nil),
			dm.namespace.Name,
			txid,
			core.TransactionTypeDaiExecutor)
		if err = addExecutorInputs(op, &executorInput.Executor); err == nil {
			err = dm.operations.AddOrReuseOperation(ctx, op)
		}
		return err
	})
	if err != nil {
		return nil, err
	}

	send := func(ctx context.Context) error {
		_, err := dm.operations.RunOperation(ctx, opExecutor(op, &executorInput.Executor))
		return err
	}

	if waitConfirm {
		_, err := dm.syncasync.WaitForInvokeOperation(ctx, op.ID, send)
		return &executorInput.Executor, err
	}

	return &executorInput.Executor, send(ctx)
}

func (dm *daiManager) updateExecutor(ctx context.Context, nameOrID string, status int, waitConfirm bool) (*core.Executor, error) {
	existing, err := dm.database.GetExecutor(ctx, dm.namespace.Name, nameOrID)
	if err != nil {
		return nil, err
	} else if existing == nil {
		return nil, i18n.NewError(ctx, coremsgs.MsgDaiExecutorNotFound, nameOrID)
	}
	existing.Status = status

	var op *core.Operation
	err = dm.database.RunAsGroup(ctx, func(ctx context.Context) (err error) {
		txid, err := dm.txHelper.SubmitNewTransaction(ctx, core.TransactionTypeDaiExecutor, "")
		if err != nil {
			return err
		}

		existing.TX.ID = txid
		existing.TX.Type = core.TransactionTypeDaiExecutor

		op = core.NewOperation(
			(*executor.Executor)(nil),
			dm.namespace.Name,
			txid,
			core.TransactionTypeDaiExecutor)
		if err = addExecutorInputs(op, existing); err == nil {
			err = dm.operations.AddOrReuseOperation(ctx, op)
		}
		return err
	})
	if err != nil {
		return nil, err
	}

	send := func(ctx context.Context) error {
		_, err := dm.operations.RunOperation(ctx, opExecutor(op, existing))
		return err
	}
	if waitConfirm {
		_, err := dm.syncasync.WaitForInvokeOperation(ctx, op.ID, send)
		return existing, err
	}
	return existing, send(ctx)
}

func (dm *daiManager) GetExecutors(ctx context.Context, filter database.AndFilter) ([]*core.Executor, *database.FilterResult, error) {
	return dm.database.GetExecutors(ctx, dm.namespace.Name, filter)
}
func (dm *daiManager) GetExecutorByNameOrID(ctx context.Context, nameOrID string) (*core.Executor, error) {
	var item *core.Executor

	itemID, err := fftypes.ParseUUID(ctx, nameOrID)
	if err != nil {
		if err := fftypes.ValidateFFNameField(ctx, nameOrID, "name"); err != nil {
			return nil, err
		}
		if item, err = dm.database.GetExecutor(ctx, dm.namespace.Name, nameOrID); err != nil {
			return nil, err
		}
	} else if item, err = dm.database.GetExecutorByID(ctx, dm.namespace.Name, itemID); err != nil {
		return nil, err
	}
	if item == nil {
		return nil, i18n.NewError(ctx, coremsgs.MsgDaiExecutorNotFound, nameOrID)
	}
	return item, nil
}

func (dm *daiManager) PublishTask(ctx context.Context, taskInput *core.TaskInput, waitConfirm bool) (*core.Task, error) {
	if err := fftypes.ValidateFFNameFieldNoUUID(ctx, taskInput.Name, "name"); err != nil {
		return nil, err
	}
	if existing, err := dm.database.GetTask(ctx, dm.namespace.Name, taskInput.Name); err != nil {
		return nil, err
	} else if existing != nil {
		return nil, i18n.NewError(ctx, coremsgs.MsgDaiTaskDuplicate, taskInput.Name)
	}
	for _, host := range *taskInput.Task.Hosts {
		id := host.GetString("NodeId")
		_, err := dm.GetExecutorByNameOrID(ctx, id)
		if err != nil {
			return nil, err
		}
	}
	if taskInput.ID.String() == "00000000-0000-0000-0000-000000000000" {
		taskInput.ID = fftypes.NewUUID()
	} else {
		if existing, err := dm.database.GetTaskByID(ctx, dm.namespace.Name, taskInput.ID); err != nil {
			return nil, err
		} else if existing != nil {
			return nil, i18n.NewError(ctx, coremsgs.MsgDaiTaskDuplicate, taskInput.ID)
		}
	}
	taskInput.Namespace = dm.namespace.Name
	taskInput.Status = TaskReady

	var op *core.Operation
	err := dm.database.RunAsGroup(ctx, func(ctx context.Context) (err error) {
		txid, err := dm.txHelper.SubmitNewTransaction(ctx, core.TransactionTypeDaiTask, "")
		if err != nil {
			return err
		}

		taskInput.TX.ID = txid
		taskInput.TX.Type = core.TransactionTypeDaiTask

		op = core.NewOperation(
			(*executor.Executor)(nil),
			dm.namespace.Name,
			txid,
			core.TransactionTypeDaiTask)
		if err = addTaskInputs(op, &taskInput.Task); err == nil {
			err = dm.operations.AddOrReuseOperation(ctx, op)
		}
		return err
	})
	if err != nil {
		return nil, err
	}

	send := func(ctx context.Context) error {
		_, err := dm.operations.RunOperation(ctx, opTask(op, &taskInput.Task))
		return err
	}
	if waitConfirm {
		_, err := dm.syncasync.WaitForInvokeOperation(ctx, op.ID, send)
		return &taskInput.Task, err
	}
	return &taskInput.Task, send(ctx)
}
func (dm *daiManager) StartTask(ctx context.Context, nameOrID string, waitConfirm bool) (*core.Task, error) {
	existing, err := dm.GetTaskByNameOrID(ctx, nameOrID)
	if err != nil {
		return nil, err
	} else if existing == nil {
		return nil, i18n.NewError(ctx, coremsgs.MsgDaiTaskNotFound, nameOrID)
	}
	if existing.Status != TaskReady && existing.Status != TaskTryStart {
		return nil, i18n.NewError(ctx, coremsgs.MsgDaiTaskInvalidOperation, nameOrID)
	}
	existing.Status = TaskTryStart

	var op *core.Operation
	err = dm.database.RunAsGroup(ctx, func(ctx context.Context) (err error) {
		txid, err := dm.txHelper.SubmitNewTransaction(ctx, core.TransactionTypeDaiTask, "")
		if err != nil {
			return err
		}

		existing.TX.ID = txid
		existing.TX.Type = core.TransactionTypeDaiTask

		op = core.NewOperation(
			(*executor.Executor)(nil),
			dm.namespace.Name,
			txid,
			core.TransactionTypeDaiTask)
		if err = addTaskInputs(op, existing); err == nil {
			err = dm.operations.AddOrReuseOperation(ctx, op)
		}
		return err
	})
	if err != nil {
		return nil, err
	}

	send := func(ctx context.Context) error {
		_, err := dm.operations.RunOperation(ctx, opTask(op, existing))
		return err
	}
	if waitConfirm {
		_, err := dm.syncasync.WaitForInvokeOperation(ctx, op.ID, send)
		return existing, err
	}
	return existing, send(ctx)
}
func (dm *daiManager) ConfirmTask(ctx context.Context, nameOrID string, waitConfirm bool) (*core.Task, error) {
	return nil, nil
}
func (dm *daiManager) StopTask(ctx context.Context, nameOrID string, waitConfirm bool) (*core.Task, error) {
	existing, err := dm.GetTaskByNameOrID(ctx, nameOrID)
	if err != nil {
		return nil, err
	} else if existing == nil {
		return nil, i18n.NewError(ctx, coremsgs.MsgDaiTaskNotFound, nameOrID)
	}
	if existing.Status != TaskStarted && existing.Status != TaskTryStop {
		return nil, i18n.NewError(ctx, coremsgs.MsgDaiTaskInvalidOperation, nameOrID)
	}
	existing.Status = TaskTryStop

	var op *core.Operation
	err = dm.database.RunAsGroup(ctx, func(ctx context.Context) (err error) {
		txid, err := dm.txHelper.SubmitNewTransaction(ctx, core.TransactionTypeDaiTask, "")
		if err != nil {
			return err
		}

		existing.TX.ID = txid
		existing.TX.Type = core.TransactionTypeDaiTask

		op = core.NewOperation(
			(*executor.Executor)(nil),
			dm.namespace.Name,
			txid,
			core.TransactionTypeDaiTask)
		if err = addTaskInputs(op, existing); err == nil {
			err = dm.operations.AddOrReuseOperation(ctx, op)
		}
		return err
	})
	if err != nil {
		return nil, err
	}

	send := func(ctx context.Context) error {
		_, err := dm.operations.RunOperation(ctx, opTask(op, existing))
		return err
	}
	if waitConfirm {
		_, err := dm.syncasync.WaitForInvokeOperation(ctx, op.ID, send)
		return existing, err
	}
	return existing, send(ctx)
}

func (dm *daiManager) updateTask(ctx context.Context, nameOrID string, status int, waitConfirm bool) (*core.Task, error) {
	existing, err := dm.database.GetTask(ctx, dm.namespace.Name, nameOrID)
	if err != nil {
		return nil, err
	} else if existing == nil {
		return nil, i18n.NewError(ctx, coremsgs.MsgDaiTaskNotFound, nameOrID)
	}
	existing.Status = status

	if existing.Status == TaskSuccess {
		existing.Results, _ = dm.mpcTaskResult(ctx, existing)
	}

	var op *core.Operation
	err = dm.database.RunAsGroup(ctx, func(ctx context.Context) (err error) {
		txid, err := dm.txHelper.SubmitNewTransaction(ctx, core.TransactionTypeDaiTask, "")
		if err != nil {
			return err
		}

		existing.TX.ID = txid
		existing.TX.Type = core.TransactionTypeDaiTask

		op = core.NewOperation(
			(*executor.Executor)(nil),
			dm.namespace.Name,
			txid,
			core.TransactionTypeDaiTask)
		if err = addTaskInputs(op, existing); err == nil {
			err = dm.operations.AddOrReuseOperation(ctx, op)
		}
		return err
	})
	if err != nil {
		return nil, err
	}

	send := func(ctx context.Context) error {
		_, err := dm.operations.RunOperation(ctx, opTask(op, existing))
		return err
	}
	if waitConfirm {
		_, err := dm.syncasync.WaitForInvokeOperation(ctx, op.ID, send)
		return existing, err
	}
	return existing, send(ctx)
}

func (dm *daiManager) GetTasks(ctx context.Context, filter database.AndFilter) ([]*core.Task, *database.FilterResult, error) {
	return dm.database.GetTasks(ctx, dm.namespace.Name, filter)
}
func (dm *daiManager) GetTaskByNameOrID(ctx context.Context, nameOrID string) (*core.Task, error) {
	var item *core.Task

	itemID, err := fftypes.ParseUUID(ctx, nameOrID)
	if err != nil {
		if err := fftypes.ValidateFFNameField(ctx, nameOrID, "name"); err != nil {
			return nil, err
		}
		if item, err = dm.database.GetTask(ctx, dm.namespace.Name, nameOrID); err != nil {
			return nil, err
		}
	} else if item, err = dm.database.GetTaskByID(ctx, dm.namespace.Name, itemID); err != nil {
		return nil, err
	}
	if item == nil {
		return nil, i18n.NewError(ctx, coremsgs.Msg404NotFound)
	}
	return item, nil
}

func (dm *daiManager) ExecutorContract(ctx context.Context, location *fftypes.JSONAny, event *blockchain.Event) (err error) {
	var executor executorData
	s := event.Output.GetString("payloadRef")
	if err := json.Unmarshal([]byte(s), &executor); err != nil {
		return i18n.WrapError(ctx, err, i18n.MsgJSONObjectParseFailed, s)
	}
	executor.Executor.Author = strings.ToLower(event.Output.GetString("author"))
	if executor.Executor.Status == ExecutorNormal {
		dm.wsConnect(ctx, executor.Executor)
	}
	return dm.database.UpsertExecutor(ctx, executor.Executor)
}

func (dm *daiManager) TaskContract(ctx context.Context, location *fftypes.JSONAny, event *blockchain.Event) (err error) {
	var task taskData
	s := event.Output.GetString("payloadRef")
	if err := json.Unmarshal([]byte(s), &task); err != nil {
		return i18n.WrapError(ctx, err, i18n.MsgJSONObjectParseFailed, s)
	}

	task.Task.Author = strings.ToLower(event.Output.GetString("author"))
	switch task.Task.Status {
	case TaskTryStart:
		if err := dm.mpcStart(ctx, task.Task); err != nil {
			log.L(dm.ctx).Errorf("mpc start task %s error %v", task.Task.ID, err)
			// TODO
		}
	case TaskTryStop:
		if err := dm.mpcStop(ctx, task.Task); err != nil {
			log.L(dm.ctx).Errorf("mpc stop task %s error %v", task.Task.ID, err)
			// TODO
		}
	}
	return dm.database.UpsertTask(ctx, task.Task)
}

// 更新节点状态
func (dm *daiManager) syncExecutorStatus(ctx context.Context) {
	log.L(dm.ctx).Infof("sync mpc node status ...")
	filter := database.ExecutorQueryFactory.NewFilter(ctx).Eq("author", dm.defaultKey)
	executors, _, err := dm.database.GetExecutors(ctx, dm.namespace.Name, filter)
	if err != nil {
		log.L(dm.ctx).Errorf("sync mpc node status, error %v", err)
	}
	for _, executor := range executors {
		status := ExecutorNormal
		if _, err := dm.mpcExecutor(ctx, executor); err != nil {
			status = ExecutorStop
		}
		if status != executor.Status {
			if _, err := dm.updateExecutor(ctx, executor.ID.String(), status, false); err != nil {
				log.L(dm.ctx).Errorf("sync mpc node status, nodeID %v, error %v", executor.ID, err)
			} else {
				log.L(dm.ctx).Infof("sync mpc node status, nodeID %v update status %v", executor.ID, status)
			}
		}
	}
}

// 更新任务状态 TaskTryStart、TaskStarted ->
func (dm *daiManager) syncTaskStatus(ctx context.Context) {
	log.L(dm.ctx).Infof("sync mpc task status ...")
	filter := database.TaskQueryFactory.NewFilter(ctx).Eq("author", dm.defaultKey).Builder().In("taskstatus", []driver.Value{TaskTryStart, TaskStarted})
	tasks, _, err := dm.database.GetTasks(ctx, dm.namespace.Name, filter)
	if err != nil {
		log.L(dm.ctx).Errorf("sync mpc task status, error %v", err)
	}
	for _, task := range tasks {
		result, err := dm.mpcTask(ctx, task)
		if err != nil {
			log.L(dm.ctx).Warningf("sync mpc task status, taskID %v error %v", task.ID, err)
			continue
		}
		updated := false
		status := task.Status
		msgs := result.GetObjectArray("msg")
		log.L(dm.ctx).Debugf("sync mpc task status, taskID  %v msg %v", task.ID, msgs)
		for _, msg := range msgs {
			switch msg.GetInt64("taskStatus") {
			case MPCTaskInit, MPCTaskProcess:
				updated = true
				status = TaskStarted
			case MPCTaskSuccess:
				updated = true
				status = TaskSuccess
			case MPCTaskFail:
				updated = true
				status = TaskFailed
			case MPCTaskStop:
				updated = true
				status = TaskStopped
			default:
			}
			if updated {
				break
			}
		}
		if updated {
			if status != task.Status {
				if _, err := dm.updateTask(ctx, task.ID.String(), status, false); err != nil {
					log.L(dm.ctx).Errorf("sync mpc task status, taskID %v update error %v", task.ID, err)
				} else {
					log.L(dm.ctx).Infof("sync mpc task status, taskID %v, update status %v", task.ID, status)
				}
			}
		}
	}
}

//func (dm *daiManager) retryMPCStart(ctx context.Context) {
//	log.L(dm.ctx).Infof("retry mpc start ...")
//	filter := database.TaskQueryFactory.NewFilter(ctx).Neq("author", "").Builder().Eq("taskstatus", TaskTryStart)
//	tasks, _, err := dm.database.GetTasks(ctx, dm.namespace.Name, filter)
//	if err != nil {
//		log.L(dm.ctx).Errorf("retry mpc start error %v", err)
//		return
//	}
//	for _, task := range tasks {
//		if err := dm.mpcStart(ctx, task); err != nil {
//			log.L(dm.ctx).Errorf("mpc start task %v status error %v", task.ID, err)
//		}
//	}
//}
//
//func (dm *daiManager) retryMPCStop(ctx context.Context) {
//	log.L(dm.ctx).Infof("retry mpc start ...")
//	filter := database.TaskQueryFactory.NewFilter(ctx).Neq("author", "").Builder().Eq("taskstatus", TaskTryStop)
//	tasks, _, err := dm.database.GetTasks(ctx, dm.namespace.Name, filter)
//	if err != nil {
//		log.L(dm.ctx).Errorf("retry mpc stop error %v", err)
//		return
//	}
//	for _, task := range tasks {
//		if err := dm.mpcStop(ctx, task); err != nil {
//			log.L(dm.ctx).Errorf("mpc stop task %v status error %v", task.ID, err)
//		}
//	}
//}

// TaskTryStart -> TaskStarted
func (dm *daiManager) mpcStart(ctx context.Context, task *core.Task) error {
	executors := map[string]*core.Executor{}
	for _, host := range *task.Hosts {
		id := host.GetString("NodeId")
		executor, err := dm.GetExecutorByNameOrID(ctx, id)
		if err != nil {
			return err
		}
		if executor.Author != dm.defaultKey {
			continue
		}
		executors[id] = executor
	}

	for _, executor := range executors {
		var url string
		if !strings.HasPrefix(executor.URL, "http") {
			url = "http://" + executor.URL
		} else {
			url = executor.URL
		}
		client := resty.New().SetBaseURL(url)
		body := map[string]interface{}{
			"task": task,
		}
		var result fftypes.JSONObject
		res, err := client.R().
			SetContext(ctx).
			SetResult(&result).
			SetBody(body).
			Post("/mpc/v1/startTask")
		if err != nil || !res.IsSuccess() {
			log.L(dm.ctx).Errorf("mpc start task, url %v, taskID %v, error %v, res %v", url, task.ID, err, res)
			return nil
		}
		log.L(dm.ctx).Infof("mpc start task, url %v, taskID %v, result %v", url, task.ID, result.String())
	}
	return nil
}

// TaskTryStop -> TaskStopped
func (dm *daiManager) mpcStop(ctx context.Context, task *core.Task) error {
	executors := map[string]*core.Executor{}
	for _, host := range *task.Hosts {
		id := host.GetString("NodeId")
		executor, err := dm.GetExecutorByNameOrID(ctx, id)
		if err != nil {
			return err
		}
		if executor.Author != dm.defaultKey {
			continue
		}
		executors[id] = executor
	}
	for _, executor := range executors {
		var url string
		if !strings.HasPrefix(executor.URL, "http") {
			url = "http://" + executor.URL
		} else {
			url = executor.URL
		}
		client := resty.New().SetBaseURL(url)
		body := map[string]interface{}{
			"TaskID": task.ID,
		}
		var result fftypes.JSONObject
		res, err := client.R().
			SetContext(ctx).
			SetResult(&result).
			SetBody(body).
			Post("/mpc/v1/stopTask")
		if err != nil || !res.IsSuccess() {
			log.L(dm.ctx).Errorf("mpc stop task, url %v, id %v, error %v, res %v", url, task.ID, err, res)
			return nil
		}
		log.L(dm.ctx).Infof("mpc stop task, url %v, id %v, result %v", url, task.ID, result.String())
	}
	return nil
}

// 获取MPC任务信息
func (dm *daiManager) mpcTask(ctx context.Context, task *core.Task) (fftypes.JSONObject, error) {
	executors := map[string]*core.Executor{}
	for _, host := range *task.Hosts {
		id := host.GetString("NodeId")
		executor, err := dm.GetExecutorByNameOrID(ctx, id)
		if err != nil {
			return nil, err
		}
		if executor.Author != dm.defaultKey {
			continue
		}
		executors[id] = executor
	}
	for _, executor := range executors {
		var url string
		if !strings.HasPrefix(executor.URL, "http") {
			url = "http://" + executor.URL
		} else {
			url = executor.URL
		}
		client := resty.New().SetBaseURL(url)
		body := map[string]interface{}{
			"TaskID": task.ID,
		}
		var result fftypes.JSONObject
		res, err := client.R().
			SetResult(&result).
			SetContext(ctx).
			SetBody(body).
			Post("/mpc/v1/getTaskByID")
		if err != nil || !res.IsSuccess() {
			log.L(dm.ctx).Errorf("mpc get task, url %v, taskID %v, error %v", url, task.ID, err)
			return nil, err
		}
		log.L(dm.ctx).Infof("mpc get task, url %v, taskID %v, result %v", url, task.ID, result.String())
		return result, nil
	}
	return nil, nil
}

func (dm *daiManager) mpcTaskResult(ctx context.Context, task *core.Task) (*fftypes.JSONObjectArray, error) {
	executors := map[string]*core.Executor{}
	for _, host := range *task.Hosts {
		id := host.GetString("NodeId")
		executor, err := dm.GetExecutorByNameOrID(ctx, id)
		if err != nil {
			return nil, err
		}
		if executor.Author != dm.defaultKey {
			continue
		}
		executors[id] = executor
	}
	for _, executor := range executors {
		var url string
		if !strings.HasPrefix(executor.URL, "http") {
			url = "http://" + executor.URL
		} else {
			url = executor.URL
		}
		client := resty.New().SetBaseURL(url)
		body := map[string]interface{}{
			"TaskID": task.ID,
		}
		var result fftypes.JSONObject
		res, err := client.R().
			SetResult(&result).
			SetContext(ctx).
			SetBody(body).
			Post("/mpc/v1/getTaskResultByID")
		if err != nil || !res.IsSuccess() {
			log.L(dm.ctx).Errorf("mpc get task result, url %v, taskID %v, error %v", url, task.ID, err)
			return nil, err
		}
		log.L(dm.ctx).Infof("mpc get task result, url %v, taskID %v, result %v", url, task.ID, result.String())

		var ret fftypes.JSONObjectArray
		if err := json.Unmarshal([]byte(result.GetString("results")), &ret); err != nil {
			log.L(dm.ctx).Errorf("mpc get task result, url %v, taskID %v, error %v", url, task.ID, err)
			return nil, err
		}
		return &ret, nil
	}
	return nil, nil
}

// 获取MPC节点信息
func (dm *daiManager) mpcExecutor(ctx context.Context, executor *core.Executor) (fftypes.JSONObject, error) {
	var url string
	if !strings.HasPrefix(executor.URL, "http") {
		url = "http://" + executor.URL
	} else {
		url = executor.URL
	}
	client := resty.New().SetBaseURL(url)
	body := map[string]interface{}{
		"NodeUUID": executor.ID,
	}
	var result fftypes.JSONObject
	res, err := client.R().
		SetResult(&result).
		SetContext(ctx).
		SetBody(body).
		Post("mpc/v1/getNodeByUUID")
	if err != nil || !res.IsSuccess() {
		log.L(dm.ctx).Errorf("mpc get executor node, url %v, nodeID %v, error %v, res %v", url, executor.ID, err, res)
		return nil, err
	}
	log.L(dm.ctx).Infof("mpc get executor node, url %v, nodeID %v, result %v", url, executor.ID, result.String())
	return result, nil
}

func (dm *daiManager) wsConnect(ctx context.Context, executor *core.Executor) {
	var url string
	if !strings.HasPrefix(executor.WS, "ws") {
		url = "ws://" + executor.WS
	} else {
		url = executor.WS
	}
	wsConfig := &wsclient.WSConfig{}
	wsConfig.HTTPURL = url
	wsConfig.WSKeyPath = "/mpc/v1/SubscribeTaskUpdate"
	wsConfig.HeartbeatInterval = 1000 * time.Millisecond
	wsConfig.InitialConnectAttempts = 10

	wsc, err := wsclient.New(context.Background(), wsConfig, nil, nil)
	if err != nil {
		log.L(dm.ctx).Errorf("add mpc executor ws, url %v, nodeID %v, err %v", wsConfig.HTTPURL, executor.ID, err)
		return
	}
	if err := wsc.Connect(); err != nil {
		log.L(dm.ctx).Errorf("add mpc executor ws, url %v, nodeID %v, err %v", wsConfig.HTTPURL, executor.ID, err)
		return
	}
	log.L(dm.ctx).Infof("add mpc executor ws, url %v, id %v", wsConfig.HTTPURL, executor.ID)
	go func(id string, wsc wsclient.WSClient) {
		dm.executorsMutex.Lock()
		if _, ok := dm.executors[executor.ID.String()]; ok {
			// Already Exist
			wsc.Close()
			dm.executorsMutex.Unlock()
			return
		}
		dm.executors[id] = wsc
		dm.executorsMutex.Unlock()
		for {
			select {
			case msgBytes, ok := <-wsc.Receive():
				if !ok {
					dm.executorsMutex.Lock()
					delete(dm.executors, id)
					dm.executorsMutex.Unlock()
					log.L(dm.ctx).Errorf("mpc executor ws exiting (receive channel closed). Terminating server!, nodeID %v", id)
					return
				}
				log.L(dm.ctx).Infof("mpc executor ws received %v", id, string(msgBytes))
				var msgs fftypes.JSONObjectArray
				err := json.Unmarshal(msgBytes, &msgs)
				if err != nil {
					log.L(dm.ctx).Errorf("mpc executor ws received unmarshal error %v", id, err)
					continue
				}
				for _, msg := range msgs {
					taskID := msg.GetString("taskID")
					taskStatus := TaskReady
					switch msg.GetInt64("taskStatus") {
					case MPCTaskInit, MPCTaskProcess:
						taskStatus = TaskStarted
					case MPCTaskSuccess:
						taskStatus = TaskSuccess
					case MPCTaskFail:
						taskStatus = TaskFailed
					case MPCTaskStop:
						taskStatus = TaskStopped
					default:
					}
					if TaskReady == taskStatus {
						if _, err := dm.updateTask(ctx, taskID, taskStatus, false); err != nil {
							log.L(dm.ctx).Errorf("mpc executor ws %v, update taskID %v status %v, error, %v", id, taskID, taskStatus, err)
						} else {
							log.L(dm.ctx).Infof("mpc executor ws %v, update taskID %v status %v", id, taskID, taskStatus)
						}
					}
				}
			}
		}
	}(executor.ID.String(), wsc)
}
