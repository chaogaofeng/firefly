package dai

import (
	"context"
	"encoding/json"
	"github.com/hyperledger/firefly-common/pkg/wsclient"
	"strings"
	"sync"
	"time"

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
		ctx:              ctx,
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
	// 更新Started任务的状态
	go dm.syncTaskStatus(ctx)
	go func() {
		timer := time.NewTicker(time.Minute)
		for {
			select {
			case <-timer.C:
				// 更新节点状态
				dm.syncExecutorStatus(ctx)
				// 更新Started任务的状态
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
		return nil, i18n.NewError(ctx, coremsgs.MsgDaiTaskDuplicate, executorInput.Name)
	}
	if executorInput.ID.String() == "00000000-0000-0000-0000-000000000000" {
		executorInput.ID = fftypes.NewUUID()
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
		return nil, i18n.NewError(ctx, coremsgs.Msg404NotFound, nameOrID)
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
		return nil, i18n.NewError(ctx, coremsgs.Msg404NotFound)
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
	if taskInput.ID.String() == "00000000-0000-0000-0000-000000000000" {
		taskInput.ID = fftypes.NewUUID()
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
	existing, err := dm.database.GetTask(ctx, dm.namespace.Name, nameOrID)
	if err != nil {
		return nil, err
	} else if existing == nil {
		return nil, i18n.NewError(ctx, coremsgs.Msg404NotFound, nameOrID)
	}
	if existing.Status != TaskReady && existing.Status != TaskTryStart {
		return nil, i18n.NewError(ctx, coremsgs.Msg404NoResult, nameOrID)
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
	existing, err := dm.database.GetTask(ctx, dm.namespace.Name, nameOrID)
	if err != nil {
		return nil, err
	} else if existing == nil {
		return nil, i18n.NewError(ctx, coremsgs.Msg404NotFound, nameOrID)
	}
	if existing.Status != TaskStarted && existing.Status != TaskTryStop {
		return nil, i18n.NewError(ctx, coremsgs.Msg404NoResult, nameOrID)
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
		return nil, i18n.NewError(ctx, coremsgs.Msg404NotFound, nameOrID)
	}
	existing.Status = status

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
		dm.mpcStart(ctx, task.Task)
	case TaskTryStop:
		dm.mpcStop(ctx, task.Task)
	}
	return dm.database.UpsertTask(ctx, task.Task)
}

// 更新节点状态
func (dm *daiManager) syncExecutorStatus(ctx context.Context) {
	log.L(ctx).Infof("sync mpc node status ...")
	filter := database.ExecutorQueryFactory.NewFilter(ctx).Eq("author", dm.defaultKey)
	executors, _, err := dm.database.GetExecutors(ctx, dm.namespace.Name, filter)
	if err != nil {
		log.L(ctx).Errorf("sync mpc node status error %v", err)
	}
	for _, executor := range executors {
		status := ExecutorNormal
		if _, err := dm.mpcExecutor(ctx, executor); err != nil {
			status = ExecutorStop
		}
		if status != executor.Status {
			if _, err := dm.updateExecutor(ctx, executor.ID.String(), status, false); err != nil {
				log.L(ctx).Errorf("sync mpc node %v status error %v", executor.ID, err)
			}
			log.L(ctx).Infof("sync mpc node %v status %v", executor.ID, status)
		}
	}
}

// 更新任务状态
func (dm *daiManager) syncTaskStatus(ctx context.Context) {
	log.L(ctx).Infof("sync mpc task status ...")
	filter := database.TaskQueryFactory.NewFilter(ctx).Eq("author", dm.defaultKey).Builder().Eq("taskstatus", TaskTryStart)
	tasks, _, err := dm.database.GetTasks(ctx, dm.namespace.Name, filter)
	if err != nil {
		log.L(ctx).Errorf("sync mpc task status error %v", err)
	}
	for _, task := range tasks {
		result, err := dm.mpcTask(ctx, task)
		if err != nil {
			continue
		}
		updated := false
		status := task.Status
		msgs := result.GetObjectArray("msg")
		for _, msg := range ([]fftypes.JSONObject)(msgs) {
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
		}
		if updated {
			if status != task.Status {
				if _, err := dm.updateTask(ctx, task.ID.String(), TaskStopped, false); err != nil {
					log.L(ctx).Errorf("sync mpc task %v status error %v", task.ID, err)
				} else {
					log.L(ctx).Infof("sync mpc task %v status  %v", task.ID, status)
				}
			}
			continue
		}
	}
}

//func (dm *daiManager) retryMPCStart(ctx context.Context) {
//	log.L(ctx).Infof("retry mpc start ...")
//	filter := database.TaskQueryFactory.NewFilter(ctx).Neq("author", "").Builder().Eq("taskstatus", TaskTryStart)
//	tasks, _, err := dm.database.GetTasks(ctx, dm.namespace.Name, filter)
//	if err != nil {
//		log.L(ctx).Errorf("retry mpc start error %v", err)
//		return
//	}
//	for _, task := range tasks {
//		if err := dm.mpcStart(ctx, task); err != nil {
//			log.L(ctx).Errorf("mpc start task %v status error %v", task.ID, err)
//		}
//	}
//}
//
//func (dm *daiManager) retryMPCStop(ctx context.Context) {
//	log.L(ctx).Infof("retry mpc start ...")
//	filter := database.TaskQueryFactory.NewFilter(ctx).Neq("author", "").Builder().Eq("taskstatus", TaskTryStop)
//	tasks, _, err := dm.database.GetTasks(ctx, dm.namespace.Name, filter)
//	if err != nil {
//		log.L(ctx).Errorf("retry mpc stop error %v", err)
//		return
//	}
//	for _, task := range tasks {
//		if err := dm.mpcStop(ctx, task); err != nil {
//			log.L(ctx).Errorf("mpc stop task %v status error %v", task.ID, err)
//		}
//	}
//}

// TaskTryStart -> TaskStarted
func (dm *daiManager) mpcStart(ctx context.Context, task *core.Task) error {
	executors := map[string]*core.Executor{}
	for _, host := range ([]fftypes.JSONObject)(*task.Hosts) {
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
			log.L(ctx).Errorf("mpc start task, url %v, taskID %v, error %v, res %v", url, task.ID, err, res)
			return nil
		}
		log.L(ctx).Infof("mpc start task, url %v, taskID %v, result %v", url, task.ID, result.String())
	}
	return nil
}

// TaskTryStop -> TaskStopped
func (dm *daiManager) mpcStop(ctx context.Context, task *core.Task) error {
	executors := map[string]*core.Executor{}
	for _, host := range ([]fftypes.JSONObject)(*task.Hosts) {
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
			log.L(ctx).Errorf("mpc stop task, url %v, id %v, error %v, res %v", url, task.ID, err, res)
			return nil
		}
		log.L(ctx).Infof("mpc stop task, url %v, id %v, result %v", url, task.ID, result.String())
	}
	return nil
}

// 获取MPC任务信息
func (dm *daiManager) mpcTask(ctx context.Context, task *core.Task) (fftypes.JSONObject, error) {
	executors := map[string]*core.Executor{}
	for _, host := range ([]fftypes.JSONObject)(*task.Hosts) {
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
			log.L(ctx).Errorf("mpc get task, url %v, taskID %v, error %v", url, task.ID, err)
			return nil, nil
		}
		log.L(ctx).Infof("mpc get task, url %v, taskID %v, result %v", url, task.ID, result.String())
		return result, nil
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
		Post("mpc/v1/getNodeByID")
	if err != nil || !res.IsSuccess() {
		log.L(ctx).Errorf("mpc get executor, url %v, id %v, error %v, res %v", url, executor.ID, err, res)
		return nil, nil
	}
	log.L(ctx).Infof("mpc get executor, url %v, id %v, result %v", url, executor.ID, result.String())
	return result, nil
}

func (dm *daiManager) wsConnect(ctx context.Context, executor *core.Executor) {
	wsConfig := &wsclient.WSConfig{}
	wsConfig.HTTPURL = executor.WS
	wsConfig.WSKeyPath = "/SubscribeTaskUpdate"
	wsConfig.HeartbeatInterval = 50 * time.Millisecond
	wsConfig.InitialConnectAttempts = 2

	wsc, err := wsclient.New(context.Background(), wsConfig, nil, nil)
	if err != nil {
		log.L(ctx).Errorf("add mpc executor ws, url %v, id %v, result %v", wsConfig.HTTPURL, executor.ID)
	}
	log.L(ctx).Infof("add mpc executor ws, url %v, id %v, result %v", wsConfig.HTTPURL, executor.ID)
	go func(id string, wsc wsclient.WSClient) {
		dm.executorsMutex.Lock()
		if _, ok := dm.executors[executor.ID.String()]; ok {
			wsc.Close()
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
					log.L(ctx).Errorf("mpc executor ws exiting (receive channel closed). Terminating server!, id %v", id)
					return
				}
				log.L(ctx).Infof("mpc executor ws %v, =========== %v", id, string(msgBytes))
				var result fftypes.JSONObject
				err := json.Unmarshal(msgBytes, &result)
				if err != nil {
					continue
				}
				taskID := result.GetString("taskID")
				taskStatus := TaskReady
				switch result.GetInt64("taskStatus") {
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
					log.L(ctx).Infof("mpc executor ws %v, update task %v status %v", id, taskID, taskStatus)
					dm.updateTask(ctx, taskID, taskStatus, false)
				}
			}
		}
	}(executor.ID.String(), wsc)
}
