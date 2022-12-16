package dai

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/ffresty"
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
	TaskReady = iota
	TaskProcessing
	TaskProcessed
	TaskStop
)

type Manager interface {
	core.Named

	RegisterExecutor(ctx context.Context, node *core.ExecutorInput, waitConfirm bool) (*core.Executor, error)
	StopExecutor(ctx context.Context, nameOrID string, waitConfirm bool) (*core.Executor, error)

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
	keyNormalization int

	executorsMutex sync.Mutex
	executors      map[string]*core.Executor
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
	// 同步MPC节点Started任务的状态
	// 向MPC节点发起Started的任务
	go dm.retryMPCStart(ctx)

	go func() {
		timer := time.NewTicker(time.Minute)
		for {
			select {
			case <-timer.C:
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

func (dm *daiManager) StopExecutor(ctx context.Context, nameOrID string, waitConfirm bool) (*core.Executor, error) {
	existing, err := dm.database.GetExecutor(ctx, dm.namespace.Name, nameOrID)
	if err != nil {
		return nil, err
	} else if existing == nil {
		return nil, i18n.NewError(ctx, coremsgs.Msg404NotFound, nameOrID)
	}
	if existing.Status != ExecutorNormal {
		return nil, i18n.NewError(ctx, coremsgs.Msg404NoResult, nameOrID)
	}
	existing.Status = ExecutorStop

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
	if existing.Status != TaskReady {
		return nil, i18n.NewError(ctx, coremsgs.Msg404NoResult, nameOrID)
	}
	existing.Status = TaskProcessing

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
	if existing.Status != TaskReady {
		return nil, i18n.NewError(ctx, coremsgs.Msg404NoResult, nameOrID)
	}
	existing.Status = TaskStop

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

func (dm *daiManager) finishTask(ctx context.Context, nameOrID string, waitConfirm bool) (*core.Task, error) {
	existing, err := dm.database.GetTask(ctx, dm.namespace.Name, nameOrID)
	if err != nil {
		return nil, err
	} else if existing == nil {
		return nil, i18n.NewError(ctx, coremsgs.Msg404NotFound, nameOrID)
	}
	if existing.Status != TaskProcessing {
		return nil, i18n.NewError(ctx, coremsgs.Msg404NoResult, nameOrID)
	}
	existing.Status = TaskProcessed

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
	executor.Executor.Author = event.Output.GetString("author")
	return dm.database.UpsertExecutor(ctx, executor.Executor)
}

func (dm *daiManager) TaskContract(ctx context.Context, location *fftypes.JSONAny, event *blockchain.Event) (err error) {
	var task taskData
	s := event.Output.GetString("payloadRef")
	if err := json.Unmarshal([]byte(s), &task); err != nil {
		return i18n.WrapError(ctx, err, i18n.MsgJSONObjectParseFailed, s)
	}
	task.Task.Author = event.Output.GetString("author")
	return dm.database.UpsertTask(ctx, task.Task)
}

func (dm *daiManager) syncTaskStatus(ctx context.Context) {
	log.L(ctx).Error("syncTaskStatus ...")
	filter := database.TaskQueryFactory.NewFilter(ctx).Eq("taskstatus", TaskProcessing)
	tasks, _, err := dm.database.GetTasks(ctx, dm.namespace.Name, filter)
	if err != nil {
		log.L(ctx).Error("syncTaskStatus", "error", err)
	}
	for _, task := range tasks {
		result, err := dm.mpcTask(ctx, task)
		if err != nil {
			continue
		}
		finished := false
		msgs := result.GetObjectArray("msg")
		for _, msg := range ([]fftypes.JSONObject)(msgs) {
			if msg.GetInt64("taskStatus") == TaskProcessed {
				finished = true
			}
		}
		if finished {
			dm.finishTask(ctx, task.ID.String(), false)
		}
	}
}

func (dm *daiManager) retryMPCStart(ctx context.Context) {
	log.L(ctx).Error("retryMPCStart ...")
	filter := database.TaskQueryFactory.NewFilter(ctx).Eq("taskstatus", TaskProcessing)
	tasks, _, err := dm.database.GetTasks(ctx, dm.namespace.Name, filter)
	if err != nil {
		log.L(ctx).Error("retryMPCStart", "error", err)
		return
	}
	for _, task := range tasks {
		if result, err := dm.mpcTask(ctx, task); err != nil {
			continue
		} else {
			finished := false
			msgs := result.GetObjectArray("msg")
			for _, msg := range ([]fftypes.JSONObject)(msgs) {
				if msg.GetInt64("taskStatus") == TaskProcessed {
					finished = true
					dm.finishTask(ctx, task.ID.String(), false)
				}
			}
			if finished {
				dm.finishTask(ctx, task.ID.String(), false)
				continue
			}
		}
		dm.mpcStart(ctx, task)
	}
}

func (dm *daiManager) mpcStart(ctx context.Context, task *core.Task) error {
	urls := map[string]bool{}
	hosts := task.Hosts
	for _, host := range ([]fftypes.JSONObject)(*hosts) {
		url := host.GetString("URL")
		if urls[url] {
			continue
		}
		urls[url] = true
		config.Set(ffresty.HTTPConfigURL, url+":8000")
		client := resty.New().SetBaseURL(fmt.Sprintf("http://%s:8000", url))
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
			log.L(ctx).Info("MPC start task", "url", url, "taskID", task.ID, "error", err)
			return nil
		}
		log.L(ctx).Info("MPC start task", "url", url, "taskID", task.ID, "response", result.String())
	}
	return nil
}

func (dm *daiManager) mpcStop(ctx context.Context, task *core.Task) error {
	urls := map[string]bool{}
	hosts := task.Hosts
	for _, host := range ([]fftypes.JSONObject)(*hosts) {
		url := host.GetString("URL")
		if urls[url] {
			continue
		}
		urls[url] = true

		client := resty.New().SetBaseURL(fmt.Sprintf("http://%s:8000", url))
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
			log.L(ctx).Info("MPC stop task", "url", url, "taskID", task.ID, "error", err)
			return nil
		}
		log.L(ctx).Info("MPC stop task", "url", url, "taskID", task.ID, "response", result.String())
	}
	return nil
}

func (dm *daiManager) mpcTask(ctx context.Context, task *core.Task) (fftypes.JSONObject, error) {
	urls := map[string]bool{}
	hosts := task.Hosts
	for _, host := range ([]fftypes.JSONObject)(*hosts) {
		url := host.GetString("URL")
		if urls[url] {
			continue
		}
		urls[url] = true

		client := resty.New().SetBaseURL(fmt.Sprintf("http://%s:8000", url))
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
			log.L(ctx).Info("MPC get task", "url", url, "taskID", task.ID, "error", err)
			return nil, nil
		}
		log.L(ctx).Info("MPC get task", "url", url, "taskID", task.ID, "response", result.String())
		return result, nil
	}
	return nil, nil
}
