package definitions

import (
	"context"

	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
)

func (dh *definitionHandler) handleTrainingNodeBroadcast(ctx context.Context, state *core.BatchState, msg *core.Message, data core.DataArray, tx *fftypes.UUID) (HandlerResult, error) {
	var node core.TrainingNode
	valid := dh.getSystemBroadcastPayload(ctx, msg, data, &node)
	if !valid {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedBadPayload, "training node", msg.Header.ID)
	}
	node.Message = msg.Header.ID
	return dh.handleTrainingNode(ctx, state, &node, tx)
}
func (dh *definitionHandler) handleTrainingNode(ctx context.Context, state *core.BatchState, node *core.TrainingNode, tx *fftypes.UUID) (HandlerResult, error) {
	l := log.L(ctx)
	node.Namespace = dh.namespace.Name
	if err := node.Validate(ctx, true); err != nil {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedValidateFail, "training node", node.ID, err)
	}

	existing, err := dh.database.GetTrainingNodeByName(ctx, node.Namespace, node.Name)
	if err != nil {
		return HandlerResult{Action: ActionRetry}, err
	} else if existing != nil {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training node", "name", existing.Name)
	}

	existing, err = dh.database.GetTrainingNodeByID(ctx, node.Namespace, node.ID)
	if err != nil {
		return HandlerResult{Action: ActionRetry}, err
	} else if existing != nil {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training node", "id", existing.ID)
	}

	if err = dh.database.UpsertTrainingNode(ctx, node, false); err != nil {
		return HandlerResult{Action: ActionRetry}, err
	}

	l.Infof("training node created id=%s", node.ID)
	state.AddFinalize(func(ctx context.Context) error {
		event := core.NewEvent(core.EventTypeTrainingNodeConfirmed, node.Namespace, node.ID, tx, core.SystemTopicDefinitions)
		return dh.database.InsertEvent(ctx, event)
	})

	return HandlerResult{Action: ActionConfirm}, nil
}

func (dh *definitionHandler) handleTrainingNodeUpdateBroadcast(ctx context.Context, state *core.BatchState, msg *core.Message, data core.DataArray, tx *fftypes.UUID) (HandlerResult, error) {
	var node core.TrainingNode
	valid := dh.getSystemBroadcastPayload(ctx, msg, data, &node)
	if !valid {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedBadPayload, "training node updated", msg.Header.ID)
	}
	node.Message = msg.Header.ID
	return dh.handleTrainingNodeUpdate(ctx, state, &node, tx)
}

func (dh *definitionHandler) handleTrainingNodeUpdate(ctx context.Context, state *core.BatchState, node *core.TrainingNode, tx *fftypes.UUID) (HandlerResult, error) {
	l := log.L(ctx)
	node.Namespace = dh.namespace.Name
	if err := node.Validate(ctx, true); err != nil {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedValidateFail, "training node updated", node.ID, err)
	}

	existing, err := dh.database.GetTrainingNodeByID(ctx, node.Namespace, node.ID)
	if err != nil {
		return HandlerResult{Action: ActionRetry}, err
	} else if existing == nil || existing.Name != node.Name {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedTrainingNodeNotFound, "training node updated", "id", node.ID)
	}

	if err = dh.database.UpsertTrainingNode(ctx, node, true); err != nil {
		return HandlerResult{Action: ActionRetry}, err
	}

	l.Infof("training node updated id=%s", node.ID)
	state.AddFinalize(func(ctx context.Context) error {
		event := core.NewEvent(core.EventTypeTrainingNodeUpdated, node.Namespace, node.ID, tx, core.SystemTopicDefinitions)
		return dh.database.InsertEvent(ctx, event)
	})

	return HandlerResult{Action: ActionConfirm}, nil
}

func (dh *definitionHandler) handleTrainingModelBroadcast(ctx context.Context, state *core.BatchState, msg *core.Message, data core.DataArray, tx *fftypes.UUID) (HandlerResult, error) {
	var item core.TrainingModel
	valid := dh.getSystemBroadcastPayload(ctx, msg, data, &item)
	if !valid {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedBadPayload, "training model", msg.Header.ID)
	}
	item.Message = msg.Header.ID
	return dh.handleTrainingModel(ctx, state, &item, tx)
}

func (dh *definitionHandler) handleTrainingModel(ctx context.Context, state *core.BatchState, model *core.TrainingModel, tx *fftypes.UUID) (HandlerResult, error) {
	l := log.L(ctx)
	model.Namespace = dh.namespace.Name
	if err := model.Validate(ctx, true); err != nil {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedValidateFail, "training model", model.ID, err)
	}

	existing, err := dh.database.GetTrainingModelByName(ctx, model.Namespace, model.FlowName)
	if err != nil {
		return HandlerResult{Action: ActionRetry}, err
	} else if existing != nil {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training model", "flowName", existing.FlowName)
	}
	existing, err = dh.database.GetTrainingModelByID(ctx, model.Namespace, model.ID)
	if err != nil {
		return HandlerResult{Action: ActionRetry}, err
	} else if existing != nil {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training model", "id", existing.ID)
	}

	if err = dh.database.UpsertTrainingModel(ctx, model, false); err != nil {
		return HandlerResult{Action: ActionRetry}, err
	}

	l.Infof("training model created id=%s", model.ID)
	state.AddFinalize(func(ctx context.Context) error {
		event := core.NewEvent(core.EventTypeTrainingModelConfirmed, model.Namespace, model.ID, tx, core.SystemTopicDefinitions)
		return dh.database.InsertEvent(ctx, event)
	})

	return HandlerResult{Action: ActionConfirm}, nil
}

func (dh *definitionHandler) handleTrainingTaskBroadcast(ctx context.Context, state *core.BatchState, msg *core.Message, data core.DataArray, tx *fftypes.UUID) (HandlerResult, error) {
	var item core.TrainingTask
	valid := dh.getSystemBroadcastPayload(ctx, msg, data, &item)
	if !valid {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedBadPayload, "training task", msg.Header.ID)
	}
	item.Message = msg.Header.ID
	return dh.handleTrainingTask(ctx, state, &item, tx)
}

func (dh *definitionHandler) handleTrainingTask(ctx context.Context, state *core.BatchState, task *core.TrainingTask, tx *fftypes.UUID) (HandlerResult, error) {
	l := log.L(ctx)
	task.Namespace = dh.namespace.Name
	if err := task.Validate(ctx, true); err != nil {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedValidateFail, "training task", task.ID, err)
	}

	existing, err := dh.database.GetTrainingTaskByName(ctx, task.Namespace, task.Name)
	if err != nil {
		return HandlerResult{Action: ActionRetry}, err
	} else if existing != nil {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training task", "name", existing.Name)
	}
	existing, err = dh.database.GetTrainingTaskByID(ctx, task.Namespace, task.ID)
	if err != nil {
		return HandlerResult{Action: ActionRetry}, err
	} else if existing != nil {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training task", "id", existing.ID)
	}

	if err = dh.database.UpsertTrainingTask(ctx, task, false); err != nil {
		return HandlerResult{Action: ActionRetry}, err
	}

	l.Infof("training task created id=%s", task.ID)
	state.AddFinalize(func(ctx context.Context) error {
		event := core.NewEvent(core.EventTypeTrainingTaskConfirmed, task.Namespace, task.ID, tx, core.SystemTopicDefinitions)
		return dh.database.InsertEvent(ctx, event)
	})

	return HandlerResult{Action: ActionConfirm}, nil
}

func (dh *definitionHandler) handleTrainingJobBroadcast(ctx context.Context, state *core.BatchState, msg *core.Message, data core.DataArray, tx *fftypes.UUID) (HandlerResult, error) {
	var item core.TrainingJob
	valid := dh.getSystemBroadcastPayload(ctx, msg, data, &item)
	if !valid {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedBadPayload, "training job", msg.Header.ID)
	}
	item.Message = msg.Header.ID
	return dh.handleTrainingJob(ctx, state, &item, tx)
}

func (dh *definitionHandler) handleTrainingJob(ctx context.Context, state *core.BatchState, job *core.TrainingJob, tx *fftypes.UUID) (HandlerResult, error) {
	l := log.L(ctx)
	job.Namespace = dh.namespace.Name
	if err := job.Validate(ctx, true); err != nil {
		return HandlerResult{Action: ActionReject}, i18n.NewError(ctx, coremsgs.MsgDefRejectedValidateFail, "training job", job.ID, err)
	}

	if err := dh.database.UpsertTrainingJob(ctx, job, true); err != nil {
		return HandlerResult{Action: ActionRetry}, err
	}

	l.Infof("training job created id=%s", job.ID)
	state.AddFinalize(func(ctx context.Context) error {
		event := core.NewEvent(core.EventTypeTrainingJobConfirmed, job.Namespace, job.ID, tx, core.SystemTopicDefinitions)
		return dh.database.InsertEvent(ctx, event)
	})

	return HandlerResult{Action: ActionConfirm}, nil
}
