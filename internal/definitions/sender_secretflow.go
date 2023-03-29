package definitions

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
)

func (bm *definitionSender) DefineTrainingNode(ctx context.Context, node *core.TrainingNode, waitConfirm bool) error {
	if err := node.Validate(ctx, false); err != nil {
		return err
	}
	if existing, err := bm.database.GetTrainingNodeByName(ctx, bm.namespace, node.Name); err != nil {
		return err
	} else if existing != nil {
		return i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training node", "name already exist", node.Name)
	}

	nodeOwningOrg, err := bm.identity.GetMultipartyRootOrg(ctx)
	if err != nil {
		return err
	}
	node.Parent = nodeOwningOrg.DID
	node.ID = fftypes.NewUUID()
	if bm.multiparty {
		node.Namespace = ""
		msg, err := bm.sendDefinitionDefault(ctx, node, core.SystemTagDefineTrainingNode, waitConfirm)
		if msg != nil {
			node.Message = msg.Header.ID
		}
		node.Namespace = bm.namespace
		return err
	}

	return fakeBatch(ctx, func(ctx context.Context, state *core.BatchState) (HandlerResult, error) {
		return bm.handler.handleTrainingNode(ctx, state, node, nil)
	})
}

func (bm *definitionSender) UpdateTrainingNode(ctx context.Context, nameOrID string, node *core.TrainingNode, waitConfirm bool) error {
	var existing *core.TrainingNode
	if uuid, err := fftypes.ParseUUID(ctx, nameOrID); err != nil {
		if existing, err = bm.database.GetTrainingNodeByName(ctx, bm.namespace, nameOrID); err != nil {
			return err
		}
	} else if existing, err = bm.database.GetTrainingNodeByID(ctx, bm.namespace, uuid); err != nil {
		return err
	}
	if existing == nil {
		return i18n.NewError(ctx, coremsgs.MsgDefRejectedTrainingNodeNotFound, "training node", "nameOrID not found", node.Name)
	}

	nodeOwningOrg, err := bm.identity.GetMultipartyRootOrg(ctx)
	if err != nil {
		return err
	}
	if existing.Parent != nodeOwningOrg.DID {
		return i18n.NewError(ctx, coremsgs.MsgAuthorInvalid)
	}
	node.ID = existing.ID
	node.Name = existing.Name
	node.Parent = existing.Parent

	if err := node.Validate(ctx, true); err != nil {
		return err
	}
	if bm.multiparty {
		node.Namespace = ""
		msg, err := bm.sendDefinitionDefault(ctx, node, core.SystemTagDefineTrainingNodeUpdate, waitConfirm)
		if msg != nil {
			node.Message = msg.Header.ID
		}
		node.Namespace = bm.namespace
		return err
	}

	return fakeBatch(ctx, func(ctx context.Context, state *core.BatchState) (HandlerResult, error) {
		return bm.handler.handleTrainingNodeUpdate(ctx, state, node, nil)
	})
}

func (bm *definitionSender) DefineTrainingModel(ctx context.Context, model *core.TrainingModel, waitConfirm bool) error {
	if err := model.Validate(ctx, false); err != nil {
		return err
	}
	if existing, err := bm.database.GetTrainingModelByName(ctx, bm.namespace, model.FlowName); err != nil {
		return err
	} else if existing != nil {
		return i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training model", "flowName already exist", model.FlowName)
	}

	model.ID = fftypes.NewUUID()
	if bm.multiparty {
		model.Namespace = ""
		msg, err := bm.sendDefinitionDefault(ctx, model, core.SystemTagDefineTrainingModel, waitConfirm)
		if msg != nil {
			model.Message = msg.Header.ID
		}
		model.Namespace = bm.namespace
		return err
	}

	return fakeBatch(ctx, func(ctx context.Context, state *core.BatchState) (HandlerResult, error) {
		return bm.handler.handleTrainingModel(ctx, state, model, nil)
	})
}

func (bm *definitionSender) DefineTrainingTask(ctx context.Context, task *core.TrainingTask, waitConfirm bool) error {
	var model *core.TrainingModel
	if _, err := fftypes.ParseUUID(ctx, task.TrainingModel); err != nil {
		model, err = bm.database.GetTrainingModelByName(ctx, bm.namespace, task.TrainingModel)
		if err != nil {
			return err
		}
		task.TrainingModel = model.ID.String()
	} else {
		model, err = bm.database.GetTrainingModelByID(ctx, bm.namespace, fftypes.MustParseUUID(task.TrainingModel))
		if err != nil {
			return err
		}
	}
	if model == nil {
		return i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training task", "model not found", task.TrainingModel)
	}
	if len(*model.FlowParties) != len(*task.Parties) {
		return i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training task", "party size mismatch", task.TrainingModel)
	}

	for _, party := range *task.Parties {
		if existing, err := bm.database.GetTrainingNodeByName(ctx, bm.namespace, party); err != nil {
			return err
		} else if existing == nil {
			return i18n.NewError(ctx, coremsgs.MsgDefRejectedTrainingNodeNotFound, "training task", "party not found", party)
		}
	}

	if err := task.Validate(ctx, false); err != nil {
		return err
	}
	if existing, err := bm.database.GetTrainingTaskByName(ctx, bm.namespace, task.Name); err != nil {
		return err
	} else if existing != nil {
		return i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training task", "name already exist", task.Name)
	}

	task.ID = fftypes.NewUUID()
	if bm.multiparty {
		task.Namespace = ""
		msg, err := bm.sendDefinitionDefault(ctx, task, core.SystemTagDefineTrainingTask, waitConfirm)
		if msg != nil {
			task.Message = msg.Header.ID
		}
		task.Namespace = bm.namespace
		return err
	}

	return fakeBatch(ctx, func(ctx context.Context, state *core.BatchState) (HandlerResult, error) {
		return bm.handler.handleTrainingTask(ctx, state, task, nil)
	})
}

func (bm *definitionSender) DefineTrainingJob(ctx context.Context, job *core.TrainingJob, waitConfirm bool) error {
	var task *core.TrainingTask
	if _, err := fftypes.ParseUUID(ctx, job.TrainingTask); err != nil {
		task, err = bm.database.GetTrainingTaskByName(ctx, bm.namespace, job.TrainingTask)
		if err != nil {
			return err
		}
		job.TrainingTask = task.ID.String()
	} else {
		task, err = bm.database.GetTrainingTaskByID(ctx, bm.namespace, fftypes.MustParseUUID(job.TrainingTask))
		if err != nil {
			return err
		}
	}
	if task == nil {
		return i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training job", "task not found", job.TrainingTask)
	}

	if err := job.Validate(ctx, false); err != nil {
		return err
	}
	if existing, err := bm.database.GetTrainingJobByName(ctx, bm.namespace, job.Name); err != nil {
		return err
	} else if existing != nil {
		return i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training job", "name already exist", job.Name)
	}

	job.ID = fftypes.NewUUID()
	job.Status = core.JobStatusPending
	if err := bm.fillPipeline(ctx, job); err != nil {
		return err
	}
	if bm.multiparty {
		job.Namespace = ""
		msg, err := bm.sendDefinitionDefault(ctx, job, core.SystemTagDefineTrainingJob, waitConfirm)
		if msg != nil {
			job.Message = msg.Header.ID
		}
		job.Namespace = bm.namespace
		return err
	}

	return fakeBatch(ctx, func(ctx context.Context, state *core.BatchState) (HandlerResult, error) {
		return bm.handler.handleTrainingJob(ctx, state, job, nil)
	})
}

func (bm *definitionSender) DefineTrainingJobUpdate(ctx context.Context, job *core.TrainingJob, waitConfirm bool) error {
	if err := job.Validate(ctx, true); err != nil {
		return err
	}
	// update
	if existing, err := bm.database.GetTrainingJobByID(ctx, bm.namespace, job.ID); err != nil {
		return err
	} else if existing == nil || existing.Name != job.Name {
		return i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training job", "id not found or name mismatch", job.ID)
	}

	if bm.multiparty {
		job.Namespace = ""
		msg, err := bm.sendDefinitionDefault(ctx, job, core.SystemTagDefineTrainingJobUpdate, waitConfirm)
		if msg != nil {
			job.Message = msg.Header.ID
		}
		job.Namespace = bm.namespace
		return err
	}

	return fakeBatch(ctx, func(ctx context.Context, state *core.BatchState) (HandlerResult, error) {
		return bm.handler.handleTrainingJob(ctx, state, job, nil)
	})
}

func (bm *definitionSender) fillPipeline(ctx context.Context, job *core.TrainingJob) error {
	tt, err := bm.database.GetTrainingTaskByID(ctx, bm.namespace, fftypes.MustParseUUID(job.TrainingTask))
	if err != nil {
		return err
	} else if tt == nil {
		return i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training job", "task not found", job.TrainingTask)
	}
	tm, err := bm.database.GetTrainingModelByID(ctx, bm.namespace, fftypes.MustParseUUID(tt.TrainingModel))
	if err != nil {
		return err
	} else if tm == nil {
		return i18n.NewError(ctx, coremsgs.MsgDefRejectedConflict, "training job", "model not found", tt.TrainingModel)
	}

	bts, err := json.Marshal(tm.FlowItems)
	if err != nil {
		return err
	}

	flowItems := strings.ReplaceAll(string(bts), tm.FlowID, job.ID.String())
	for i, party := range *tm.FlowParties {
		flowItems = strings.ReplaceAll(flowItems, party, (*tt.Parties)[i])
	}

	var items fftypes.JSONObjectArray
	if err := json.Unmarshal([]byte(flowItems), &items); err != nil {
		return err
	}

	var pipeline fftypes.JSONObject
	pipeline = map[string]interface{}{
		"flowItems":   items,
		"flowId":      job.ID.String(),
		"flowName":    tm.FlowName,
		"flowParties": tt.Parties,
	}
	job.Pipeline = &pipeline
	return nil
}
