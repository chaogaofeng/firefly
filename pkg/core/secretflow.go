package core

import (
	"context"

	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
)

type TrainingNodeBase struct {
	Name        string `ffstruct:"TrainingNode" json:"name" ffexcludeinput:"patchUpdateTrainingNode"`
	Head        string `ffstruct:"TrainingNode" json:"head"`
	Address     string `ffstruct:"TrainingNode" json:"address"`
	Port        uint   `ffstruct:"TrainingNode" json:"port"`
	RangeStart  uint   `ffstruct:"TrainingNode" json:"range_start"`
	RangeEnd    uint   `ffstruct:"TrainingNode" json:"range_end"`
	Status      uint   `ffstruct:"TrainingNode" json:"status"`
	Description string `ffstruct:"TrainingNode" json:"description,omitempty"`
}

type TrainingNode struct {
	ID        *fftypes.UUID `ffstruct:"TrainingNode" json:"id,omitempty" ffexcludeinput:"true"`
	Namespace string        `ffstruct:"TrainingNode" json:"namespace,omitempty" ffexcludeinput:"true"`
	Message   *fftypes.UUID `ffstruct:"TrainingNode" json:"message,omitempty" ffexcludeinput:"true"`
	Parent    string        `ffstruct:"TrainingNode" json:"parent" ffexcludeinput:"true"`
	TrainingNodeBase
}

func (tt *TrainingNode) Validate(ctx context.Context, existing bool) (err error) {
	if err = fftypes.ValidateFFNameFieldNoUUID(ctx, tt.Name, "name"); err != nil {
		return err
	}
	if err = fftypes.ValidateFFNameField(ctx, tt.Name, "name"); err != nil {
		return err
	}
	if len(tt.Head) == 0 {
		return i18n.NewError(ctx, i18n.MsgMissingRequiredField, "head")
	}
	if len(tt.Address) == 0 {
		return i18n.NewError(ctx, i18n.MsgMissingRequiredField, "address")
	}
	if tt.Port == 0 {
		return i18n.NewError(ctx, i18n.MsgMissingRequiredField, "port")
	}
	if tt.RangeStart == 0 {
		return i18n.NewError(ctx, i18n.MsgMissingRequiredField, "range_start")
	}
	if tt.RangeEnd == 0 {
		return i18n.NewError(ctx, i18n.MsgMissingRequiredField, "range_end")
	}
	if err = fftypes.ValidateLength(ctx, tt.Description, "description", 4096); err != nil {
		return err
	}
	if existing {
		if tt.ID == nil {
			return i18n.NewError(ctx, i18n.MsgNilID)
		}
		if len(tt.Parent) == 0 {
			return i18n.NewError(ctx, i18n.MsgMissingRequiredField, "parent")
		}
	}
	return nil
}

func (tt *TrainingNode) Topic() string {
	return fftypes.TypeNamespaceNameTopicHash("training_node", tt.Namespace, tt.ID.String())
}

func (tt *TrainingNode) SetBroadcastMessage(msgID *fftypes.UUID) {
	tt.Message = msgID
}

type TrainingModelBase struct {
	FlowID      string                   `ffstruct:"TrainingModel" json:"flowId"`
	FlowName    string                   `ffstruct:"TrainingModel" json:"flowName"`
	FlowParties *fftypes.FFStringArray   `ffstruct:"TrainingModel" json:"flowParties"`
	FlowItems   *fftypes.JSONObjectArray `ffstruct:"TrainingModel" json:"flowItems"`
}

type TrainingModel struct {
	ID        *fftypes.UUID `ffstruct:"TrainingModel" json:"id,omitempty" ffexcludeinput:"true"`
	Namespace string        `ffstruct:"TrainingModel" json:"namespace,omitempty" ffexcludeinput:"true"`
	Message   *fftypes.UUID `ffstruct:"TrainingModel" json:"message,omitempty" ffexcludeinput:"true"`
	TrainingModelBase
}

func (tm *TrainingModel) Validate(ctx context.Context, existing bool) (err error) {
	if err = fftypes.ValidateFFNameFieldNoUUID(ctx, tm.FlowName, "name"); err != nil {
		return err
	}
	if len(tm.FlowName) == 0 {
		return i18n.NewError(ctx, i18n.MsgMissingRequiredField, "flowName")
	}
	if len(tm.FlowID) == 0 {
		return i18n.NewError(ctx, i18n.MsgMissingRequiredField, "flowId")
	}
	if tm.FlowParties == nil || len(*tm.FlowParties) == 0 {
		return i18n.NewError(ctx, i18n.MsgMissingRequiredField, "flowParties")
	}
	if tm.FlowItems == nil || len(*tm.FlowItems) == 0 {
		return i18n.NewError(ctx, i18n.MsgMissingRequiredField, "flowItems")
	}
	if existing {
		if tm.ID == nil {
			return i18n.NewError(ctx, i18n.MsgNilID)
		}
	}
	return nil
}

func (tm *TrainingModel) Topic() string {
	return fftypes.TypeNamespaceNameTopicHash("training_model", tm.Namespace, tm.ID.String())
}

func (tm *TrainingModel) SetBroadcastMessage(msgID *fftypes.UUID) {
	tm.Message = msgID
}

type TrainingTaskBase struct {
	Parties       *fftypes.FFStringArray `ffstruct:"TrainingTask" json:"parties"`
	TrainingModel string                 `ffstruct:"TrainingTask" json:"model"`
}

type TrainingTask struct {
	ID        *fftypes.UUID `ffstruct:"TrainingTask" json:"id,omitempty" ffexcludeinput:"true"`
	Namespace string        `ffstruct:"TrainingTask" json:"namespace,omitempty" ffexcludeinput:"true"`
	Message   *fftypes.UUID `ffstruct:"TrainingTask" json:"message,omitempty" ffexcludeinput:"true"`
	// Group      *InputGroup   `ffstruct:"TrainingTask" json:"group"`
	Name string `ffstruct:"TrainingTask" json:"name"`
	TrainingTaskBase
}

func (tt *TrainingTask) Validate(ctx context.Context, existing bool) (err error) {
	if err = fftypes.ValidateFFNameFieldNoUUID(ctx, tt.Name, "name"); err != nil {
		return err
	}
	if err = fftypes.ValidateFFNameField(ctx, tt.Name, "name"); err != nil {
		return err
	}
	if _, err = fftypes.ParseUUID(ctx, tt.TrainingModel); err != nil {
		return i18n.NewError(ctx, i18n.MsgInvalidUUID, "model_id")
	}
	if tt.Parties == nil || len(*tt.Parties) == 0 {
		return i18n.NewError(ctx, i18n.MsgMissingRequiredField, "parties")
	}
	if existing {
		if tt.ID == nil {
			return i18n.NewError(ctx, i18n.MsgNilID)
		}
	}
	return nil
}

func (tt *TrainingTask) Topic() string {
	return fftypes.TypeNamespaceNameTopicHash("training_task", tt.Namespace, tt.ID.String())
}

func (tt *TrainingTask) SetBroadcastMessage(msgID *fftypes.UUID) {
	tt.Message = msgID
}

type JobStatus string

const (
	// JobStatusPending indicates the operation has been submitted, but is not yet confirmed as successful or failed
	JobStatusPending JobStatus = "Pending"
	// JobStatusSucceeded the infrastructure runtime has returned success for the operation
	JobStatusSucceeded JobStatus = "Succeeded"
	// JobStatusFailed happens when an error is reported by the infrastructure runtime
	JobStatusFailed JobStatus = "Failed"
)

type TrainingJob struct {
	ID           *fftypes.UUID       `ffstruct:"TrainingJob" json:"id,omitempty" ffexcludeinput:"true"`
	Name         string              `ffstruct:"TrainingJob" json:"name"`
	Namespace    string              `ffstruct:"TrainingJob" json:"namespace,omitempty" ffexcludeinput:"true"`
	Message      *fftypes.UUID       `ffstruct:"TrainingJob" json:"message,omitempty" ffexcludeinput:"true"`
	TrainingTask string              `ffstruct:"TrainingJob" json:"task"`
	Pipeline     *fftypes.JSONObject `ffstruct:"TrainingJob" json:"pipeline" ffexcludeinput:"true"`
	Status       JobStatus           `ffstruct:"TrainingJob" json:"status" ffexcludeinput:"true"`
	Created      *fftypes.FFTime     `ffstruct:"TrainingJob" json:"created,omitempty" ffexcludeinput:"true"`
	Updated      *fftypes.FFTime     `ffstruct:"TrainingJob" json:"updated,omitempty" ffexcludeinput:"true"`
}

func (tt *TrainingJob) Validate(ctx context.Context, existing bool) (err error) {
	if err = fftypes.ValidateFFNameField(ctx, tt.Name, "name"); err != nil {
		return err
	}

	if _, err = fftypes.ParseUUID(ctx, tt.TrainingTask); err != nil {
		return i18n.NewError(ctx, i18n.MsgInvalidUUID, "task_id")
	}
	if existing {
		if tt.ID == nil {
			return i18n.NewError(ctx, i18n.MsgNilID)
		}
	}
	return nil
}

func (tt *TrainingJob) Topic() string {
	return fftypes.TypeNamespaceNameTopicHash("training_task", tt.Namespace, tt.TrainingTask)
}

func (tt *TrainingJob) SetBroadcastMessage(msgID *fftypes.UUID) {
	tt.Message = msgID
}
