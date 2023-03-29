package apiserver

import (
	"net/http"
	"strings"

	"github.com/hyperledger/firefly/internal/orchestrator"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
)

var postNewTrainingTask = &ffapi.Route{
	Name:       "postNewTrainingTask",
	Path:       "secretflow/tasks",
	Method:     http.MethodPost,
	PathParams: nil,
	QueryParams: []*ffapi.QueryParam{
		{Name: "confirm", Description: coremsgs.APIConfirmQueryParam, IsBool: true, Example: "true"},
	},
	Description:     coremsgs.APIEndpointsPostNewTrainingTask,
	JSONInputValue:  func() interface{} { return &core.TrainingTask{} },
	JSONOutputValue: func() interface{} { return &core.TrainingTask{} },
	JSONOutputCodes: []int{http.StatusAccepted, http.StatusOK},
	Extensions: &coreExtensions{
		EnabledIf: func(or orchestrator.Orchestrator) bool {
			return or.SecretFlow() != nil
		},
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			waitConfirm := strings.EqualFold(r.QP["confirm"], "true")
			r.SuccessStatus = syncRetcode(waitConfirm)
			err = cr.or.DefinitionSender().DefineTrainingTask(cr.ctx, r.Input.(*core.TrainingTask), waitConfirm)
			return r.Input, err
		},
	},
}
