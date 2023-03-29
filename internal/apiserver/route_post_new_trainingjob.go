package apiserver

import (
	"net/http"
	"strings"

	"github.com/hyperledger/firefly/internal/orchestrator"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
)

var postNewTrainingJob = &ffapi.Route{
	Name:       "postNewTrainingJob",
	Path:       "secretflow/jobs",
	Method:     http.MethodPost,
	PathParams: nil,
	QueryParams: []*ffapi.QueryParam{
		{Name: "confirm", Description: coremsgs.APIConfirmQueryParam, IsBool: true, Example: "true"},
	},
	Description:     coremsgs.APIEndpointsPostTrainingJob,
	JSONInputValue:  func() interface{} { return &core.TrainingJob{} },
	JSONOutputValue: func() interface{} { return &core.TrainingJob{} },
	JSONOutputCodes: []int{http.StatusAccepted, http.StatusOK},
	Extensions: &coreExtensions{
		EnabledIf: func(or orchestrator.Orchestrator) bool {
			return or.SecretFlow() != nil
		},
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			waitConfirm := strings.EqualFold(r.QP["confirm"], "true")
			r.SuccessStatus = syncRetcode(waitConfirm)
			err = cr.or.DefinitionSender().DefineTrainingJob(cr.ctx, r.Input.(*core.TrainingJob), waitConfirm)
			return r.Input, err
		},
	},
}
