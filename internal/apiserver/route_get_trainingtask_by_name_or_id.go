package apiserver

import (
	"github.com/hyperledger/firefly/internal/orchestrator"
	"net/http"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
)

var getTrainingTask = &ffapi.Route{
	Name:   "getTrainingTask",
	Path:   "secretflow/tasks/{nameOrID}",
	Method: http.MethodGet,
	PathParams: []*ffapi.PathParam{
		{Name: "nameOrID", Description: coremsgs.APIParamsTrainingTaskNameOrID},
	},
	QueryParams:     nil,
	Description:     coremsgs.APIEndpointsGetTrainingTask,
	JSONInputValue:  nil,
	JSONOutputValue: func() interface{} { return &core.TrainingTask{} },
	JSONOutputCodes: []int{http.StatusOK},
	Extensions: &coreExtensions{
		EnabledIf: func(or orchestrator.Orchestrator) bool {
			return or.SecretFlow() != nil
		},
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			return cr.or.SecretFlow().GetTrainingTaskByNameOrID(cr.ctx, r.PP["nameOrID"])
		},
	},
}
