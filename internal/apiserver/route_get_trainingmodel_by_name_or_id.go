package apiserver

import (
	"github.com/hyperledger/firefly/internal/orchestrator"
	"net/http"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
)

var getTrainingModel = &ffapi.Route{
	Name:   "getTrainingModel",
	Path:   "secretflow/models/{nameOrID}",
	Method: http.MethodGet,
	PathParams: []*ffapi.PathParam{
		{Name: "nameOrID", Description: coremsgs.APIParamsTrainingModelNameOrID},
	},
	QueryParams:     nil,
	Description:     coremsgs.APIEndpointsGetTrainingModel,
	JSONInputValue:  nil,
	JSONOutputValue: func() interface{} { return &core.TrainingModel{} },
	JSONOutputCodes: []int{http.StatusOK},
	Extensions: &coreExtensions{
		EnabledIf: func(or orchestrator.Orchestrator) bool {
			return or.SecretFlow() != nil
		},
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			return cr.or.SecretFlow().GetTrainingModelByNameOrID(cr.ctx, r.PP["nameOrID"])
		},
	},
}
