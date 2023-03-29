package apiserver

import (
	"net/http"

	"github.com/hyperledger/firefly/internal/orchestrator"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
)

var getTrainingJob = &ffapi.Route{
	Name:   "getTrainingJob",
	Path:   "secretflow/jobs/{nameOrID}",
	Method: http.MethodGet,
	PathParams: []*ffapi.PathParam{
		{Name: "nameOrID", Description: coremsgs.APIParamsTrainingJobNameOrID},
	},
	QueryParams:     nil,
	Description:     coremsgs.APIEndpointsGetTrainingJob,
	JSONInputValue:  nil,
	JSONOutputValue: func() interface{} { return &core.TrainingJob{} },
	JSONOutputCodes: []int{http.StatusOK},
	Extensions: &coreExtensions{
		EnabledIf: func(or orchestrator.Orchestrator) bool {
			return or.SecretFlow() != nil
		},
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			return cr.or.SecretFlow().GetTrainingJobByNameOrID(cr.ctx, r.PP["nameOrID"])
		},
	},
}
