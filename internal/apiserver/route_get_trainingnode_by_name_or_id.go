package apiserver

import (
	"github.com/hyperledger/firefly/internal/orchestrator"
	"net/http"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
)

var getTrainingNode = &ffapi.Route{
	Name:   "getTrainingNode",
	Path:   "secretflow/nodes/{nameOrID}",
	Method: http.MethodGet,
	PathParams: []*ffapi.PathParam{
		{Name: "nameOrID", Description: coremsgs.APIParamsTrainingNodeNameOrID},
	},
	QueryParams:     nil,
	Description:     coremsgs.APIEndpointsGetTrainingNode,
	JSONInputValue:  nil,
	JSONOutputValue: func() interface{} { return &core.TrainingNode{} },
	JSONOutputCodes: []int{http.StatusOK},
	Extensions: &coreExtensions{
		EnabledIf: func(or orchestrator.Orchestrator) bool {
			return or.SecretFlow() != nil
		},
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			return cr.or.SecretFlow().GetTrainingNodeByNameOrID(cr.ctx, r.PP["nameOrID"])
		},
	},
}
