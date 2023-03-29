package apiserver

import (
	"github.com/hyperledger/firefly/internal/orchestrator"
	"net/http"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
	"github.com/hyperledger/firefly/pkg/database"
)

var getTrainingNodes = &ffapi.Route{
	Name:            "getTrainingNodes",
	Path:            "secretflow/nodes",
	Method:          http.MethodGet,
	PathParams:      nil,
	QueryParams:     nil,
	FilterFactory:   database.TrainingNodeQueryFactory,
	Description:     coremsgs.APIEndpointsGetTrainingNodes,
	JSONInputValue:  nil,
	JSONOutputValue: func() interface{} { return []*core.TrainingNode{} },
	JSONOutputCodes: []int{http.StatusOK},
	Extensions: &coreExtensions{
		EnabledIf: func(or orchestrator.Orchestrator) bool {
			return or.SecretFlow() != nil
		},
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			return r.FilterResult(cr.or.SecretFlow().GetTrainingNodes(cr.ctx, r.Filter))
		},
	},
}
