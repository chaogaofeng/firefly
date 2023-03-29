package apiserver

import (
	"net/http"

	"github.com/hyperledger/firefly/internal/orchestrator"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
	"github.com/hyperledger/firefly/pkg/database"
)

var getTrainingTasks = &ffapi.Route{
	Name:            "getTrainingTasks",
	Path:            "secretflow/tasks",
	Method:          http.MethodGet,
	PathParams:      nil,
	QueryParams:     nil,
	FilterFactory:   database.TrainingTaskQueryFactory,
	Description:     coremsgs.APIEndpointsGetTrainingTasks,
	JSONInputValue:  nil,
	JSONOutputValue: func() interface{} { return []*core.TrainingTask{} },
	JSONOutputCodes: []int{http.StatusOK},
	Extensions: &coreExtensions{
		EnabledIf: func(or orchestrator.Orchestrator) bool {
			return or.SecretFlow() != nil
		},
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			return r.FilterResult(cr.or.SecretFlow().GetTrainingTasks(cr.ctx, r.Filter))
		},
	},
}
