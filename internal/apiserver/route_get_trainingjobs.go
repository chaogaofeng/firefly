package apiserver

import (
	"net/http"

	"github.com/hyperledger/firefly/internal/orchestrator"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
	"github.com/hyperledger/firefly/pkg/database"
)

var getTrainingJobs = &ffapi.Route{
	Name:            "getTrainingJobs",
	Path:            "secretflow/jobs",
	Method:          http.MethodGet,
	PathParams:      nil,
	QueryParams:     nil,
	FilterFactory:   database.TrainingJobQueryFactory,
	Description:     coremsgs.APIEndpointsGetTrainingJobs,
	JSONInputValue:  nil,
	JSONOutputValue: func() interface{} { return []*core.TrainingJob{} },
	JSONOutputCodes: []int{http.StatusOK},
	Extensions: &coreExtensions{
		EnabledIf: func(or orchestrator.Orchestrator) bool {
			return or.SecretFlow() != nil
		},
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			return r.FilterResult(cr.or.SecretFlow().GetTrainingJobs(cr.ctx, r.Filter))
		},
	},
}
