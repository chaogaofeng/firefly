package apiserver

import (
	"github.com/hyperledger/firefly/pkg/database"
	"net/http"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
)

var getExecutors = &ffapi.Route{
	Name:            "getExecutors",
	Path:            "dai/executors",
	Method:          http.MethodGet,
	PathParams:      nil,
	QueryParams:     nil,
	Description:     coremsgs.APIEndpointsGetExecutors,
	JSONInputValue:  nil,
	JSONOutputValue: func() interface{} { return []*core.Executor{} },
	JSONOutputCodes: []int{http.StatusOK},
	Extensions: &coreExtensions{
		FilterFactory: database.ExecutorQueryFactory,
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			return filterResult(cr.or.Dai().GetExecutors(cr.ctx, cr.filter))
		},
	},
}
