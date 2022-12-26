package apiserver

import (
	"github.com/hyperledger/firefly/pkg/database"
	"net/http"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
)

var getTasks = &ffapi.Route{
	Name:            "ListTasks",
	Path:            "dai/ListTasks",
	Method:          http.MethodGet,
	PathParams:      nil,
	QueryParams:     nil,
	Description:     coremsgs.APIEndpointsGetTasks,
	JSONInputValue:  nil,
	JSONOutputValue: func() interface{} { return []*core.Task{} },
	JSONOutputCodes: []int{http.StatusOK},
	Extensions: &coreExtensions{
		FilterFactory: database.TaskQueryFactory,
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			return filterResult(cr.or.Dai().GetTasks(cr.ctx, cr.filter))
		},
	},
}
