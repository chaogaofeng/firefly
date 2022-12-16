package apiserver

import (
	"net/http"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
)

var getExecutorByNameOrID = &ffapi.Route{
	Name:   "getExecutorByNameOrID",
	Path:   "dai/executors/{nameOrId}",
	Method: http.MethodGet,
	PathParams: []*ffapi.PathParam{
		{Name: "nameOrId", Description: coremsgs.APIParamsExecutorNameOrID},
	},
	QueryParams:     nil,
	Description:     coremsgs.APIEndpointsGetExecutorByNameOrID,
	JSONInputValue:  nil,
	JSONOutputValue: func() interface{} { return &core.Executor{} },
	JSONOutputCodes: []int{http.StatusOK},
	Extensions: &coreExtensions{
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			output, err = cr.or.Dai().GetExecutorByNameOrID(cr.ctx, r.PP["nameOrId"])
			return output, err
		},
	},
}
