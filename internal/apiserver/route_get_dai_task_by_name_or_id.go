package apiserver

import (
	"net/http"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
)

var getTaskByNameOrID = &ffapi.Route{
	Name:   "getTaskByNameOrID",
	Path:   "dai/tasks/{nameOrId}",
	Method: http.MethodGet,
	PathParams: []*ffapi.PathParam{
		{Name: "nameOrId", Description: coremsgs.APIParamsTaskNameOrID},
	},
	QueryParams:     nil,
	Description:     coremsgs.APIEndpointsGetTaskByNameOrID,
	JSONInputValue:  nil,
	JSONOutputValue: func() interface{} { return &core.Task{} },
	JSONOutputCodes: []int{http.StatusOK},
	Extensions: &coreExtensions{
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			output, err = cr.or.Dai().GetTaskByNameOrID(cr.ctx, r.PP["nameOrId"])
			return output, err
		},
	},
}
