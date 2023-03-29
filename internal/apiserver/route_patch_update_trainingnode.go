package apiserver

import (
	"github.com/hyperledger/firefly/internal/orchestrator"
	"net/http"
	"strings"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
)

var patchUpdateTrainingNode = &ffapi.Route{
	Name:   "patchUpdateTrainingNode",
	Path:   "secretflow/nodes/{nameOrID}",
	Method: http.MethodPatch,
	PathParams: []*ffapi.PathParam{
		{Name: "nameOrID", Description: coremsgs.APIParamsTrainingNodeNameOrID},
	},
	QueryParams: []*ffapi.QueryParam{
		{Name: "confirm", Description: coremsgs.APIConfirmQueryParam, IsBool: true},
	},
	Description:     coremsgs.APIEndpointsPatchUpdateIdentity,
	JSONInputValue:  func() interface{} { return &core.TrainingNode{} },
	JSONOutputValue: func() interface{} { return &core.TrainingNode{} },
	JSONOutputCodes: []int{http.StatusAccepted, http.StatusOK},
	Extensions: &coreExtensions{
		EnabledIf: func(or orchestrator.Orchestrator) bool {
			return or.SecretFlow() != nil
		},
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			waitConfirm := strings.EqualFold(r.QP["confirm"], "true")
			r.SuccessStatus = syncRetcode(waitConfirm)
			err = cr.or.DefinitionSender().UpdateTrainingNode(cr.ctx, r.PP["nameOrID"], r.Input.(*core.TrainingNode), waitConfirm)
			return r.Input, err
		},
	},
}
