package apiserver

import (
	"github.com/hyperledger/firefly/internal/orchestrator"
	"net/http"
	"strings"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
)

var postNewTrainingNode = &ffapi.Route{
	Name:       "postNewTrainingNode",
	Path:       "secretflow/nodes",
	Method:     http.MethodPost,
	PathParams: nil,
	QueryParams: []*ffapi.QueryParam{
		{Name: "confirm", Description: coremsgs.APIConfirmQueryParam, IsBool: true, Example: "true"},
	},
	Description:     coremsgs.APIEndpointsPostNewTrainingNode,
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
			err = cr.or.DefinitionSender().DefineTrainingNode(cr.ctx, r.Input.(*core.TrainingNode), waitConfirm)
			return r.Input, err
		},
	},
}
