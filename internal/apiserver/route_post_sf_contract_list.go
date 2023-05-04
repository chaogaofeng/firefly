package apiserver

import (
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly/internal/orchestrator"
	"net/http"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
)

var postSFContractList = &ffapi.Route{
	Name:            "postSFContractList",
	Path:            "contract/list",
	Method:          http.MethodPost,
	PathParams:      nil,
	Description:     "根据查询条件查询合同数据",
	JSONInputValue:  func() interface{} { return &fftypes.JSONObject{} },
	JSONOutputValue: func() interface{} { return &fftypes.JSONObject{} },
	JSONOutputCodes: []int{http.StatusOK},
	Extensions: &coreExtensions{
		EnabledIf: func(or orchestrator.Orchestrator) bool {
			return or.SecretFlow() != nil
		},
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			return cr.or.SecretFlow().PostProxy(cr.ctx, "/api/dev_centre/reconciliation/contractlist/", r.Input.(*fftypes.JSONObject))
		},
	},
}

var postSFContractListByContracts = &ffapi.Route{
	Name:            "postSFContractListByContracts",
	Path:            "contract/list/by/contracts",
	Method:          http.MethodPost,
	PathParams:      nil,
	Description:     "根据合同流水号数组查询合同数据",
	JSONInputValue:  func() interface{} { return &fftypes.JSONObject{} },
	JSONOutputValue: func() interface{} { return &fftypes.JSONObject{} },
	JSONOutputCodes: []int{http.StatusOK},
	Extensions: &coreExtensions{
		EnabledIf: func(or orchestrator.Orchestrator) bool {
			return or.SecretFlow() != nil
		},
		CoreJSONHandler: func(r *ffapi.APIRequest, cr *coreRequest) (output interface{}, err error) {
			return cr.or.SecretFlow().PostProxy(cr.ctx, "/api/dev_centre/reconciliation/contractlistbycontracts/", r.Input.(*fftypes.JSONObject))

		},
	},
}
