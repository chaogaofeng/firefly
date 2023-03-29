package secretflow

import (
	"context"

	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly/pkg/core"
)

func (am *sfManager) PrepareOperation(ctx context.Context, op *core.Operation) (*core.PreparedOperation, error) {
	return nil, nil
}

func (am *sfManager) RunOperation(ctx context.Context, op *core.PreparedOperation) (outputs fftypes.JSONObject, complete bool, err error) {
	return nil, true, nil
}

func (am *sfManager) OnOperationUpdate(ctx context.Context, op *core.Operation, update *core.OperationUpdate) error {
	return nil
}
