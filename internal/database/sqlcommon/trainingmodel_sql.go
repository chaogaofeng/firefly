package sqlcommon

import (
	"context"
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
	"github.com/hyperledger/firefly/pkg/database"
)

var (
	trainingmodelColumns = []string{
		"id",
		"namespace",
		"message_id",
		"flowId",
		"flowName",
		"flowParties",
		"flowItems",
	}
	trainingmodelFilterFieldMap = map[string]string{
		"message": "message_id",
	}
)

const trainingmodelsTable = "trainingmodels"

func (s *SQLCommon) UpsertTrainingModel(ctx context.Context, item *core.TrainingModel, allowExisting bool) (err error) {
	ctx, tx, autoCommit, err := s.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer s.RollbackTx(ctx, tx, autoCommit)

	existing := false
	if allowExisting {
		// Do a select within the transaction to detemine if the UUID already exists
		itemRows, _, err := s.QueryTx(ctx, trainingmodelsTable, tx,
			sq.Select("id").
				From(trainingmodelsTable).
				Where(sq.Eq{
					"namespace": item.Namespace,
					"id":        item.ID,
				}),
		)
		if err != nil {
			return err
		}
		existing = itemRows.Next()
		itemRows.Close()
	}

	if existing {

		if _, err = s.UpdateTx(ctx, trainingmodelsTable, tx,
			sq.Update(trainingmodelsTable).
				Set("message_id", item.Message).
				Set("flowId", item.FlowID).
				Set("flowName", item.FlowName).
				Set("flowParties", item.FlowParties).
				Set("flowItems", item.FlowItems).
				Where(sq.Eq{"id": item.ID}),
			func() {
				s.callbacks.UUIDCollectionNSEvent(database.CollectionTrainingModels, core.ChangeEventTypeUpdated, item.Namespace, item.ID)
			},
		); err != nil {
			return err
		}
	} else {
		if _, err = s.InsertTx(ctx, trainingmodelsTable, tx,
			sq.Insert(trainingmodelsTable).
				Columns(trainingmodelColumns...).
				Values(
					item.ID,
					item.Namespace,
					item.Message,
					item.FlowID,
					item.FlowName,
					item.FlowParties,
					item.FlowItems,
				),
			func() {
				s.callbacks.UUIDCollectionNSEvent(database.CollectionTrainingModels, core.ChangeEventTypeCreated, item.Namespace, item.ID)
			},
		); err != nil {
			return err
		}
	}

	return s.CommitTx(ctx, tx, autoCommit)
}

func (s *SQLCommon) trainingmodelResult(ctx context.Context, row *sql.Rows) (*core.TrainingModel, error) {
	var item core.TrainingModel
	err := row.Scan(
		&item.ID,
		&item.Namespace,
		&item.Message,
		&item.FlowID,
		&item.FlowName,
		&item.FlowParties,
		&item.FlowItems,
	)
	if err != nil {
		return nil, i18n.WrapError(ctx, err, coremsgs.MsgDBReadErr, trainingmodelsTable)
	}
	return &item, nil
}

func (s *SQLCommon) getTrainingModelEq(ctx context.Context, eq sq.Eq, textName string) (item *core.TrainingModel, err error) {

	rows, _, err := s.Query(ctx, trainingmodelsTable,
		sq.Select(trainingmodelColumns...).
			From(trainingmodelsTable).
			Where(eq),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		log.L(ctx).Debugf("TrainingModel '%s' not found", textName)
		return nil, nil
	}

	item, err = s.trainingmodelResult(ctx, rows)
	if err != nil {
		return nil, err
	}

	return item, nil
}

func (s *SQLCommon) GetTrainingModelByID(ctx context.Context, namespace string, id *fftypes.UUID) (message *core.TrainingModel, err error) {
	return s.getTrainingModelEq(ctx, sq.Eq{"id": id, "namespace": namespace}, id.String())
}

func (s *SQLCommon) GetTrainingModelByName(ctx context.Context, ns, name string) (message *core.TrainingModel, err error) {
	return s.getTrainingModelEq(ctx, sq.Eq{"namespace": ns, "flowName": name}, fmt.Sprintf("%s:%s", ns, name))
}

func (s *SQLCommon) GetTrainingModels(ctx context.Context, namespace string, filter ffapi.Filter) (message []*core.TrainingModel, res *ffapi.FilterResult, err error) {

	query, fop, fi, err := s.FilterSelect(
		ctx, "", sq.Select(trainingmodelColumns...).From(trainingmodelsTable),
		filter, trainingmodelFilterFieldMap, []interface{}{"sequence"}, sq.Eq{"namespace": namespace})
	if err != nil {
		return nil, nil, err
	}

	rows, tx, err := s.Query(ctx, trainingmodelsTable, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	items := []*core.TrainingModel{}
	for rows.Next() {
		item, err := s.trainingmodelResult(ctx, rows)
		if err != nil {
			return nil, nil, err
		}
		items = append(items, item)
	}

	return items, s.QueryRes(ctx, trainingmodelsTable, tx, fop, fi), err

}
