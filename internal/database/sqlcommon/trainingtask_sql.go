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
	trainingtaskColumns = []string{
		"id",
		"namespace",
		"message_id",
		"name",
		"parties",
		"model",
	}
	trainingtaskFilterFieldMap = map[string]string{
		"message": "message_id",
	}
)

const trainingtasksTable = "trainingtasks"

func (s *SQLCommon) UpsertTrainingTask(ctx context.Context, item *core.TrainingTask, allowExisting bool) (err error) {
	ctx, tx, autoCommit, err := s.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer s.RollbackTx(ctx, tx, autoCommit)

	existing := false
	if allowExisting {
		// Do a select within the transaction to detemine if the UUID already exists
		itemRows, _, err := s.QueryTx(ctx, trainingtasksTable, tx,
			sq.Select("id").
				From(trainingtasksTable).
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

		if _, err = s.UpdateTx(ctx, trainingtasksTable, tx,
			sq.Update(trainingtasksTable).
				Set("message_id", item.Message).
				Set("name", item.Name).
				Set("parties", item.Parties).
				Set("model", item.TrainingModel).
				Where(sq.Eq{"id": item.ID}),
			func() {
				s.callbacks.UUIDCollectionNSEvent(database.CollectionTrainingTasks, core.ChangeEventTypeUpdated, item.Namespace, item.ID)
			},
		); err != nil {
			return err
		}
	} else {
		if _, err = s.InsertTx(ctx, trainingtasksTable, tx,
			sq.Insert(trainingtasksTable).
				Columns(trainingtaskColumns...).
				Values(
					item.ID,
					item.Namespace,
					item.Message,
					item.Name,
					item.Parties,
					item.TrainingModel,
				),
			func() {
				s.callbacks.UUIDCollectionNSEvent(database.CollectionTrainingTasks, core.ChangeEventTypeCreated, item.Namespace, item.ID)
			},
		); err != nil {
			return err
		}
	}

	return s.CommitTx(ctx, tx, autoCommit)
}

func (s *SQLCommon) trainingtaskResult(ctx context.Context, row *sql.Rows) (*core.TrainingTask, error) {
	var item core.TrainingTask
	err := row.Scan(
		&item.ID,
		&item.Namespace,
		&item.Message,
		&item.Name,
		&item.Parties,
		&item.TrainingModel,
	)
	if err != nil {
		return nil, i18n.WrapError(ctx, err, coremsgs.MsgDBReadErr, trainingtasksTable)
	}
	return &item, nil
}

func (s *SQLCommon) getTrainingTaskEq(ctx context.Context, eq sq.Eq, textName string) (item *core.TrainingTask, err error) {

	rows, _, err := s.Query(ctx, trainingtasksTable,
		sq.Select(trainingtaskColumns...).
			From(trainingtasksTable).
			Where(eq),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		log.L(ctx).Debugf("TrainingTask '%s' not found", textName)
		return nil, nil
	}

	item, err = s.trainingtaskResult(ctx, rows)
	if err != nil {
		return nil, err
	}

	return item, nil
}

func (s *SQLCommon) GetTrainingTaskByID(ctx context.Context, namespace string, id *fftypes.UUID) (message *core.TrainingTask, err error) {
	return s.getTrainingTaskEq(ctx, sq.Eq{"id": id, "namespace": namespace}, id.String())
}

func (s *SQLCommon) GetTrainingTaskByName(ctx context.Context, ns, name string) (message *core.TrainingTask, err error) {
	return s.getTrainingTaskEq(ctx, sq.Eq{"namespace": ns, "name": name}, fmt.Sprintf("%s:%s", ns, name))
}

func (s *SQLCommon) GetTrainingTasks(ctx context.Context, namespace string, filter ffapi.Filter) (message []*core.TrainingTask, res *ffapi.FilterResult, err error) {

	query, fop, fi, err := s.FilterSelect(
		ctx, "", sq.Select(trainingtaskColumns...).From(trainingtasksTable),
		filter, trainingtaskFilterFieldMap, []interface{}{"sequence"}, sq.Eq{"namespace": namespace})
	if err != nil {
		return nil, nil, err
	}

	rows, tx, err := s.Query(ctx, trainingtasksTable, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	items := []*core.TrainingTask{}
	for rows.Next() {
		item, err := s.trainingtaskResult(ctx, rows)
		if err != nil {
			return nil, nil, err
		}
		items = append(items, item)
	}

	return items, s.QueryRes(ctx, trainingtasksTable, tx, fop, fi), err

}
