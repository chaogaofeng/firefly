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
	trainingjobColumns = []string{
		"id",
		"namespace",
		"message_id",
		"name",
		"status",
		"task",
		"pipeline",
		"created",
		"updated",
	}
	trainingjobFilterFieldMap = map[string]string{
		"message": "message_id",
	}
)

const trainingjobsTable = "trainingjobs"

func (s *SQLCommon) UpsertTrainingJob(ctx context.Context, item *core.TrainingJob, allowExisting bool) (err error) {
	ctx, tx, autoCommit, err := s.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer s.RollbackTx(ctx, tx, autoCommit)

	existing := false
	if allowExisting {
		// Do a select within the transaction to detemine if the UUID already exists
		itemRows, _, err := s.QueryTx(ctx, trainingjobsTable, tx,
			sq.Select("id").
				From(trainingjobsTable).
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
		item.Updated = fftypes.Now()

		if _, err = s.UpdateTx(ctx, trainingjobsTable, tx,
			sq.Update(trainingjobsTable).
				Set("message_id", item.Message).
				Set("name", item.Name).
				Set("status", item.Status).
				Set("task", item.TrainingTask).
				Set("pipeline", item.Pipeline).
				Set("updated", item.Updated).
				Where(sq.Eq{"id": item.ID}),
			func() {
				s.callbacks.UUIDCollectionNSEvent(database.CollectionTrainingJobs, core.ChangeEventTypeUpdated, item.Namespace, item.ID)
			},
		); err != nil {
			return err
		}
	} else {
		item.Created = fftypes.Now()
		if _, err = s.InsertTx(ctx, trainingjobsTable, tx,
			sq.Insert(trainingjobsTable).
				Columns(trainingjobColumns...).
				Values(
					item.ID,
					item.Namespace,
					item.Message,
					item.Name,
					item.Status,
					item.TrainingTask,
					item.Pipeline,
					item.Created,
					item.Updated,
				),
			func() {
				s.callbacks.UUIDCollectionNSEvent(database.CollectionTrainingJobs, core.ChangeEventTypeCreated, item.Namespace, item.ID)
			},
		); err != nil {
			return err
		}
	}

	return s.CommitTx(ctx, tx, autoCommit)
}

func (s *SQLCommon) trainingjobResult(ctx context.Context, row *sql.Rows) (*core.TrainingJob, error) {
	var item core.TrainingJob
	err := row.Scan(
		&item.ID,
		&item.Namespace,
		&item.Message,
		&item.Name,
		&item.Status,
		&item.TrainingTask,
		&item.Pipeline,
		&item.Created,
		&item.Updated,
	)
	if err != nil {
		return nil, i18n.WrapError(ctx, err, coremsgs.MsgDBReadErr, trainingjobsTable)
	}
	return &item, nil
}

func (s *SQLCommon) getTrainingJobEq(ctx context.Context, eq sq.Eq, textName string) (item *core.TrainingJob, err error) {

	rows, _, err := s.Query(ctx, trainingjobsTable,
		sq.Select(trainingjobColumns...).
			From(trainingjobsTable).
			Where(eq),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		log.L(ctx).Debugf("TrainingJob '%s' not found", textName)
		return nil, nil
	}

	item, err = s.trainingjobResult(ctx, rows)
	if err != nil {
		return nil, err
	}

	return item, nil
}

func (s *SQLCommon) GetTrainingJobByID(ctx context.Context, namespace string, id *fftypes.UUID) (message *core.TrainingJob, err error) {
	return s.getTrainingJobEq(ctx, sq.Eq{"id": id, "namespace": namespace}, id.String())
}

func (s *SQLCommon) GetTrainingJobByName(ctx context.Context, ns, name string) (message *core.TrainingJob, err error) {
	return s.getTrainingJobEq(ctx, sq.Eq{"name": name, "namespace": ns}, fmt.Sprintf("%s:%s", ns, name))
}

func (s *SQLCommon) GetTrainingJobs(ctx context.Context, namespace string, filter ffapi.Filter) (message []*core.TrainingJob, res *ffapi.FilterResult, err error) {

	query, fop, fi, err := s.FilterSelect(
		ctx, "", sq.Select(trainingjobColumns...).From(trainingjobsTable),
		filter, trainingjobFilterFieldMap, []interface{}{"sequence"}, sq.Eq{"namespace": namespace})
	if err != nil {
		return nil, nil, err
	}

	rows, tx, err := s.Query(ctx, trainingjobsTable, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	items := []*core.TrainingJob{}
	for rows.Next() {
		item, err := s.trainingjobResult(ctx, rows)
		if err != nil {
			return nil, nil, err
		}
		items = append(items, item)
	}

	return items, s.QueryRes(ctx, trainingjobsTable, tx, fop, fi), err

}
