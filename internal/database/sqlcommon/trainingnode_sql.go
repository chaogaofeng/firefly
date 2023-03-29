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
	trainingnodeColumns = []string{
		"id",
		"namespace",
		"message_id",
		"name",
		"parent",
		"head",
		"address",
		"port",
		"range_start",
		"range_end",
		"status",
		"description",
	}
	trainingnodeFilterFieldMap = map[string]string{
		"message": "message_id",
	}
)

const trainingnodesTable = "trainingnodes"

func (s *SQLCommon) UpsertTrainingNode(ctx context.Context, item *core.TrainingNode, allowExisting bool) (err error) {
	ctx, tx, autoCommit, err := s.BeginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer s.RollbackTx(ctx, tx, autoCommit)

	existing := false
	if allowExisting {
		// Do a select within the transaction to detemine if the UUID already exists
		itemRows, _, err := s.QueryTx(ctx, trainingnodesTable, tx,
			sq.Select("id").
				From(trainingnodesTable).
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

		if _, err = s.UpdateTx(ctx, trainingnodesTable, tx,
			sq.Update(trainingnodesTable).
				Set("message_id", item.Message).
				Set("name", item.Name).
				Set("parent", item.Parent).
				Set("head", item.Head).
				Set("address", item.Address).
				Set("port", item.Port).
				Set("range_start", item.RangeStart).
				Set("range_end", item.RangeEnd).
				Set("status", item.Status).
				Set("description", item.Description).
				Where(sq.Eq{"id": item.ID}),
			func() {
				s.callbacks.UUIDCollectionNSEvent(database.CollectionTrainingNodes, core.ChangeEventTypeUpdated, item.Namespace, item.ID)
			},
		); err != nil {
			return err
		}
	} else {
		if _, err = s.InsertTx(ctx, trainingnodesTable, tx,
			sq.Insert(trainingnodesTable).
				Columns(trainingnodeColumns...).
				Values(
					item.ID,
					item.Namespace,
					item.Message,
					item.Name,
					item.Parent,
					item.Head,
					item.Address,
					item.Port,
					item.RangeStart,
					item.RangeEnd,
					item.Status,
					item.Description,
				),
			func() {
				s.callbacks.UUIDCollectionNSEvent(database.CollectionTrainingNodes, core.ChangeEventTypeCreated, item.Namespace, item.ID)
			},
		); err != nil {
			return err
		}
	}

	return s.CommitTx(ctx, tx, autoCommit)
}

func (s *SQLCommon) trainingnodeResult(ctx context.Context, row *sql.Rows) (*core.TrainingNode, error) {
	var item core.TrainingNode
	err := row.Scan(
		&item.ID,
		&item.Namespace,
		&item.Message,
		&item.Name,
		&item.Parent,
		&item.Head,
		&item.Address,
		&item.Port,
		&item.RangeStart,
		&item.RangeEnd,
		&item.Status,
		&item.Description,
	)
	if err != nil {
		return nil, i18n.WrapError(ctx, err, coremsgs.MsgDBReadErr, trainingnodesTable)
	}
	return &item, nil
}

func (s *SQLCommon) getTrainingNodeEq(ctx context.Context, eq sq.Eq, textName string) (item *core.TrainingNode, err error) {

	rows, _, err := s.Query(ctx, trainingnodesTable,
		sq.Select(trainingnodeColumns...).
			From(trainingnodesTable).
			Where(eq),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		log.L(ctx).Debugf("TrainingNode '%s' not found", textName)
		return nil, nil
	}

	item, err = s.trainingnodeResult(ctx, rows)
	if err != nil {
		return nil, err
	}

	return item, nil
}

func (s *SQLCommon) GetTrainingNodeByID(ctx context.Context, namespace string, id *fftypes.UUID) (message *core.TrainingNode, err error) {
	return s.getTrainingNodeEq(ctx, sq.Eq{"id": id, "namespace": namespace}, id.String())
}

func (s *SQLCommon) GetTrainingNodeByName(ctx context.Context, ns, name string) (message *core.TrainingNode, err error) {
	return s.getTrainingNodeEq(ctx, sq.Eq{"namespace": ns, "name": name}, fmt.Sprintf("%s:%s", ns, name))
}

func (s *SQLCommon) GetTrainingNodes(ctx context.Context, namespace string, filter ffapi.Filter) (message []*core.TrainingNode, res *ffapi.FilterResult, err error) {

	query, fop, fi, err := s.FilterSelect(
		ctx, "", sq.Select(trainingnodeColumns...).From(trainingnodesTable),
		filter, trainingnodeFilterFieldMap, []interface{}{"sequence"}, sq.Eq{"namespace": namespace})
	if err != nil {
		return nil, nil, err
	}

	rows, tx, err := s.Query(ctx, trainingnodesTable, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	items := []*core.TrainingNode{}
	for rows.Next() {
		item, err := s.trainingnodeResult(ctx, rows)
		if err != nil {
			return nil, nil, err
		}
		items = append(items, item)
	}

	return items, s.QueryRes(ctx, trainingnodesTable, tx, fop, fi), err

}
