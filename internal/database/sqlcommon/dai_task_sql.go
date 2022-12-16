package sqlcommon

import (
	"context"
	"database/sql"

	sq "github.com/Masterminds/squirrel"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
	"github.com/hyperledger/firefly/pkg/database"
)

var (
	taskColumns = []string{
		"id",
		"namespace",
		"name",
		"desc",
		"requester",
		"status",
		"hosts",
		"datasets",
		"params",
		"results",
		"author",
		"message_id",
		"message_hash",
		"tx_type",
		"tx_id",
		"blockchain_event",
		"created",
	}
	taskFilterFieldMap = map[string]string{
		"taskid":          "id",
		"taskname":        "name",
		"taskstatus":      "status",
		"invoker":         "requester",
		"message":         "message_id",
		"messagehash":     "message_hash",
		"tx.type":         "tx_type",
		"tx.id":           "tx_id",
		"blockchainevent": "blockchain_event",
	}
)

const taskTable = "daitasks"

func (s *SQLCommon) UpsertTask(ctx context.Context, task *core.Task) (err error) {
	ctx, tx, autoCommit, err := s.beginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer s.rollbackTx(ctx, tx, autoCommit)

	rows, _, err := s.queryTx(ctx, taskTable, tx,
		sq.Select("id").
			From(taskTable).
			Where(sq.Eq{
				"namespace": task.Namespace,
				"name":      task.Name,
			}),
	)
	if err != nil {
		return err
	}
	existing := rows.Next()

	if existing {
		var id fftypes.UUID
		_ = rows.Scan(&id)
		if task.ID != nil && *task.ID != id {
			rows.Close()
			return database.IDMismatch
		}
		task.ID = &id // Update on returned object
	}
	rows.Close()

	if existing {
		if _, err = s.updateTx(ctx, taskTable, tx,
			sq.Update(taskTable).
				Set("name", task.Name).
				Set("desc", task.Desc).
				Set("requester", task.Requester).
				Set("status", task.Status).
				Set("hosts", task.Hosts).
				Set("datasets", task.DataSets).
				Set("params", task.Params).
				Set("results", task.Results).
				Set("author", task.Author).
				Set("message_id", task.Message).
				Set("message_hash", task.MessageHash).
				Set("tx_type", task.TX.Type).
				Set("tx_id", task.TX.ID).
				Set("blockchain_event", task.BlockchainEvent).
				Where(sq.Eq{"id": task.ID}),
			func() {
				s.callbacks.UUIDCollectionNSEvent(database.CollectionDaiExecutors, core.ChangeEventTypeUpdated, task.Namespace, task.ID)
			},
		); err != nil {
			return err
		}
	} else {
		task.Created = fftypes.Now()
		if _, err = s.insertTx(ctx, taskTable, tx,
			sq.Insert(taskTable).
				Columns(taskColumns...).
				Values(
					task.ID,
					task.Namespace,
					task.Name,
					task.Desc,
					task.Requester,
					task.Status,
					task.Hosts,
					task.DataSets,
					task.Params,
					task.Results,
					task.Author,
					task.Message,
					task.MessageHash,
					task.TX.Type,
					task.TX.ID,
					task.BlockchainEvent,
					task.Created,
				),
			func() {
				s.callbacks.UUIDCollectionNSEvent(database.CollectionDaiTasks, core.ChangeEventTypeCreated, task.Namespace, task.ID)
			},
		); err != nil {
			return err
		}
	}

	return s.commitTx(ctx, tx, autoCommit)
}

func (s *SQLCommon) taskResult(ctx context.Context, row *sql.Rows) (*core.Task, error) {
	task := core.Task{}
	err := row.Scan(
		&task.ID,
		&task.Namespace,
		&task.Name,
		&task.Desc,
		&task.Requester,
		&task.Status,
		&task.Hosts,
		&task.DataSets,
		&task.Params,
		&task.Results,
		&task.Author,
		&task.Message,
		&task.MessageHash,
		&task.TX.Type,
		&task.TX.ID,
		&task.BlockchainEvent,
		&task.Created,
	)
	if err != nil {
		return nil, i18n.WrapError(ctx, err, coremsgs.MsgDBReadErr, taskTable)
	}
	return &task, nil
}

func (s *SQLCommon) getTaskPred(ctx context.Context, desc string, pred interface{}) (*core.Task, error) {
	rows, _, err := s.query(ctx, taskTable,
		sq.Select(taskColumns...).
			From(taskTable).
			Where(pred),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		log.L(ctx).Debugf("Task '%s' not found", desc)
		return nil, nil
	}

	task, err := s.taskResult(ctx, rows)
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (s *SQLCommon) GetTask(ctx context.Context, namespace string, name string) (message *core.Task, err error) {
	return s.getTaskPred(ctx, namespace+":"+name, sq.Eq{"namespace": namespace, "name": name})
}

func (s *SQLCommon) GetTaskByID(ctx context.Context, namespace string, id *fftypes.UUID) (message *core.Task, err error) {
	return s.getTaskPred(ctx, id.String(), sq.Eq{"id": id, "namespace": namespace})
}

func (s *SQLCommon) GetTasks(ctx context.Context, namespace string, filter database.Filter) (message []*core.Task, fr *database.FilterResult, err error) {
	query, fop, fi, err := s.filterSelect(ctx, "", sq.Select(taskColumns...).From(taskTable),
		filter, taskFilterFieldMap, []interface{}{"created"}, sq.Eq{"namespace": namespace})
	if err != nil {
		return nil, nil, err
	}

	rows, tx, err := s.query(ctx, taskTable, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var tasks []*core.Task
	for rows.Next() {
		d, err := s.taskResult(ctx, rows)
		if err != nil {
			return nil, nil, err
		}
		tasks = append(tasks, d)
	}

	return tasks, s.queryRes(ctx, taskTable, tx, fop, fi), err
}
