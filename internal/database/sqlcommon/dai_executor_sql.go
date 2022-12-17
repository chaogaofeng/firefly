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
	executorColumns = []string{
		"id",
		"namespace",
		"name",
		"tag",
		"type",
		"address",
		"url",
		"ws",
		"mpc",
		"role",
		"status",
		"author",
		"message_id",
		"message_hash",
		"tx_type",
		"tx_id",
		"blockchain_event",
		"created",
	}
	executorFilterFieldMap = map[string]string{
		"nodeuuid":        "id",
		"nodename":        "name",
		"nodetag":         "tag",
		"nodetype":        "type",
		"nodeaddress":     "address",
		"nodeurl":         "url",
		"role":            "role",
		"nodestatus":      "status",
		"message":         "message_id",
		"messagehash":     "message_hash",
		"tx.type":         "tx_type",
		"tx.id":           "tx_id",
		"blockchainevent": "blockchain_event",
	}
)

const executorTable = "daiexecutors"

func (s *SQLCommon) UpsertExecutor(ctx context.Context, executor *core.Executor) (err error) {
	ctx, tx, autoCommit, err := s.beginOrUseTx(ctx)
	if err != nil {
		return err
	}
	defer s.rollbackTx(ctx, tx, autoCommit)

	rows, _, err := s.queryTx(ctx, executorTable, tx,
		sq.Select("id").
			From(executorTable).
			Where(sq.Eq{
				"namespace": executor.Namespace,
				"name":      executor.Name,
			}),
	)
	if err != nil {
		return err
	}
	existing := rows.Next()

	if existing {
		var id fftypes.UUID
		_ = rows.Scan(&id)
		if executor.ID != nil && *executor.ID != id {
			rows.Close()
			return database.IDMismatch
		}
		executor.ID = &id // Update on returned object
	}
	rows.Close()

	if existing {
		if _, err = s.updateTx(ctx, executorTable, tx,
			sq.Update(executorTable).
				Set("name", executor.Name).
				Set("tag", executor.Tag).
				Set("type", executor.Type).
				Set("address", executor.Address).
				Set("url", executor.URL).
				Set("ws", executor.WS).
				Set("mpc", executor.MPC).
				Set("role", executor.Role).
				Set("status", executor.Status).
				Set("author", executor.Author).
				Set("message_id", executor.Message).
				Set("message_hash", executor.MessageHash).
				Set("tx_type", executor.TX.Type).
				Set("tx_id", executor.TX.ID).
				Set("blockchain_event", executor.BlockchainEvent).
				Where(sq.Eq{"id": executor.ID}),
			func() {
				s.callbacks.UUIDCollectionNSEvent(database.CollectionDaiExecutors, core.ChangeEventTypeUpdated, executor.Namespace, executor.ID)
			},
		); err != nil {
			return err
		}
	} else {
		executor.Created = fftypes.Now()
		if _, err = s.insertTx(ctx, executorTable, tx,
			sq.Insert(executorTable).
				Columns(executorColumns...).
				Values(
					executor.ID,
					executor.Namespace,
					executor.Name,
					executor.Tag,
					executor.Type,
					executor.Address,
					executor.URL,
					executor.WS,
					executor.MPC,
					executor.Role,
					executor.Status,
					executor.Author,
					executor.Message,
					executor.MessageHash,
					executor.TX.Type,
					executor.TX.ID,
					executor.BlockchainEvent,
					executor.Created,
				),
			func() {
				s.callbacks.UUIDCollectionNSEvent(database.CollectionDaiExecutors, core.ChangeEventTypeCreated, executor.Namespace, executor.ID)
			},
		); err != nil {
			return err
		}
	}

	return s.commitTx(ctx, tx, autoCommit)
}

func (s *SQLCommon) executorResult(ctx context.Context, row *sql.Rows) (*core.Executor, error) {
	executor := core.Executor{}
	err := row.Scan(
		&executor.ID,
		&executor.Namespace,
		&executor.Name,
		&executor.Tag,
		&executor.Type,
		&executor.Address,
		&executor.URL,
		&executor.WS,
		&executor.MPC,
		&executor.Role,
		&executor.Status,
		&executor.Author,
		&executor.Message,
		&executor.MessageHash,
		&executor.TX.Type,
		&executor.TX.ID,
		&executor.BlockchainEvent,
		&executor.Created,
	)
	if err != nil {
		return nil, i18n.WrapError(ctx, err, coremsgs.MsgDBReadErr, executorTable)
	}
	return &executor, nil
}

func (s *SQLCommon) getExecutorPred(ctx context.Context, desc string, pred interface{}) (*core.Executor, error) {
	rows, _, err := s.query(ctx, executorTable,
		sq.Select(executorColumns...).
			From(executorTable).
			Where(pred),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		log.L(ctx).Debugf("Executor '%s' not found", desc)
		return nil, nil
	}

	executor, err := s.executorResult(ctx, rows)
	if err != nil {
		return nil, err
	}

	return executor, nil
}

func (s *SQLCommon) GetExecutor(ctx context.Context, namespace string, name string) (message *core.Executor, err error) {
	return s.getExecutorPred(ctx, namespace+":"+name, sq.Eq{"namespace": namespace, "name": name})
}

func (s *SQLCommon) GetExecutorByID(ctx context.Context, namespace string, id *fftypes.UUID) (message *core.Executor, err error) {
	return s.getExecutorPred(ctx, id.String(), sq.Eq{"id": id, "namespace": namespace})
}

func (s *SQLCommon) GetExecutors(ctx context.Context, namespace string, filter database.Filter) (message []*core.Executor, fr *database.FilterResult, err error) {
	query, fop, fi, err := s.filterSelect(ctx, "", sq.Select(executorColumns...).From(executorTable),
		filter, executorFilterFieldMap, []interface{}{"created"}, sq.Eq{"namespace": namespace})
	if err != nil {
		return nil, nil, err
	}

	rows, tx, err := s.query(ctx, executorTable, query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var executors []*core.Executor
	for rows.Next() {
		d, err := s.executorResult(ctx, rows)
		if err != nil {
			return nil, nil, err
		}
		executors = append(executors, d)
	}

	return executors, s.queryRes(ctx, executorTable, tx, fop, fi), err
}
