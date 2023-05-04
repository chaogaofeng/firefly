package secretflow

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/hyperledger/firefly-common/pkg/log"

	"github.com/hyperledger/firefly-common/pkg/ffapi"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly/internal/broadcast"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/internal/definitions"
	"github.com/hyperledger/firefly/internal/identity"
	"github.com/hyperledger/firefly/internal/metrics"
	"github.com/hyperledger/firefly/internal/operations"
	"github.com/hyperledger/firefly/internal/privatemessaging"
	"github.com/hyperledger/firefly/internal/syncasync"
	"github.com/hyperledger/firefly/internal/txcommon"
	"github.com/hyperledger/firefly/pkg/core"
	"github.com/hyperledger/firefly/pkg/database"
	sf "github.com/hyperledger/firefly/pkg/secretflow"
)

type Manager interface {
	core.Named

	GetTrainingNodeByNameOrID(ctx context.Context, nameOrId string) (*core.TrainingNode, error)
	GetTrainingNodes(ctx context.Context, filter ffapi.AndFilter) ([]*core.TrainingNode, *ffapi.FilterResult, error)
	GetTrainingModelByNameOrID(ctx context.Context, nameOrId string) (*core.TrainingModel, error)
	GetTrainingModels(ctx context.Context, filter ffapi.AndFilter) ([]*core.TrainingModel, *ffapi.FilterResult, error)
	GetTrainingTaskByNameOrID(ctx context.Context, nameOrId string) (*core.TrainingTask, error)
	GetTrainingTasks(ctx context.Context, filter ffapi.AndFilter) ([]*core.TrainingTask, *ffapi.FilterResult, error)
	GetTrainingJobByNameOrID(ctx context.Context, nameOrId string) (*core.TrainingJob, error)
	GetTrainingJobs(ctx context.Context, filter ffapi.AndFilter) ([]*core.TrainingJob, *ffapi.FilterResult, error)

	PostProxy(ctx context.Context, path string, body *fftypes.JSONObject) (*fftypes.JSONObject, error)

	Start() error
	TrainingJob(ctx context.Context, msg *core.Message, data core.DataArray, tx *fftypes.UUID) error
	TrainingJobUpdated(ctx context.Context, plugin sf.Plugin, flowId string, compId string, status int, log string) error

	// From operations.OperationHandler
	PrepareOperation(ctx context.Context, op *core.Operation) (*core.PreparedOperation, error)
	RunOperation(ctx context.Context, op *core.PreparedOperation) (outputs fftypes.JSONObject, complete bool, err error)
}

type sfManager struct {
	ctx              context.Context
	namespace        string
	database         database.Plugin
	txHelper         txcommon.Helper
	identity         identity.Manager
	syncasync        syncasync.Bridge
	broadcast        broadcast.Manager        // optional
	messaging        privatemessaging.Manager // optional
	secretflows      map[string]sf.Plugin
	metrics          metrics.Manager
	operations       operations.Manager
	defsender        definitions.Sender
	keyNormalization int
}

func NewManager(ctx context.Context, ns, keyNormalization string, di database.Plugin, ti map[string]sf.Plugin, ds definitions.Sender, im identity.Manager, sa syncasync.Bridge, bm broadcast.Manager, pm privatemessaging.Manager, mm metrics.Manager, om operations.Manager, txHelper txcommon.Helper) (Manager, error) {
	if di == nil || im == nil || sa == nil || ti == nil || ds == nil || mm == nil || om == nil {
		return nil, i18n.NewError(ctx, coremsgs.MsgInitializationNilDepError, "SecretFlowManager")
	}
	am := &sfManager{
		ctx:              ctx,
		namespace:        ns,
		database:         di,
		txHelper:         txHelper,
		defsender:        ds,
		identity:         im,
		syncasync:        sa,
		broadcast:        bm,
		messaging:        pm,
		secretflows:      ti,
		keyNormalization: identity.ParseKeyNormalizationConfig(keyNormalization),
		metrics:          mm,
		operations:       om,
	}

	om.RegisterHandler(ctx, am, []core.OpType{})

	return am, nil
}

func (am *sfManager) Name() string {
	return "SecretFlowManager"
}

func (am *sfManager) Start() error {
	go func() {
		timer := time.NewTicker(time.Minute)
		for {
			select {
			case <-timer.C:
				ctx := context.Background()
				fb := database.TrainingJobQueryFactory.NewFilter(ctx)
				filter := fb.And(fb.Gt("created", time.Now().Add(-10*time.Minute).UTC()), fb.Eq("status", core.JobStatusPending))
				jobs, _, err := am.database.GetTrainingJobs(ctx, am.namespace, filter)
				if err != nil {
					log.L(ctx).Errorf("submit training job %v", err)
				}
				for _, job := range jobs {
					log.L(ctx).Infof("submit training job(%s)...", job.ID)
					am.trainingJob(ctx, job)
				}
			}
		}
	}()

	return nil
}

func (am *sfManager) selectPlugin(ctx context.Context, name string) (sf.Plugin, error) {
	for pluginName, plugin := range am.secretflows {
		if pluginName == name {
			return plugin, nil
		}
	}
	return nil, i18n.NewError(ctx, coremsgs.MsgUnknownSecretFlowsPlugin, name)
}

func (am *sfManager) GetPlugins(ctx context.Context) []string {
	var plugins []string
	for name := range am.secretflows {
		plugins = append(plugins, name)
	}
	return plugins
}

func (am *sfManager) getDefaultPlugin(ctx context.Context) (string, error) {
	plugins := am.GetPlugins(ctx)
	if len(plugins) != 1 {
		return "", i18n.NewError(ctx, coremsgs.MsgFieldNotSpecified, "connector")
	}
	return plugins[0], nil
}

func (am *sfManager) GetTrainingNodeByNameOrID(ctx context.Context, nameOrID string) (*core.TrainingNode, error) {
	uuid, err := fftypes.ParseUUID(ctx, nameOrID)
	if err != nil {
		return am.database.GetTrainingNodeByName(ctx, am.namespace, nameOrID)
	}
	return am.database.GetTrainingNodeByID(ctx, am.namespace, uuid)
}

func (am *sfManager) GetTrainingNodes(ctx context.Context, filter ffapi.AndFilter) ([]*core.TrainingNode, *ffapi.FilterResult, error) {
	return am.database.GetTrainingNodes(ctx, am.namespace, filter)
}

func (am *sfManager) GetTrainingModelByNameOrID(ctx context.Context, nameOrID string) (*core.TrainingModel, error) {
	uuid, err := fftypes.ParseUUID(ctx, nameOrID)
	if err != nil {
		return am.database.GetTrainingModelByName(ctx, am.namespace, nameOrID)
	}
	return am.database.GetTrainingModelByID(ctx, am.namespace, uuid)
}

func (am *sfManager) GetTrainingModels(ctx context.Context, filter ffapi.AndFilter) ([]*core.TrainingModel, *ffapi.FilterResult, error) {
	return am.database.GetTrainingModels(ctx, am.namespace, filter)
}

func (am *sfManager) GetTrainingTaskByNameOrID(ctx context.Context, nameOrID string) (*core.TrainingTask, error) {
	uuid, err := fftypes.ParseUUID(ctx, nameOrID)
	if err != nil {
		return am.database.GetTrainingTaskByName(ctx, am.namespace, nameOrID)
	}
	return am.database.GetTrainingTaskByID(ctx, am.namespace, uuid)
}

func (am *sfManager) GetTrainingTasks(ctx context.Context, filter ffapi.AndFilter) ([]*core.TrainingTask, *ffapi.FilterResult, error) {
	return am.database.GetTrainingTasks(ctx, am.namespace, filter)
}

func (am *sfManager) GetTrainingJobByNameOrID(ctx context.Context, nameOrID string) (*core.TrainingJob, error) {
	uuid, err := fftypes.ParseUUID(ctx, nameOrID)
	if err != nil {
		return am.database.GetTrainingJobByName(ctx, am.namespace, nameOrID)
	}
	return am.database.GetTrainingJobByID(ctx, am.namespace, uuid)
}

func (am *sfManager) GetTrainingJobs(ctx context.Context, filter ffapi.AndFilter) ([]*core.TrainingJob, *ffapi.FilterResult, error) {
	return am.database.GetTrainingJobs(ctx, am.namespace, filter)
}

func (am *sfManager) getSystemBroadcastPayload(ctx context.Context, msg *core.Message, data core.DataArray, res core.Definition) (valid bool) {
	l := log.L(ctx)
	if len(data) != 1 {
		l.Warnf("Unable to process system definition %s - expecting 1 attachment, found %d", msg.Header.ID, len(data))
		return false
	}
	err := json.Unmarshal(data[0].Value.Bytes(), &res)
	if err != nil {
		l.Warnf("Unable to process system definition %s - unmarshal failed: %s", msg.Header.ID, err)
		return false
	}
	res.SetBroadcastMessage(msg.Header.ID)
	return true
}

func (am *sfManager) TrainingJob(ctx context.Context, msg *core.Message, data core.DataArray, tx *fftypes.UUID) (err error) {
	l := log.L(ctx)
	var job core.TrainingJob
	valid := am.getSystemBroadcastPayload(ctx, msg, data, &job)
	if !valid {
		l.Errorf("TrainingJob: message(%s) bad payload", msg.Header.ID)
		return i18n.NewError(ctx, coremsgs.MsgDefRejectedBadPayload, "training job", msg.Header.ID)
	}

	return am.trainingJob(ctx, &job)
}

func (am *sfManager) trainingJob(ctx context.Context, job *core.TrainingJob) (err error) {
	l := log.L(ctx)

	connector, err := am.getDefaultPlugin(ctx)
	if err != nil {
		l.Errorf("trainingJob %s: get default plugin %v", job.ID, err)
		return err
	}
	plugin, err := am.selectPlugin(ctx, connector)
	if err != nil {
		l.Errorf("trainingJob %s : select plugin(%s) %v", job.ID, connector, err)
		return err
	}

	task, err := am.database.GetTrainingTaskByID(ctx, am.namespace, fftypes.MustParseUUID(job.TrainingTask))
	if err != nil {
		l.Errorf("trainingJob %s : get task(%s) %v", job.ID, job.TrainingTask, err)
		return err
	}

	nodeOwningOrg, err := am.identity.GetMultipartyRootOrg(ctx)
	if err != nil {
		l.Errorf("trainingJob %s: get org %v", job.ID, err)
		return err
	}

	var self *core.TrainingNode
	var selfPort uint
	var deviceList fftypes.JSONObjectArray
	for i, party := range *task.Parties {
		node, err := am.GetTrainingNodeByNameOrID(ctx, party)
		if err != nil {
			l.Errorf("trainingJob %s: get node(%s) %v", job.ID, party, err)
			return err
		}
		port := availablePort(node.Address, node.RangeStart, node.RangeEnd)
		if port == 0 {
			l.Errorf("trainingJob %s: no available port in node(%s)", job.ID, party)
			return nil
		}
		deviceList = append(deviceList, map[string]interface{}{
			"node_alias": node.Name,
			"ip":         node.Address,
			"port":       port,
		})
		if i == 0 && node.Parent == nodeOwningOrg.DID {
			self = node
			selfPort = port
		}
	}
	if self == nil {
		return nil
	}

	var body fftypes.JSONObject = make(map[string]interface{})
	body["initdata"] = map[string]interface{}{
		"deviceList": deviceList,
		"address":    self.Head,
		"selfDevice": map[string]interface{}{
			"node_alias": self.Name,
			"ip":         self.Address,
			"port":       selfPort,
		},
		"projectId": 1,
		"flowId":    job.ID.String(),
	}
	body["pipeline"] = job.Pipeline

	if err := plugin.RunPipeline(ctx, &body); err != nil {
		l.Errorf("trainingJob %s: run pipeline %s %v", job.ID, body.String(), err)
		job.Status = core.JobStatusFailed
		return am.defsender.DefineTrainingJobUpdate(ctx, job, false)
	}
	l.Debugf("trainingJob %s: %s submitted", job.ID, body.String())

	job.Status = core.JobStatusSucceeded
	return am.defsender.DefineTrainingJobUpdate(ctx, job, false)
}

func (am *sfManager) TrainingJobUpdated(ctx context.Context, plugin sf.Plugin, flowId string, compId string, status int, log string) error {
	uuid, err := fftypes.ParseUUID(ctx, flowId)
	if err != nil {
		return err
	}
	job, err := am.database.GetTrainingJobByID(ctx, am.namespace, uuid)
	if err != nil {
		return err
	}
	if job == nil {
		return fmt.Errorf("job not found")
	}
	job.Status = core.JobStatusSucceeded

	items := job.Pipeline.GetObjectArray("flowItems")
	for _, comp := range items {
		if comp.GetString("id") == compId {
			comp["processed"] = true
			comp["status"] = status
			comp["log"] = log
		}
	}
	return am.defsender.DefineTrainingJobUpdate(ctx, job, true)
}

func availablePort(hostname string, start, end uint) uint {
	for port := start; port <= end; port++ {
		if !scanPort("tcp", hostname, port) {
			return port
		}
	}
	return 0
}

func scanPort(protocol string, hostname string, port uint) bool {
	p := strconv.Itoa(int(port))
	addr := net.JoinHostPort(hostname, p)
	conn, err := net.DialTimeout(protocol, addr, 3*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()
	return true
}

func (am *sfManager) PostProxy(ctx context.Context, path string, body *fftypes.JSONObject) (*fftypes.JSONObject, error) {
	l := log.L(ctx)

	connector, err := am.getDefaultPlugin(ctx)
	if err != nil {
		l.Errorf("PostProxy(%s): get default plugin %v", path, err)
		return nil, err
	}
	plugin, err := am.selectPlugin(ctx, connector)
	if err != nil {
		l.Errorf("PostProxy(%s): select plugin(%s) %v",path, connector, err)
		return nil, err
	}
	t := time.Now()
	resp, err := plugin.RunPostProxy(ctx, path, body)
	l.Infof("PostProxy(%s): elapsed %s ", path, time.Since(t))
	return resp, err
}