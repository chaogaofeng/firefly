package goldnet

import (
	"context"
	"encoding/json"
	"github.com/go-resty/resty/v2"
	"github.com/hyperledger/firefly-common/pkg/config"
	"github.com/hyperledger/firefly-common/pkg/ffresty"
	"github.com/hyperledger/firefly-common/pkg/fftypes"
	"github.com/hyperledger/firefly-common/pkg/i18n"
	"github.com/hyperledger/firefly-common/pkg/log"
	"github.com/hyperledger/firefly-common/pkg/wsclient"
	"github.com/hyperledger/firefly/internal/coremsgs"
	"github.com/hyperledger/firefly/pkg/core"
	sf "github.com/hyperledger/firefly/pkg/secretflow"
)

type SecretFlow struct {
	ctx            context.Context
	cancelCtx      context.CancelFunc
	configuredName string
	capabilities   *sf.Capabilities
	callbacks      callbacks
	client         *resty.Client
	wsconn         wsclient.WSClient
}

type callbacks struct {
	plugin     sf.Plugin
	handlers   map[string]sf.Callbacks
	opHandlers map[string]core.OperationCallbacks
}

func (cb *callbacks) TrainingJobUpdated(ctx context.Context, flowId string, compId string, status int, log string) error {
	for _, handler := range cb.handlers {
		if err := handler.TrainingJobUpdated(ctx, cb.plugin, flowId, compId, status, log); err != nil {
			return err
		}
	}
	return nil
}

func (gsf *SecretFlow) Name() string {
	return "goldnet"
}

func (gsf *SecretFlow) Init(ctx context.Context, cancelCtx context.CancelFunc, name string, config config.Section) (err error) {
	gsf.ctx = log.WithLogField(ctx, "proto", "goldnet")
	gsf.cancelCtx = cancelCtx
	gsf.configuredName = name
	gsf.capabilities = &sf.Capabilities{}
	gsf.callbacks = callbacks{
		plugin:     gsf,
		handlers:   make(map[string]sf.Callbacks),
		opHandlers: make(map[string]core.OperationCallbacks),
	}

	if config.GetString(ffresty.HTTPConfigURL) == "" {
		return i18n.NewError(ctx, coremsgs.MsgMissingPluginConfig, "url", "secretflow.glodnet")
	}
	gsf.client = ffresty.New(gsf.ctx, config)

	wsConfig := wsclient.GenerateConfig(config)
	if wsConfig.WSKeyPath == "" {
		wsConfig.WSKeyPath = "/v1/ws/telecom/roll_call/000000/"
	}

	gsf.wsconn, err = wsclient.New(ctx, wsConfig, nil, nil)
	if err != nil {
		return err
	}

	go gsf.eventLoop()

	return nil
}

func (gsf *SecretFlow) SetHandler(namespace string, handler sf.Callbacks) {
	gsf.callbacks.handlers[namespace] = handler
}

func (gsf *SecretFlow) SetOperationHandler(namespace string, handler core.OperationCallbacks) {
	gsf.callbacks.opHandlers[namespace] = handler
}

func (gsf *SecretFlow) Start() error {
	return gsf.wsconn.Connect()
}

func (gsf *SecretFlow) Capabilities() *sf.Capabilities {
	return gsf.capabilities
}

func (gsf *SecretFlow) eventLoop() {
	defer gsf.wsconn.Close()
	l := log.L(gsf.ctx).WithField("role", "event-loop")
	ctx := log.WithLogger(gsf.ctx, l)
	for {
		select {
		case <-ctx.Done():
			l.Debugf("Event loop exiting (context cancelled)")
			return
		case msgBytes, ok := <-gsf.wsconn.Receive():
			if !ok {
				l.Debugf("Event loop exiting (receive channel closed). Terminating server!")
				gsf.cancelCtx()
				return
			}
			if err := gsf.handleMessage(ctx, msgBytes); err != nil {
				l.Errorf("Event loop exiting (%s). Terminating server!", err)
				gsf.cancelCtx()
				return
			}
		}
	}
}

type wsEvent struct {
	Event  string `json:"contentType"`
	FlowID string `json:"flowId"`
	CompID string `json:"compId"`
	Status int    `json:"status"`
	Log    string `json:"logInfo"`
}

func (gsf *SecretFlow) handleMessage(ctx context.Context, msgBytes []byte) (err error) {
	l := log.L(ctx)

	var msg wsEvent
	if err = json.Unmarshal(msgBytes, &msg); err != nil {
		l.Errorf("Message cannot be parsed as JSON: %s\n%s", err, string(msgBytes))
		return nil // Swallow this and move on
	}
	if msg.Event != "BUSINESS" {
		return nil
	}

	l.Debugf("Received %s event flow %s comp %s", msg.Event, msg.FlowID, msg.CompID)
	if err := gsf.callbacks.TrainingJobUpdated(ctx, msg.FlowID, msg.CompID, msg.Status, msg.Log); err != nil {
		l.Errorf("TrainingJobUpdated: %s\n%s", err, string(msgBytes))
	}
	return nil
}

type respError struct {
	Error   string `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
}

func (gsf *SecretFlow) RunPipeline(ctx context.Context, body *fftypes.JSONObject) error {
	var errRes respError
	res, err := gsf.client.R().SetContext(ctx).
		SetBody(body).
		SetError(&errRes).
		Post("/api/dev_centre/train_flow/startPipeline/")
	if err != nil || !res.IsSuccess() {
		return wrapError(ctx, &errRes, res, err)
	}

	var obj fftypes.JSONObject
	if err := json.Unmarshal(res.Body(), &obj); err != nil {
		return i18n.WrapError(ctx, err, i18n.MsgJSONObjectParseFailed, res.Body())
	}
	return nil
}

func (gsf *SecretFlow) RunPostProxy(ctx context.Context, path string, body *fftypes.JSONObject) (*fftypes.JSONObject, error) {
	var errRes respError
	res, err := gsf.client.R().SetContext(ctx).
		SetBody(body).
		SetError(&errRes).
		Post(path)
	if err != nil || !res.IsSuccess() {
		return nil, wrapError(ctx, &errRes, res, err)
	}

	var obj fftypes.JSONObject
	if err := json.Unmarshal(res.Body(), &obj); err != nil {
		return nil, i18n.WrapError(ctx, err, i18n.MsgJSONObjectParseFailed, res.Body())
	}
	return &obj, nil
}

func wrapError(ctx context.Context, errRes *respError, res *resty.Response, err error) error {
	if errRes != nil && errRes.Message != "" {
		if errRes.Error != "" {
			return i18n.WrapError(ctx, err, coremsgs.MsgSFRESTErr, errRes.Error+": "+errRes.Message)
		}
		return i18n.WrapError(ctx, err, coremsgs.MsgSFRESTErr, errRes.Message)
	}
	return ffresty.WrapRestErr(ctx, res, err, coremsgs.MsgSFRESTErr)
}