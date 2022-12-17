package core

import "github.com/hyperledger/firefly-common/pkg/fftypes"

type Executor struct {
	ID              *fftypes.UUID      `ffstruct:"Executor" json:"NodeUUID,omitempty"`
	Namespace       string             `ffstruct:"Executor" json:"namespace,omitempty" ffexcludeinput:"true"`
	Name            string             `ffstruct:"Executor" json:"NodeName,omitempty"`
	Tag             string             `ffstruct:"Executor" json:"NodeTag,omitempty"`
	Type            int                `ffstruct:"Executor" json:"NodeType,omitempty"`
	Address         string             `ffstruct:"Executor" json:"NodeAddress,omitempty"`
	URL             string             `ffstruct:"Executor" json:"NodeURL,omitempty"`
	WS              string             `ffstruct:"Executor" json:"NodeWebsocket,omitempty"`
	MPC             string             `ffstruct:"Executor" json:"NodeMPC,omitempty"`
	Role            int                `ffstruct:"Executor" json:"Role,omitempty"`
	Status          int                `ffstruct:"Executor" json:"NodeStatus"`
	Author          string             `ffstruct:"Executor" json:"author,omitempty" ffexcludeinput:"true"`
	Message         *fftypes.UUID      `ffstruct:"Executor" json:"message,omitempty" ffexcludeinput:"true"`
	MessageHash     *fftypes.Bytes32   `ffstruct:"Executor" json:"messageHash,omitempty" ffexcludeinput:"true"`
	Created         *fftypes.FFTime    `ffstruct:"Executor" json:"created,omitempty" ffexcludeinput:"true"`
	TX              TransactionRef     `ffstruct:"Executor" json:"tx,omitempty" ffexcludeinput:"true"`
	BlockchainEvent *fftypes.UUID      `ffstruct:"Executor" json:"blockchainEvent,omitempty" ffexcludeinput:"true"`
	Config          fftypes.JSONObject `ffstruct:"Executor" json:"config,omitempty" ffexcludeinput:"true" ffexcludeoutput:"true"` // for REST calls only (not stored)
}

type ExecutorInput struct {
	Executor `ffstruct:"ExecutorInput" json:"node,omitempty"`
	//Message        *MessageInOut  `ffstruct:"ExecutorInput" json:"message,omitempty"`
	//IdempotencyKey IdempotencyKey `ffstruct:"ExecutorInput" json:"idempotencyKey,omitempty" ffexcludeoutput:"true"`
}

type Task struct {
	ID              *fftypes.UUID            `ffstruct:"Task" json:"TaskID,omitempty"`
	Namespace       string                   `ffstruct:"Task" json:"namespace,omitempty" ffexcludeinput:"true"`
	Name            string                   `ffstruct:"Task" json:"TaskName,omitempty"`
	Desc            string                   `ffstruct:"Task" json:"Desc,omitempty"`
	Requester       string                   `ffstruct:"Task" json:"Invoker,omitempty"`
	Status          int                      `ffstruct:"Task" json:"TaskStatus"`
	Hosts           *fftypes.JSONObjectArray `ffstruct:"Task" json:"Hosts,omitempty"`
	DataSets        *fftypes.JSONObjectArray `ffstruct:"Task" json:"DataSets,omitempty"`
	Params          *fftypes.JSONObject      `ffstruct:"Task" json:"Params,omitempty"`
	Results         *fftypes.JSONObjectArray `ffstruct:"Task" json:"Results,omitempty"`
	Author          string                   `ffstruct:"Task" json:"author,omitempty" ffexcludeinput:"true"`
	Message         *fftypes.UUID            `ffstruct:"Task" json:"message,omitempty" ffexcludeinput:"true"`
	MessageHash     *fftypes.Bytes32         `ffstruct:"Task" json:"messageHash,omitempty" ffexcludeinput:"true"`
	Created         *fftypes.FFTime          `ffstruct:"Task" json:"created,omitempty" ffexcludeinput:"true"`
	TX              TransactionRef           `ffstruct:"Task" json:"tx,omitempty" ffexcludeinput:"true"`
	BlockchainEvent *fftypes.UUID            `ffstruct:"Task" json:"blockchainEvent,omitempty" ffexcludeinput:"true"`
	Config          fftypes.JSONObject       `ffstruct:"Task" json:"config,omitempty" ffexcludeinput:"true" ffexcludeoutput:"true"` // for REST calls only (not stored)
}

type TaskInput struct {
	Task `ffstruct:"TaskInput" json:"task,omitempty"`
	// Message        *MessageInOut  `ffstruct:"TaskInput" json:"message,omitempty"`
	//IdempotencyKey IdempotencyKey `ffstruct:"TaskInput" json:"idempotencyKey,omitempty" ffexcludeoutput:"true"`
}
