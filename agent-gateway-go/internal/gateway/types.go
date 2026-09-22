package gateway

import (
	"encoding/json"
	"time"
)

type SessionStatus string

const (
	StatusRunning             SessionStatus = "running"
	StatusWaitingInput        SessionStatus = "waiting_input"
	StatusWaitingConfirmation SessionStatus = "waiting_confirmation"
	StatusCompleted           SessionStatus = "completed"
	StatusError               SessionStatus = "error"
	StatusInterrupted         SessionStatus = "interrupted"
)

type authMessage struct {
	ServerURL string `json:"serverUrl,omitempty"`
	Token     string `json:"token"`
	TokenType string `json:"tokenType,omitempty"`
	Type      string `json:"type"`
}

type clientAttachment struct {
	Authenticated bool
	ConnectedAt   int64
	IsAdmin       bool
	LastHeartbeat int64
	PendingOwner  bool
	UserID        string
}

type operationMeta struct {
	AgentID             string `json:"agentId,omitempty"`
	GroupID             string `json:"groupId,omitempty"`
	MirrorToOperationID string `json:"mirrorToOperationId,omitempty"`
	ParentOperationID   string `json:"parentOperationId,omitempty"`
	RootOperationID     string `json:"rootOperationId,omitempty"`
	Scope               string `json:"scope,omitempty"`
	TaskID              string `json:"taskId,omitempty"`
	ThreadID            string `json:"threadId,omitempty"`
	TopicID             string `json:"topicId,omitempty"`
}

type agentStreamEvent struct {
	Data        json.RawMessage `json:"data"`
	OperationID string          `json:"operationId"`
	StepIndex   int             `json:"stepIndex"`
	Timestamp   int64           `json:"timestamp"`
	Type        string          `json:"type"`
}

type toolCallInfo struct {
	APIName    string          `json:"apiName"`
	Arguments  json.RawMessage `json:"arguments"`
	Identifier string          `json:"identifier"`
	Name       string          `json:"name"`
}

type bufferedEvent struct {
	Data json.RawMessage
	ID   string
}

type operationHTTPBody struct {
	Data        json.RawMessage    `json:"data,omitempty"`
	ErrorType   string             `json:"errorType,omitempty"`
	Event       *agentStreamEvent  `json:"event,omitempty"`
	Events      []agentStreamEvent `json:"events,omitempty"`
	Model       string             `json:"model,omitempty"`
	Meta        *operationMeta     `json:"meta,omitempty"`
	OperationID string             `json:"operationId,omitempty"`
	Prompt      string             `json:"prompt,omitempty"`
	Provider    string             `json:"provider,omitempty"`
	Status      SessionStatus      `json:"status,omitempty"`
	Summary     string             `json:"summary,omitempty"`
	Timeout     int                `json:"timeout,omitempty"`
	Tool        *toolCallInfo      `json:"tool,omitempty"`
	ToolCallID  string             `json:"toolCallId,omitempty"`
	UserID      string             `json:"userId,omitempty"`
}

type operationRecord struct {
	CreatedAt   time.Time
	Meta        *operationMeta
	OperationID string
	Status      SessionStatus
	UserID      string
}

type toolResultMessage struct {
	Content          *string         `json:"content"`
	Error            json.RawMessage `json:"error,omitempty"`
	ExecutionTimeMS  *int64          `json:"executionTimeMs,omitempty"`
	State            json.RawMessage `json:"state,omitempty"`
	Success          bool            `json:"success"`
	ToolCallID       string          `json:"toolCallId"`
	Type             string          `json:"type,omitempty"`
	WorkRegistration json.RawMessage `json:"workRegistration,omitempty"`
}

type agentRuntimeEndData struct {
	ErrorType  string `json:"errorType,omitempty"`
	FinalState struct {
		Error *struct {
			ErrorType string `json:"errorType,omitempty"`
			Type      string `json:"type,omitempty"`
		} `json:"error,omitempty"`
		ModelRuntimeConfig *struct {
			CompressionModel *struct {
				Model    string `json:"model"`
				Provider string `json:"provider"`
			} `json:"compressionModel,omitempty"`
			Model    string `json:"model"`
			Provider string `json:"provider"`
		} `json:"modelRuntimeConfig,omitempty"`
		Status string `json:"status,omitempty"`
	} `json:"finalState,omitempty"`
	Reason       string `json:"reason,omitempty"`
	ReasonDetail string `json:"reasonDetail,omitempty"`
}

type finalizeAbandonedResult struct {
	Abandoned *bool `json:"abandoned,omitempty"`
	Finalized *bool `json:"finalized,omitempty"`
	Found     *bool `json:"found,omitempty"`
}
