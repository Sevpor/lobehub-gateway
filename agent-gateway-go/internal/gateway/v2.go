package gateway

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const defaultHubHeartbeatTimeout = 180 * time.Second

type hubSubscription struct {
	executor bool
	lastSeq  int
	queue    []json.RawMessage
	state    string
}

type hubConnection struct {
	heartbeat *time.Timer
	mu        sync.Mutex
	server    *Server
	subs      map[string]*hubSubscription
	userID    string
	ws        *wsConn
	closed    bool
}

func (s *Server) handleV2WebSocket(w http.ResponseWriter, r *http.Request) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		writeText(w, http.StatusUpgradeRequired, "Expected WebSocket upgrade")
		return
	}

	query := r.URL.Query()
	userID, authErr := s.auth.resolve(r.Context(), "", authMessage{
		ServerURL: query.Get("serverUrl"),
		Token:     query.Get("token"),
		TokenType: query.Get("tokenType"),
		Type:      "auth",
	})
	ws, err := upgradeWebSocket(w, r)
	if err != nil {
		return
	}
	if authErr != nil {
		reason := "auth_failed"
		if errors.Is(authErr, errTokenExpired) {
			reason = "auth_expired"
		}
		_ = ws.writeClose(wsCloseAuth, reason)
		_ = ws.close()
		return
	}

	conn := &hubConnection{
		server: s,
		subs:   map[string]*hubSubscription{},
		userID: userID,
		ws:     ws,
	}
	s.v2Mu.Lock()
	s.v2Connections[conn] = struct{}{}
	s.v2Mu.Unlock()
	conn.resetHeartbeat()
	if !conn.writeJSONOrClose(map[string]any{
		"connectionId": randomID(),
		"protocol":     2,
		"type":         "ready",
		"userId":       userID,
	}) {
		return
	}
	go conn.readLoop()
}

func (c *hubConnection) readLoop() {
	defer c.close(wsCloseNormal, "")
	for {
		payload, err := c.ws.readMessage()
		if err != nil {
			return
		}
		var envelope struct {
			OperationID string `json:"operationId"`
			Type        string `json:"type"`
		}
		if json.Unmarshal(payload, &envelope) != nil {
			c.sendError("invalid_message", "Malformed JSON", "")
			continue
		}
		switch envelope.Type {
		case "heartbeat":
			c.resetHeartbeat()
			if !c.writeJSONOrClose(map[string]string{"type": "heartbeat_ack"}) {
				return
			}
		case "subscribe":
			var msg struct {
				Executor    bool   `json:"executor"`
				LastEventID string `json:"lastEventId"`
				OperationID string `json:"operationId"`
			}
			if json.Unmarshal(payload, &msg) != nil || msg.OperationID == "" {
				c.writeJSONOrClose(map[string]any{"operationId": msg.OperationID, "reason": "invalid", "type": "subscribe_failed"})
				continue
			}
			c.subscribe(msg.OperationID, msg.LastEventID, msg.Executor)
		case "unsubscribe":
			c.mu.Lock()
			delete(c.subs, envelope.OperationID)
			c.mu.Unlock()
		case "tool_result", "tool_confirmation", "user_input", "interrupt":
			c.forwardClientMessage(envelope.OperationID, payload)
		default:
			c.sendError("unknown_type", "Unknown message type: "+envelope.Type, "")
		}
	}
}

func (c *hubConnection) subscribe(operationID string, lastEventID string, executor bool) {
	c.mu.Lock()
	c.subs[operationID] = &hubSubscription{
		executor: executor,
		lastSeq:  parseEventID(lastEventID),
		state:    "replaying",
	}
	c.mu.Unlock()
	c.completeSubscribe(operationID)
}

func (c *hubConnection) completeSubscribe(operationID string) {
	op := c.server.getOrCreateOperation(operationID)
	owner, status, _ := op.hubSnapshot()
	if owner != "" && owner != c.userID {
		c.dropSubscription(operationID)
		c.writeJSONOrClose(map[string]any{"operationId": operationID, "reason": "forbidden", "type": "subscribe_failed"})
		return
	}
	if owner == "" {
		c.mu.Lock()
		if sub := c.subs[operationID]; sub != nil {
			sub.state = "pending"
			sub.queue = nil
		}
		c.mu.Unlock()
		// Close the init/subscribe race: init may have landed after the owner
		// snapshot but before this subscription was parked.
		if currentOwner, _, _ := op.hubSnapshot(); currentOwner != "" {
			c.mu.Lock()
			if sub := c.subs[operationID]; sub != nil && sub.state == "pending" {
				sub.state = "replaying"
				sub.queue = nil
			}
			c.mu.Unlock()
			c.completeSubscribe(operationID)
			return
		}
		c.writeJSONOrClose(map[string]any{"gap": false, "operationId": operationID, "pending": true, "type": "resume_complete"})
		return
	}

	c.mu.Lock()
	sub := c.subs[operationID]
	if sub == nil {
		c.mu.Unlock()
		return
	}
	since := sub.lastSeq
	c.mu.Unlock()
	events, gap, replayStatus := op.collectReplay(jsonNumberString(since))

	c.mu.Lock()
	sub = c.subs[operationID]
	if sub == nil {
		c.mu.Unlock()
		return
	}
	queued := append([]json.RawMessage(nil), sub.queue...)
	sort.SliceStable(queued, func(i, j int) bool { return messageID(queued[i]) < messageID(queued[j]) })
	var writeErr error
	for _, payload := range append(events, queued...) {
		if writeErr = c.deliverLocked(operationID, sub, payload); writeErr != nil {
			break
		}
	}
	if writeErr != nil {
		c.mu.Unlock()
		c.close(wsCloseError, "")
		return
	}
	sub.queue = nil
	sub.state = "live"
	c.mu.Unlock()
	if replayStatus != "" {
		status = replayStatus
	}
	c.writeJSONOrClose(map[string]any{"gap": gap, "operationId": operationID, "status": status, "type": "resume_complete"})
}

func (c *hubConnection) forwardClientMessage(operationID string, payload []byte) {
	if operationID == "" {
		c.sendError("invalid_message", "Missing operationId", "")
		return
	}
	c.mu.Lock()
	sub := c.subs[operationID]
	c.mu.Unlock()
	if sub == nil {
		c.sendError("not_subscribed", "Not subscribed to this operation", operationID)
		return
	}
	op := c.server.getOperation(operationID)
	if op == nil {
		c.sendError("forward_failed", "Operation unavailable", operationID)
		return
	}
	owner, _, _ := op.hubSnapshot()
	if owner != "" && owner != c.userID {
		c.dropSubscription(operationID)
		c.sendError("forbidden", "Operation belongs to another user", operationID)
		return
	}
	var envelope struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(payload, &envelope)
	switch envelope.Type {
	case "interrupt":
		// The reference gateway keeps interrupt as a compatibility no-op.
	case "tool_confirmation":
		var msg struct {
			Approved   bool   `json:"approved"`
			ToolCallID string `json:"toolCallId"`
		}
		if json.Unmarshal(payload, &msg) == nil {
			op.resolveConfirmation(msg.ToolCallID, msg.Approved)
		}
	case "user_input":
		var msg struct {
			Content   string `json:"content"`
			RequestID string `json:"requestId"`
		}
		if json.Unmarshal(payload, &msg) == nil {
			op.resolveInput(msg.RequestID, msg.Content)
		}
	case "tool_result":
		var msg toolResultMessage
		if json.Unmarshal(payload, &msg) == nil {
			go op.forwardToolResult(msg)
		}
	}
}

func (s *Server) deliverOperationEvent(op *operation, payload json.RawMessage, isToolExecute bool) {
	owner, _, _ := op.hubSnapshot()
	if owner == "" {
		return
	}
	operationID := op.recordOperationID()
	connections := s.userHubConnections(owner)
	hasExecutor := false
	if isToolExecute {
		for _, conn := range connections {
			conn.mu.Lock()
			hasExecutor = hasExecutor || (conn.subs[operationID] != nil && conn.subs[operationID].executor)
			conn.mu.Unlock()
		}
	}
	for _, conn := range connections {
		var writeErr error
		conn.mu.Lock()
		sub := conn.subs[operationID]
		if sub == nil || (isToolExecute && hasExecutor && !sub.executor) {
			conn.mu.Unlock()
			continue
		}
		if sub.state == "replaying" {
			sub.queue = append(sub.queue, append(json.RawMessage(nil), payload...))
		} else if sub.state == "live" {
			writeErr = conn.deliverLocked(operationID, sub, payload)
		}
		conn.mu.Unlock()
		if writeErr != nil {
			conn.close(wsCloseError, "")
		}
	}
}

func (s *Server) notifyLifecycle(op *operation, status SessionStatus, summary string) {
	owner, _, meta := op.hubSnapshot()
	if owner == "" {
		return
	}
	operationID := op.recordOperationID()
	message := map[string]any{
		"at":          time.Now().UnixMilli(),
		"operationId": operationID,
		"status":      status,
		"type":        "op_lifecycle",
	}
	if meta != nil {
		message["meta"] = meta
	}
	if summary != "" {
		message["summary"] = summary
	}
	for _, conn := range s.userHubConnections(owner) {
		if !conn.writeJSONOrClose(message) {
			continue
		}
		if status == StatusRunning {
			conn.mu.Lock()
			pending := conn.subs[operationID] != nil && conn.subs[operationID].state == "pending"
			if pending {
				conn.subs[operationID].state = "replaying"
				conn.subs[operationID].queue = nil
			}
			conn.mu.Unlock()
			if pending {
				go conn.completeSubscribe(operationID)
			}
		}
		if status == "gone" {
			conn.dropSubscription(operationID)
		}
	}
}

func (s *Server) userHubConnections(userID string) []*hubConnection {
	s.v2Mu.RLock()
	defer s.v2Mu.RUnlock()
	connections := make([]*hubConnection, 0)
	for conn := range s.v2Connections {
		if conn.userID == userID {
			connections = append(connections, conn)
		}
	}
	return connections
}

func (c *hubConnection) deliverLocked(operationID string, sub *hubSubscription, payload json.RawMessage) error {
	id := messageID(payload)
	if id <= sub.lastSeq {
		return nil
	}
	var message map[string]any
	if json.Unmarshal(payload, &message) != nil {
		return nil
	}
	message["operationId"] = operationID
	if event, ok := message["event"].(map[string]any); ok && event["operationId"] == operationID {
		delete(event, "operationId")
	}
	if err := c.writeJSON(message); err != nil {
		return err
	}
	sub.lastSeq = id
	return nil
}

func (c *hubConnection) dropSubscription(operationID string) {
	c.mu.Lock()
	delete(c.subs, operationID)
	c.mu.Unlock()
}

func (c *hubConnection) sendError(code string, message string, operationID string) {
	payload := map[string]any{"code": code, "message": message, "type": "error"}
	if operationID != "" {
		payload["operationId"] = operationID
	}
	c.writeJSONOrClose(payload)
}

func (c *hubConnection) writeJSONOrClose(value any) bool {
	if err := c.writeJSON(value); err != nil {
		c.close(wsCloseError, "")
		return false
	}
	return true
}

func (c *hubConnection) writeJSON(value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.ws.writeJSON(payload)
}

func (c *hubConnection) resetHeartbeat() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.heartbeat == nil {
		c.heartbeat = time.AfterFunc(defaultHubHeartbeatTimeout, func() {
			c.close(wsCloseNormal, "Heartbeat timeout")
		})
		return
	}
	c.heartbeat.Reset(defaultHubHeartbeatTimeout)
}

func (c *hubConnection) close(code int, reason string) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	if c.heartbeat != nil {
		c.heartbeat.Stop()
	}
	c.subs = map[string]*hubSubscription{}
	c.mu.Unlock()
	if reason != "" {
		_ = c.ws.writeClose(code, reason)
	}
	_ = c.ws.close()
	c.server.v2Mu.Lock()
	delete(c.server.v2Connections, c)
	c.server.v2Mu.Unlock()
}

func (s *Server) closeOperationHubSubscribers(operationID string) {
	s.v2Mu.RLock()
	connections := make([]*hubConnection, 0, len(s.v2Connections))
	for conn := range s.v2Connections {
		connections = append(connections, conn)
	}
	s.v2Mu.RUnlock()
	for _, conn := range connections {
		conn.mu.Lock()
		_, subscribed := conn.subs[operationID]
		conn.mu.Unlock()
		if subscribed {
			// A hard close avoids blocking again on the same backpressured socket.
			conn.close(wsCloseError, "")
		}
	}
}

func (o *operation) recordOperationID() string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.record.OperationID
}

func messageID(payload json.RawMessage) int {
	var message struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(payload, &message)
	return parseEventID(message.ID)
}
