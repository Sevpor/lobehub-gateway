package gateway

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func newV2TestServer(t *testing.T) (*Server, *httptest.Server, func(string, time.Time, time.Time) string) {
	t.Helper()
	jwks, signJWT := testJWTSigner(t)
	srv := NewServer(Config{
		JWKSPublicKey:  jwks,
		LobeAPIBaseURL: "http://127.0.0.1:1",
		ServiceToken:   "service-token",
	})
	srv.cleanupDelay = time.Second
	return srv, httptest.NewServer(srv.Routes()), signJWT
}

func dialV2(t *testing.T, serverURL string, signJWT func(string, time.Time, time.Time) string, userID string) *testWS {
	t.Helper()
	token := signJWT(userID, time.Now().Add(time.Minute), time.Now().Add(-time.Minute))
	return dialWebSocket(t, serverURL, "/v2/ws?clientId=test-client&token="+url.QueryEscape(token))
}

func readUntil(t *testing.T, ws *testWS, match func(map[string]any) bool) map[string]any {
	t.Helper()
	for i := 0; i < 12; i++ {
		message := ws.readJSON(t)
		if match(message) {
			return message
		}
	}
	t.Fatal("matching websocket message was not received")
	return nil
}

func TestV2MultiplexesOperationsAndReplays(t *testing.T) {
	_, ts, signJWT := newV2TestServer(t)
	defer ts.Close()

	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "mux-a", "userId": "user-1"}, 200)
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "mux-b", "userId": "user-1"}, 200)
	postJSON(t, ts.URL+"/api/operations/push-event", eventBody("mux-a", "a1", "stream_chunk"), 200)
	postJSON(t, ts.URL+"/api/operations/push-event", eventBody("mux-b", "b1", "stream_chunk"), 200)

	ws := dialV2(t, ts.URL, signJWT, "user-1")
	defer ws.close()
	ready := ws.readJSON(t)
	if ready["type"] != "ready" || ready["protocol"] != float64(2) || ready["userId"] != "user-1" {
		t.Fatalf("unexpected ready message: %+v", ready)
	}

	ws.writeJSON(t, map[string]any{"type": "subscribe", "operationId": "mux-a"})
	a := ws.readJSON(t)
	if a["type"] != "agent_event" || a["operationId"] != "mux-a" || a["id"] != "1" {
		t.Fatalf("unexpected mux-a replay: %+v", a)
	}
	if event := a["event"].(map[string]any); event["operationId"] != nil {
		t.Fatalf("redundant event operationId should be omitted: %+v", event)
	}
	if done := ws.readJSON(t); done["type"] != "resume_complete" || done["status"] != "running" || done["gap"] != false {
		t.Fatalf("unexpected mux-a resume completion: %+v", done)
	}

	ws.writeJSON(t, map[string]any{"type": "subscribe", "operationId": "mux-b"})
	b := ws.readJSON(t)
	if b["type"] != "agent_event" || b["operationId"] != "mux-b" || b["id"] != "1" {
		t.Fatalf("unexpected mux-b replay: %+v", b)
	}
	_ = ws.readJSON(t)

	postJSON(t, ts.URL+"/api/operations/push-event", eventBody("mux-a", "a2", "stream_chunk"), 200)
	live := ws.readJSON(t)
	if live["operationId"] != "mux-a" || live["id"] != "2" {
		t.Fatalf("unexpected live multiplexed event: %+v", live)
	}

	ws.writeJSON(t, map[string]any{"type": "unsubscribe", "operationId": "mux-a"})
	postJSON(t, ts.URL+"/api/operations/push-event", eventBody("mux-a", "a3", "stream_chunk"), 200)
	wsExpectNoMessage(t, ws, 100*time.Millisecond)
}

func TestV2UpgradeAuthenticationCloseReasons(t *testing.T) {
	_, ts, signJWT := newV2TestServer(t)
	defer ts.Close()

	tests := []struct {
		name   string
		path   string
		reason string
	}{
		{name: "missing", path: "/v2/ws", reason: "auth_failed"},
		{name: "invalid", path: "/v2/ws?token=invalid", reason: "auth_failed"},
		{
			name:   "expired",
			path:   "/v2/ws?token=" + url.QueryEscape(signJWT("user-1", time.Now().Add(-time.Minute), time.Now().Add(-2*time.Minute))),
			reason: "auth_expired",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ws := dialWebSocket(t, ts.URL, test.path)
			defer ws.close()
			code, reason := readCloseFrame(t, ws)
			if code != wsCloseAuth || reason != test.reason {
				t.Fatalf("unexpected auth close: code=%d reason=%q", code, reason)
			}
		})
	}
}

func TestV2Heartbeat(t *testing.T) {
	_, ts, signJWT := newV2TestServer(t)
	defer ts.Close()
	ws := dialV2(t, ts.URL, signJWT, "user-1")
	defer ws.close()
	_ = ws.readJSON(t)
	ws.writeJSON(t, map[string]any{"type": "heartbeat"})
	if message := ws.readJSON(t); message["type"] != "heartbeat_ack" {
		t.Fatalf("unexpected heartbeat response: %+v", message)
	}
}

func TestV2PendingSubscribeLifecycleAndOwnership(t *testing.T) {
	_, ts, signJWT := newV2TestServer(t)
	defer ts.Close()

	ws := dialV2(t, ts.URL, signJWT, "user-1")
	defer ws.close()
	_ = ws.readJSON(t)
	ws.writeJSON(t, map[string]any{"type": "subscribe", "operationId": "mux-pending"})
	pending := ws.readJSON(t)
	if pending["type"] != "resume_complete" || pending["pending"] != true || pending["gap"] != false {
		t.Fatalf("unexpected pending response: %+v", pending)
	}

	postJSON(t, ts.URL+"/api/operations/init", map[string]any{
		"meta":        map[string]any{"agentId": "agent-1", "topicId": "topic-1", "ignored": "value"},
		"operationId": "mux-pending",
		"userId":      "user-1",
	}, 200)
	lifecycle := readUntil(t, ws, func(message map[string]any) bool { return message["type"] == "op_lifecycle" })
	if lifecycle["status"] != "running" || lifecycle["operationId"] != "mux-pending" {
		t.Fatalf("unexpected lifecycle: %+v", lifecycle)
	}
	meta := lifecycle["meta"].(map[string]any)
	if meta["agentId"] != "agent-1" || meta["topicId"] != "topic-1" || meta["ignored"] != nil {
		t.Fatalf("unexpected lifecycle meta: %+v", meta)
	}
	resumed := readUntil(t, ws, func(message map[string]any) bool {
		return message["type"] == "resume_complete" && message["pending"] == nil
	})
	if resumed["status"] != "running" {
		t.Fatalf("unexpected resumed status: %+v", resumed)
	}

	wrong := dialV2(t, ts.URL, signJWT, "user-2")
	defer wrong.close()
	_ = wrong.readJSON(t)
	wrong.writeJSON(t, map[string]any{"type": "subscribe", "operationId": "mux-pending"})
	failed := wrong.readJSON(t)
	if failed["type"] != "subscribe_failed" || failed["reason"] != "forbidden" {
		t.Fatalf("unexpected ownership response: %+v", failed)
	}
}

func TestInitWithoutMetaPreservesExistingMetadata(t *testing.T) {
	srv := NewServer(Config{ServiceToken: "service-token"})
	op := srv.getOrCreateOperation("meta-retry")
	op.init("meta-retry", "user-1", &operationMeta{AgentID: "agent-1", TopicID: "topic-1"})
	op.init("meta-retry", "user-1", nil)

	_, _, meta := op.hubSnapshot()
	if meta == nil || meta.AgentID != "agent-1" || meta.TopicID != "topic-1" {
		t.Fatalf("init retry cleared operation metadata: %+v", meta)
	}
}

func TestV2ToolExecutePrefersExecutor(t *testing.T) {
	_, ts, signJWT := newV2TestServer(t)
	defer ts.Close()
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "mux-tool", "userId": "user-1"}, 200)

	executor := dialV2(t, ts.URL, signJWT, "user-1")
	defer executor.close()
	observer := dialV2(t, ts.URL, signJWT, "user-1")
	defer observer.close()
	_ = executor.readJSON(t)
	_ = observer.readJSON(t)
	executor.writeJSON(t, map[string]any{"executor": true, "operationId": "mux-tool", "type": "subscribe"})
	observer.writeJSON(t, map[string]any{"operationId": "mux-tool", "type": "subscribe"})
	_ = executor.readJSON(t)
	_ = observer.readJSON(t)

	postJSON(t, ts.URL+"/api/operations/tool-execute", map[string]any{
		"data":        map[string]any{"apiName": "files.read", "arguments": "{}", "identifier": "files", "toolCallId": "call-1"},
		"operationId": "mux-tool",
	}, 200)
	message := executor.readJSON(t)
	if message["type"] != "agent_event" || message["operationId"] != "mux-tool" {
		t.Fatalf("unexpected executor event: %+v", message)
	}
	wsExpectNoMessage(t, observer, 100*time.Millisecond)
}

func TestMessagePatchIsV2Only(t *testing.T) {
	_, ts, signJWT := newV2TestServer(t)
	defer ts.Close()
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "mux-patch", "userId": "user-1"}, 200)

	v1 := dialWebSocket(t, ts.URL, "/ws?operationId=mux-patch")
	defer v1.close()
	v1.writeJSON(t, map[string]any{"token": "service-token", "type": "auth"})
	_ = v1.readJSON(t)
	v2 := dialV2(t, ts.URL, signJWT, "user-1")
	defer v2.close()
	_ = v2.readJSON(t)
	v2.writeJSON(t, map[string]any{"operationId": "mux-patch", "type": "subscribe"})
	_ = v2.readJSON(t)

	postJSON(t, ts.URL+"/api/operations/push-event", eventBody("mux-patch", "patch", "message_patch"), 200)
	patch := v2.readJSON(t)
	if event := patch["event"].(map[string]any); patch["type"] != "agent_event" || event["type"] != "message_patch" {
		t.Fatalf("unexpected v2 message patch: %+v", patch)
	}
	wsExpectNoMessage(t, v1, 100*time.Millisecond)

	v1.writeJSON(t, map[string]any{"lastEventId": "", "type": "resume"})
	wsExpectNoMessage(t, v1, 100*time.Millisecond)
}

func TestReplayReportsTrimmedGap(t *testing.T) {
	srv := NewServer(Config{ServiceToken: "service-token"})
	op := srv.getOrCreateOperation("gap-op")
	op.init("gap-op", "user-1", nil)
	for i := 0; i < eventBufferMax+1; i++ {
		op.pushEvent(agentStreamEvent{OperationID: "gap-op", Type: "step_complete"})
	}
	events, gap, status := op.collectReplay("1")
	if !gap || len(events) != eventBufferTrim || status != StatusRunning {
		t.Fatalf("unexpected replay gap result: events=%d gap=%v status=%s", len(events), gap, status)
	}
	var first struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(events[0], &first); err != nil || first.ID != "502" {
		t.Fatalf("unexpected first replay event: id=%s err=%v", first.ID, err)
	}
}

func TestV2ClientMessagesRequireSubscription(t *testing.T) {
	_, ts, signJWT := newV2TestServer(t)
	defer ts.Close()
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "mux-input", "userId": "user-1"}, 200)

	ws := dialV2(t, ts.URL, signJWT, "user-1")
	defer ws.close()
	_ = ws.readJSON(t)
	ws.writeJSON(t, map[string]any{"content": "nope", "operationId": "mux-input", "requestId": "r1", "type": "user_input"})
	errorMessage := ws.readJSON(t)
	if errorMessage["type"] != "error" || errorMessage["code"] != "not_subscribed" {
		t.Fatalf("unexpected not-subscribed error: %+v", errorMessage)
	}

	ws.writeJSON(t, map[string]any{"operationId": "mux-input", "type": "subscribe"})
	_ = ws.readJSON(t)
	done := make(chan map[string]any, 1)
	go func() {
		done <- postJSONBody(t, ts.URL+"/api/operations/request-input", map[string]any{
			"operationId": "mux-input", "prompt": "name?", "timeout": 1000,
		}, 200)
	}()
	request := ws.readJSON(t)
	requestID, _ := request["requestId"].(string)
	ws.writeJSON(t, map[string]any{"content": "alice", "operationId": "mux-input", "requestId": requestID, "type": "user_input"})
	if result := <-done; result["content"] != "alice" {
		t.Fatalf("unexpected input result: %+v", result)
	}
}

func TestV2PendingSubscriptionForwardsClientMessages(t *testing.T) {
	forwarded := make(chan struct{}, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/agent/tool-result" {
			forwarded <- struct{}{}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	jwks, signJWT := testJWTSigner(t)
	srv := NewServer(Config{JWKSPublicKey: jwks, LobeAPIBaseURL: backend.URL, ServiceToken: "service-token"})
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	ws := dialV2(t, ts.URL, signJWT, "user-1")
	defer ws.close()
	_ = ws.readJSON(t)
	ws.writeJSON(t, map[string]any{"operationId": "pending-forward", "type": "subscribe"})
	if pending := ws.readJSON(t); pending["pending"] != true {
		t.Fatalf("unexpected pending response: %+v", pending)
	}

	ws.writeJSON(t, map[string]any{
		"content": "forged", "operationId": "pending-forward", "success": true,
		"toolCallId": "call-1", "type": "tool_result",
	})
	select {
	case <-forwarded:
	case <-time.After(2 * time.Second):
		t.Fatal("pending subscription did not forward the client message")
	}
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{
		"operationId": "pending-forward",
		"userId":      "other-user",
	}, 200)
	ws.writeJSON(t, map[string]any{
		"content": "forged", "operationId": "pending-forward", "success": true,
		"toolCallId": "call-1", "type": "tool_result",
	})
	if message := ws.readJSON(t); message["type"] != "error" || message["code"] != "forbidden" {
		t.Fatalf("unexpected ownership error: %+v", message)
	}
	select {
	case <-forwarded:
		t.Fatal("wrong-owner subscription forwarded a client message")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestV2WatchdogLifecycleIncludesSummary(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/agent/finalize-abandoned" {
			t.Fatalf("unexpected backend path: %s", r.URL.Path)
		}
		writeJSON(w, http.StatusOK, map[string]any{"abandoned": true})
	}))
	defer backend.Close()

	jwks, signJWT := testJWTSigner(t)
	srv := NewServer(Config{JWKSPublicKey: jwks, LobeAPIBaseURL: backend.URL, ServiceToken: "service-token"})
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "watchdog-lifecycle", "userId": "user-1"}, 200)

	ws := dialV2(t, ts.URL, signJWT, "user-1")
	defer ws.close()
	_ = ws.readJSON(t)

	op := srv.getOperation("watchdog-lifecycle")
	op.mu.Lock()
	op.lastEventAt = time.Now().Add(-defaultIdleWatchdog - time.Second)
	op.lastEventTyp = "step_complete"
	op.mu.Unlock()
	op.fireWatchdog()

	lifecycle := readUntil(t, ws, func(message map[string]any) bool {
		return message["type"] == "op_lifecycle" && message["status"] == "error"
	})
	if summary, _ := lifecycle["summary"].(string); summary == "" || !strings.Contains(summary, "presumed killed mid-flight") {
		t.Fatalf("watchdog lifecycle omitted its summary: %+v", lifecycle)
	}
}

func TestV2ConcurrentEventsStayInSequence(t *testing.T) {
	srv, ts, signJWT := newV2TestServer(t)
	defer ts.Close()
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "ordered", "userId": "user-1"}, 200)

	ws := dialV2(t, ts.URL, signJWT, "user-1")
	defer ws.close()
	_ = ws.readJSON(t)
	ws.writeJSON(t, map[string]any{"operationId": "ordered", "type": "subscribe"})
	_ = ws.readJSON(t)

	op := srv.getOperation("ordered")
	const eventCount = 100
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < eventCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			op.pushEvent(agentStreamEvent{OperationID: "ordered", Type: "stream_chunk"})
		}()
	}
	close(start)
	wg.Wait()

	for i := 1; i <= eventCount; i++ {
		message := ws.readJSON(t)
		if message["id"] != strconv.Itoa(i) {
			t.Fatalf("event sequence broke at %d: %+v", i, message)
		}
	}
}

func TestV2HubQueueOverflowDisconnectsSubscribers(t *testing.T) {
	srv, ts, signJWT := newV2TestServer(t)
	defer ts.Close()
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "overflow", "userId": "user-1"}, 200)

	ws := dialV2(t, ts.URL, signJWT, "user-1")
	defer ws.close()
	_ = ws.readJSON(t)
	ws.writeJSON(t, map[string]any{"operationId": "overflow", "type": "subscribe"})
	_ = ws.readJSON(t)

	op := srv.getOperation("overflow")
	op.mu.Lock()
	op.hubDispatch = true
	for i := 0; i <= eventBufferMax; i++ {
		op.queueHubEventLocked(map[string]any{"id": strconv.Itoa(i + 1), "type": "status_change"})
	}
	if !op.hubOverflow || len(op.hubQueue) > eventBufferMax {
		t.Fatalf("hub queue was not bounded: overflow=%v len=%d", op.hubOverflow, len(op.hubQueue))
	}
	op.mu.Unlock()
	go op.drainHubEvents()

	_ = ws.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := ws.br.ReadByte(); err == nil {
		t.Fatal("expected overflow to close the subscribed socket")
	}
}

func TestV2WriteFailureDropsConnectionWithoutAdvancingSequence(t *testing.T) {
	srv := NewServer(Config{ServiceToken: "service-token"})
	op := srv.getOrCreateOperation("write-fail")
	op.init("write-fail", "user-1", nil)

	serverSide, peer := net.Pipe()
	_ = peer.Close()
	defer serverSide.Close()
	sub := &hubSubscription{state: "live"}
	conn := &hubConnection{
		server: srv,
		subs:   map[string]*hubSubscription{"write-fail": sub},
		userID: "user-1",
		ws:     &wsConn{conn: serverSide},
	}
	srv.v2Connections[conn] = struct{}{}

	srv.deliverOperationEvent(op, json.RawMessage(`{"id":"1","status":"running","type":"status_change"}`), false)

	if sub.lastSeq != 0 {
		t.Fatalf("failed write advanced subscription sequence to %d", sub.lastSeq)
	}
	srv.v2Mu.RLock()
	_, connected := srv.v2Connections[conn]
	srv.v2Mu.RUnlock()
	if connected {
		t.Fatal("failed write left the hub connection registered")
	}
}

func TestV2ToolResultForwarding(t *testing.T) {
	forwarded := make(chan map[string]any, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/agent/tool-result" || r.Header.Get("Authorization") != "Bearer service-token" {
			t.Errorf("unexpected tool result request: %s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode tool result: %v", err)
		}
		forwarded <- body
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	jwks, signJWT := testJWTSigner(t)
	srv := NewServer(Config{JWKSPublicKey: jwks, LobeAPIBaseURL: backend.URL, ServiceToken: "service-token"})
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "mux-result", "userId": "user-1"}, 200)
	ws := dialV2(t, ts.URL, signJWT, "user-1")
	defer ws.close()
	_ = ws.readJSON(t)
	ws.writeJSON(t, map[string]any{"operationId": "mux-result", "type": "subscribe"})
	_ = ws.readJSON(t)
	ws.writeJSON(t, map[string]any{
		"content": "done", "executionTimeMs": 25, "operationId": "mux-result",
		"state": map[string]any{"cursor": 2}, "success": true, "toolCallId": "call-1",
		"type": "tool_result", "workRegistration": map[string]any{"kind": "job"},
	})

	select {
	case body := <-forwarded:
		if body["content"] != "done" || body["executionTimeMs"] != float64(25) || body["toolCallId"] != "call-1" {
			t.Fatalf("unexpected tool result body: %+v", body)
		}
		if _, ok := body["type"]; ok {
			t.Fatalf("client message type should not be forwarded: %+v", body)
		}
		if _, ok := body["error"]; ok {
			t.Fatalf("undefined optional error should be omitted: %+v", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("tool result was not forwarded")
	}
}

func TestV1PendingOwnerIsRecheckedOnInit(t *testing.T) {
	_, ts, signJWT := newV2TestServer(t)
	defer ts.Close()

	ws := dialWebSocket(t, ts.URL, "/ws?operationId=pending-owner")
	defer ws.close()
	ws.writeJSON(t, map[string]any{
		"token": signJWT("wrong-user", time.Now().Add(time.Minute), time.Now().Add(-time.Minute)),
		"type":  "auth",
	})
	if message := ws.readJSON(t); message["type"] != "auth_success" {
		t.Fatalf("unexpected pre-init auth response: %+v", message)
	}
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "pending-owner", "userId": "right-user"}, 200)

	_ = ws.conn.SetReadDeadline(time.Now().Add(time.Second))
	first, err := ws.br.ReadByte()
	if err != nil || first&0x0f != wsCloseMessage {
		t.Fatalf("expected policy close after owner mismatch, opcode=%d err=%v", first&0x0f, err)
	}
}

func TestV1OwnerIsRecheckedWhenInitRacesAuthentication(t *testing.T) {
	authStarted := make(chan struct{}, 1)
	releaseAuth := make(chan struct{})
	identity := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		authStarted <- struct{}{}
		<-releaseAuth
		writeJSON(w, http.StatusOK, map[string]any{"data": map[string]any{"id": "wrong-user"}})
	}))
	defer identity.Close()

	srv := NewServer(Config{ServiceToken: "service-token"})
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()
	ws := dialWebSocket(t, ts.URL, "/ws?operationId=auth-init-race")
	defer ws.close()

	ws.writeJSON(t, map[string]any{
		"serverUrl": identity.URL,
		"token":     "api-key",
		"tokenType": "apiKey",
		"type":      "auth",
	})
	select {
	case <-authStarted:
	case <-time.After(time.Second):
		t.Fatal("api-key authentication did not start")
	}
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{
		"operationId": "auth-init-race",
		"userId":      "right-user",
	}, 200)
	close(releaseAuth)

	message := ws.readJSON(t)
	if message["type"] != "auth_failed" || message["reason"] != "userId mismatch" {
		t.Fatalf("unexpected auth race response: %+v", message)
	}
}

func TestToolExecuteUsesInflightWatchdog(t *testing.T) {
	if !inflightEventTypes["tool_execute"] {
		t.Fatal("tool_execute must use the inflight watchdog timeout")
	}
}

func eventBody(operationID string, content string, eventType string) map[string]any {
	return map[string]any{
		"event": map[string]any{
			"data": map[string]any{"content": content}, "operationId": operationID, "stepIndex": 0,
			"timestamp": time.Now().UnixMilli(), "type": eventType,
		},
		"operationId": operationID,
	}
}

func readCloseFrame(t *testing.T, ws *testWS) (int, string) {
	t.Helper()
	_ = ws.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	header := make([]byte, 2)
	if _, err := io.ReadFull(ws.br, header); err != nil {
		t.Fatal(err)
	}
	if header[0]&0x0f != wsCloseMessage {
		t.Fatalf("unexpected websocket opcode %d", header[0]&0x0f)
	}
	length := int(header[1] & 0x7f)
	payload := make([]byte, length)
	if _, err := io.ReadFull(ws.br, payload); err != nil {
		t.Fatal(err)
	}
	if len(payload) < 2 {
		t.Fatalf("close frame missing code: %x", payload)
	}
	return int(binary.BigEndian.Uint16(payload[:2])), string(payload[2:])
}
