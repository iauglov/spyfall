package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWebSocketCircleAndReconnect(t *testing.T) {
	var handlers sync.WaitGroup
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		handlers.Add(1)
		defer handlers.Done()
		handleWS(w, req)
	}))
	var connections []*websocket.Conn
	var code string
	t.Cleanup(func() {
		for _, c := range connections {
			c.Close()
		}
		server.Close()
		handlers.Wait()
		roomsMu.Lock()
		r := rooms[code]
		delete(rooms, code)
		roomsMu.Unlock()
		if r != nil {
			r.mu.Lock()
			for _, p := range r.Players {
				if p.graceTimer != nil {
					p.graceTimer.Stop()
				}
			}
			if r.RoundVote != nil {
				r.RoundVote.timer.Stop()
			}
			r.mu.Unlock()
		}
	})
	connect := func() *websocket.Conn {
		t.Helper()
		c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, c)
		return c
	}
	send := func(c *websocket.Conn, kind string, payload interface{}) {
		t.Helper()
		if err := c.WriteJSON(enc(kind, payload)); err != nil {
			t.Fatal(err)
		}
	}
	read := func(c *websocket.Conn, kind string, out interface{}) {
		t.Helper()
		c.SetReadDeadline(time.Now().Add(3 * time.Second))
		for {
			var msg Msg
			if err := c.ReadJSON(&msg); err != nil {
				t.Fatalf("waiting for %s: %v", kind, err)
			}
			if msg.Type == "error" {
				t.Fatalf("server error: %s", msg.Payload)
			}
			if msg.Type == kind {
				if err := json.Unmarshal(msg.Payload, out); err != nil {
					t.Fatal(err)
				}
				return
			}
		}
	}
	type joined struct {
		Code   string
		YourID string `json:"your_id"`
		Token  string
	}
	clients := []*websocket.Conn{connect(), connect(), connect()}
	names := []string{"Первый", "Второй", "Третий"}
	ids := make([]string, 3)
	var first joined
	send(clients[0], "create_room", map[string]interface{}{"name": names[0], "spy_count": 1})
	read(clients[0], "room_joined", &first)
	code, ids[0] = first.Code, first.YourID
	for i := 1; i < 3; i++ {
		send(clients[i], "join_room", map[string]string{"code": code, "name": names[i]})
		var p joined
		read(clients[i], "room_joined", &p)
		ids[i] = p.YourID
	}
	type flow struct {
		State string
		Round int
		Turn  *TurnState
		Vote  *struct {
			ID    uint64
			Voted []string
		}
	}
	send(clients[0], "start_game", struct{}{})
	var current flow
	read(clients[0], "round_state", &current)
	if current.Turn == nil || current.Turn.AskerID != ids[0] || current.Turn.AnswererID != ids[1] {
		t.Fatalf("initial turn: %+v", current)
	}
	send(clients[0], "turn_confirm", map[string]interface{}{"turn_id": current.Turn.ID})
	read(clients[0], "round_state", &current)
	if !current.Turn.Confirmed[ids[0]] {
		t.Fatal("first confirmation not acknowledged")
	}
	// Reconnect even while the prior connection's disconnect callback may be running.
	clients[0].Close()
	clients[0] = connect()
	send(clients[0], "reconnect", map[string]string{"code": code, "name": names[0], "token": first.Token})
	var restored struct {
		RoundState flow `json:"round_state"`
	}
	read(clients[0], "reconnected", &restored)
	current = restored.RoundState
	read(clients[0], "round_state", &current) // The handshake must be followed by a fresh authoritative snapshot.
	if current.Turn == nil || !current.Turn.Confirmed[ids[0]] {
		t.Fatal("reconnect lost the confirmation")
	}
	send(clients[1], "turn_confirm", map[string]interface{}{"turn_id": current.Turn.ID})
	read(clients[0], "round_state", &current)
	for i := 1; i < 3; i++ {
		if current.Turn == nil || current.Turn.AskerID != ids[i] || current.Turn.AnswererID != ids[(i+1)%3] {
			t.Fatalf("wrong pair at step %d: %+v", i, current.Turn)
		}
		send(clients[i], "turn_confirm", map[string]interface{}{"turn_id": current.Turn.ID})
		read(clients[0], "round_state", &current)
		send(clients[(i+1)%3], "turn_confirm", map[string]interface{}{"turn_id": current.Turn.ID})
		read(clients[0], "round_state", &current)
	}
	if current.State != "voting" || current.Vote == nil {
		t.Fatal("circle did not start vote")
	}
	voteID := current.Vote.ID
	for i := 0; i < 3; i++ {
		send(clients[i], "round_vote", map[string]interface{}{"vote_id": voteID, "target_id": ""})
		read(clients[0], "round_state", &current)
	}
	var result struct {
		TargetID string `json:"target_id"`
	}
	read(clients[0], "round_vote_result", &result)
	if result.TargetID != "" {
		t.Fatal("abstentions eliminated player")
	}
	read(clients[0], "round_state", &current)
	if current.Round != 2 || current.Turn == nil || current.Turn.AskerID != ids[0] {
		t.Fatal("next circle did not start")
	}
}
