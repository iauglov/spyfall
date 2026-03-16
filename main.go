package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const defaultGameDuration = 8 * 60 // seconds

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var allLocations = []string{
	"Самолёт", "Банк", "Пляж", "Посольство", "Больница",
	"Отель", "Военная база", "Казино", "Цирк", "Университет",
	"Кинотеатр", "Круизный лайнер", "Полицейский участок", "Ресторан",
	"Подводная лодка", "Супермаркет", "Театр", "Поезд", "Зоопарк",
	"Космическая станция", "Пиратский корабль", "Замок", "Тюрьма",
	"Корпоративная вечеринка", "Школа",
}

// ── Msg ───────────────────────────────────────────────────────────────────────

type Msg struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func enc(t string, v interface{}) Msg {
	b, _ := json.Marshal(v)
	return Msg{Type: t, Payload: b}
}

// ── Player ────────────────────────────────────────────────────────────────────

type Player struct {
	ID    string
	Name  string
	Conn  *websocket.Conn
	mu    sync.Mutex
	IsSpy bool
}

func (p *Player) send(v interface{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_ = p.Conn.WriteJSON(v)
}

type PlayerInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func toInfoList(players []*Player) []PlayerInfo {
	list := make([]PlayerInfo, len(players))
	for i, p := range players {
		list[i] = PlayerInfo{p.ID, p.Name}
	}
	return list
}

// ── Room ──────────────────────────────────────────────────────────────────────

type AccuseState struct {
	TargetID  string
	AccuserID string
	Votes     map[string]bool // playerID → guilty?
}

type Room struct {
	Code     string
	Players  []*Player
	HostID   string
	mu       sync.Mutex
	State    string // lobby | playing | voting | over
	Location string
	SpyID    string
	Duration int
	Accuse   *AccuseState
	quit     chan struct{}
}

func (r *Room) broadcast(v interface{}) {
	r.mu.Lock()
	snap := make([]*Player, len(r.Players))
	copy(snap, r.Players)
	r.mu.Unlock()
	for _, p := range snap {
		p.send(v)
	}
}

func (r *Room) startGame() {
	r.mu.Lock()
	r.Location = allLocations[rand.Intn(len(allLocations))]
	spyIdx := rand.Intn(len(r.Players))
	r.SpyID = r.Players[spyIdx].ID
	for _, p := range r.Players {
		p.IsSpy = p.ID == r.SpyID
	}
	r.State = "playing"
	r.quit = make(chan struct{})
	snap := make([]*Player, len(r.Players))
	copy(snap, r.Players)
	loc := r.Location
	r.mu.Unlock()

	infos := toInfoList(snap)
	for _, p := range snap {
		role := "civilian"
		var locVal interface{} = loc
		if p.IsSpy {
			role = "spy"
			locVal = nil
		}
		p.send(enc("game_started", map[string]interface{}{
			"role":      role,
			"location":  locVal,
			"players":   infos,
			"locations": allLocations,
			"duration":  r.Duration,
		}))
	}

	go r.runTimer()
}

func (r *Room) runTimer() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	left := r.Duration
	for {
		select {
		case <-r.quit:
			return
		case <-ticker.C:
			left--
			r.broadcast(enc("timer_tick", map[string]int{"seconds": left}))
			if left <= 0 {
				r.endGame("time", true) // spy wins on timeout
				return
			}
		}
	}
}

func (r *Room) endGame(reason string, spyWins bool) {
	r.mu.Lock()
	if r.State == "over" {
		r.mu.Unlock()
		return
	}
	r.State = "over"
	quit := r.quit
	spyID := r.SpyID
	loc := r.Location
	spyName := ""
	for _, p := range r.Players {
		if p.ID == spyID {
			spyName = p.Name
			break
		}
	}
	r.mu.Unlock()

	// safe close
	select {
	case <-quit:
	default:
		close(quit)
	}

	winner := "civilians"
	if spyWins {
		winner = "spy"
	}
	r.broadcast(enc("game_over", map[string]interface{}{
		"reason":   reason,
		"spy_id":   spyID,
		"spy_name": spyName,
		"location": loc,
		"winner":   winner,
	}))
}

func (r *Room) startAccuse(accuserID, targetID string) {
	r.mu.Lock()
	if r.State != "playing" {
		r.mu.Unlock()
		return
	}
	r.State = "voting"
	r.Accuse = &AccuseState{
		TargetID:  targetID,
		AccuserID: accuserID,
		Votes:     make(map[string]bool),
	}
	snap := make([]*Player, len(r.Players))
	copy(snap, r.Players)
	r.mu.Unlock()

	accuserName, targetName := "", ""
	for _, p := range snap {
		if p.ID == accuserID {
			accuserName = p.Name
		}
		if p.ID == targetID {
			targetName = p.Name
		}
	}

	r.broadcast(enc("accuse_started", map[string]interface{}{
		"accuser_id":   accuserID,
		"accuser_name": accuserName,
		"target_id":    targetID,
		"target_name":  targetName,
		"players":      toInfoList(snap),
	}))
}

func (r *Room) submitAccuseVote(voterID string, guilty bool) {
	r.mu.Lock()
	acc := r.Accuse
	if acc == nil || r.State != "voting" {
		r.mu.Unlock()
		return
	}
	if voterID == acc.AccuserID || voterID == acc.TargetID {
		r.mu.Unlock()
		return
	}
	if _, already := acc.Votes[voterID]; already {
		r.mu.Unlock()
		return
	}
	acc.Votes[voterID] = guilty

	eligible := 0
	for _, p := range r.Players {
		if p.ID != acc.AccuserID && p.ID != acc.TargetID {
			eligible++
		}
	}
	if len(acc.Votes) < eligible {
		r.mu.Unlock()
		return
	}

	yes, no := 0, 0
	for _, v := range acc.Votes {
		if v {
			yes++
		} else {
			no++
		}
	}
	targetID := acc.TargetID
	targetName, targetIsSpy := "", false
	for _, p := range r.Players {
		if p.ID == targetID {
			targetName = p.Name
			targetIsSpy = p.IsSpy
			break
		}
	}

	r.State = "playing"
	r.Accuse = nil
	r.mu.Unlock()

	majority := yes > no
	r.broadcast(enc("accuse_result", map[string]interface{}{
		"target_id":      targetID,
		"target_name":    targetName,
		"target_is_spy":  targetIsSpy,
		"guilty_votes":   yes,
		"innocent_votes": no,
		"majority":       majority,
	}))

	if majority {
		if targetIsSpy {
			r.endGame("voted_spy", false)
		} else {
			r.endGame("wrongly_accused", true)
		}
	}
}

// ── Global store ──────────────────────────────────────────────────────────────

var (
	rooms   = make(map[string]*Room)
	roomsMu sync.Mutex
)

func randCode() string {
	const ch = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	b := make([]byte, 4)
	for i := range b {
		b[i] = ch[rand.Intn(len(ch))]
	}
	return string(b)
}

func randID() string {
	const ch = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 12)
	for i := range b {
		b[i] = ch[rand.Intn(len(ch))]
	}
	return string(b)
}

// ── WebSocket handler ─────────────────────────────────────────────────────────

func handleWS(w http.ResponseWriter, req *http.Request) {
	conn, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		log.Println("upgrade:", err)
		return
	}
	defer conn.Close()

	player := &Player{ID: randID(), Conn: conn}
	var room *Room

	defer func() {
		if room == nil {
			return
		}
		room.mu.Lock()
		for i, p := range room.Players {
			if p.ID == player.ID {
				room.Players = append(room.Players[:i], room.Players[i+1:]...)
				break
			}
		}
		n := len(room.Players)
		if n == 0 {
			room.mu.Unlock()
			roomsMu.Lock()
			delete(rooms, room.Code)
			roomsMu.Unlock()
			return
		}
		if room.HostID == player.ID {
			room.HostID = room.Players[0].ID
		}
		hostID := room.HostID
		snap := make([]*Player, n)
		copy(snap, room.Players)
		room.mu.Unlock()

		for _, p := range snap {
			p.send(enc("player_list", map[string]interface{}{
				"players": toInfoList(snap),
				"host_id": hostID,
			}))
		}
	}()

	for {
		var msg Msg
		if err := conn.ReadJSON(&msg); err != nil {
			break
		}

		switch msg.Type {

		case "create_room":
			var p struct {
				Name     string `json:"name"`
				Duration int    `json:"duration"`
			}
			json.Unmarshal(msg.Payload, &p)
			if p.Name == "" {
				player.send(enc("error", map[string]string{"message": "Введите имя"}))
				continue
			}
			if p.Duration <= 0 {
				p.Duration = defaultGameDuration
			}
			player.Name = p.Name
			code := randCode()
			room = &Room{Code: code, Players: []*Player{player}, HostID: player.ID, State: "lobby", Duration: p.Duration}
			roomsMu.Lock()
			rooms[code] = room
			roomsMu.Unlock()
			player.send(enc("room_joined", map[string]interface{}{
				"code":    code,
				"your_id": player.ID,
				"host_id": player.ID,
				"players": toInfoList(room.Players),
			}))

		case "join_room":
			var p struct {
				Code string `json:"code"`
				Name string `json:"name"`
			}
			json.Unmarshal(msg.Payload, &p)
			if p.Name == "" {
				player.send(enc("error", map[string]string{"message": "Введите имя"}))
				continue
			}
			roomsMu.Lock()
			r, ok := rooms[p.Code]
			roomsMu.Unlock()
			if !ok {
				player.send(enc("error", map[string]string{"message": "Комната не найдена"}))
				continue
			}
			r.mu.Lock()
			if r.State != "lobby" {
				r.mu.Unlock()
				player.send(enc("error", map[string]string{"message": "Игра уже началась"}))
				continue
			}
			player.Name = p.Name
			r.Players = append(r.Players, player)
			snap := make([]*Player, len(r.Players))
			copy(snap, r.Players)
			hostID := r.HostID
			r.mu.Unlock()
			room = r

			infos := toInfoList(snap)
			player.send(enc("room_joined", map[string]interface{}{
				"code":    r.Code,
				"your_id": player.ID,
				"host_id": hostID,
				"players": infos,
			}))
			for _, pl := range snap {
				if pl.ID != player.ID {
					pl.send(enc("player_list", map[string]interface{}{
						"players": infos,
						"host_id": hostID,
					}))
				}
			}

		case "start_game":
			if room == nil {
				continue
			}
			room.mu.Lock()
			isHost := room.HostID == player.ID
			n := len(room.Players)
			room.mu.Unlock()
			if !isHost {
				player.send(enc("error", map[string]string{"message": "Только хост может начать игру"}))
				continue
			}
			if n < 3 {
				player.send(enc("error", map[string]string{"message": "Нужно минимум 3 игрока"}))
				continue
			}
			room.startGame()

		case "chat":
			if room == nil {
				continue
			}
			var p struct {
				Text string `json:"text"`
			}
			json.Unmarshal(msg.Payload, &p)
			if p.Text == "" {
				continue
			}
			room.broadcast(enc("chat_msg", map[string]interface{}{
				"from_id":   player.ID,
				"from_name": player.Name,
				"text":      p.Text,
			}))

		case "accuse":
			if room == nil {
				continue
			}
			var p struct {
				TargetID string `json:"target_id"`
			}
			json.Unmarshal(msg.Payload, &p)
			if p.TargetID == player.ID {
				player.send(enc("error", map[string]string{"message": "Нельзя обвинить себя"}))
				continue
			}
			room.startAccuse(player.ID, p.TargetID)

		case "accuse_vote":
			if room == nil {
				continue
			}
			var p struct {
				Guilty bool `json:"guilty"`
			}
			json.Unmarshal(msg.Payload, &p)
			room.submitAccuseVote(player.ID, p.Guilty)

		case "spy_guess":
			if room == nil || !player.IsSpy {
				continue
			}
			var p struct {
				Location string `json:"location"`
			}
			json.Unmarshal(msg.Payload, &p)
			correct := p.Location == room.Location
			room.broadcast(enc("spy_guess_result", map[string]interface{}{
				"spy_name": player.Name,
				"location": p.Location,
				"correct":  correct,
			}))
			if correct {
				room.endGame("spy_guessed", true)
			} else {
				room.endGame("spy_guessed_wrong", false)
			}
		}
	}
}

func main() {
	http.Handle("/", http.FileServer(http.Dir("static")))
	http.HandleFunc("/ws", handleWS)
	log.Println("Сервер запущен на http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
