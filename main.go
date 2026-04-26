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
	ID         string
	Name       string
	Token      string
	Conn       *websocket.Conn
	mu         sync.Mutex
	IsSpy      bool
	Alive      bool
	Connected  bool
	graceTimer *time.Timer
}

func (p *Player) send(v interface{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.Conn == nil {
		return
	}
	_ = p.Conn.WriteJSON(v)
}

type PlayerInfo struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Connected bool   `json:"connected"`
	Alive     bool   `json:"alive"`
}

func toInfoList(players []*Player) []PlayerInfo {
	list := make([]PlayerInfo, len(players))
	for i, p := range players {
		list[i] = PlayerInfo{p.ID, p.Name, p.Connected, p.Alive}
	}
	return list
}

// ── Room ──────────────────────────────────────────────────────────────────────

type AccuseState struct {
	TargetID  string
	AccuserID string
	Votes     map[string]bool // playerID → guilty?
	EndsAt    time.Time
	timer     *time.Timer
}

type Room struct {
	Code         string
	Players      []*Player
	HostID       string
	mu           sync.Mutex
	State        string // lobby | playing | voting | over
	Location     string
	SpyCount     int
	Accuse       *AccuseState
	LastGameOver map[string]interface{}
}

const (
	graceLobby   = 60 * time.Second
	graceGame    = 5 * time.Minute
	maxSpies     = 3
	voteDuration = 30 * time.Second
)

func (r *Room) aliveSpies() int {
	n := 0
	for _, p := range r.Players {
		if p.IsSpy && p.Alive {
			n++
		}
	}
	return n
}

func (r *Room) aliveCivilians() int {
	n := 0
	for _, p := range r.Players {
		if !p.IsSpy && p.Alive {
			n++
		}
	}
	return n
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

	// Pick SpyCount unique spies.
	n := len(r.Players)
	perm := rand.Perm(n)
	spyCount := r.SpyCount
	if spyCount < 1 {
		spyCount = 1
	}
	if spyCount > n-2 {
		spyCount = n - 2
	}
	spySet := make(map[int]bool, spyCount)
	for i := 0; i < spyCount; i++ {
		spySet[perm[i]] = true
	}
	for i, p := range r.Players {
		p.IsSpy = spySet[i]
		p.Alive = true
	}
	r.State = "playing"
	r.LastGameOver = nil
	snap := make([]*Player, n)
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
			"spy_count": spyCount,
		}))
	}
}

func (r *Room) endGame(reason string, spyWins bool) {
	r.mu.Lock()
	if r.State == "over" {
		r.mu.Unlock()
		return
	}
	r.State = "over"
	loc := r.Location
	spies := []map[string]string{}
	for _, p := range r.Players {
		if p.IsSpy {
			spies = append(spies, map[string]string{"id": p.ID, "name": p.Name})
		}
	}
	winner := "civilians"
	if spyWins {
		winner = "spies"
	}
	payload := map[string]interface{}{
		"reason":   reason,
		"spies":    spies,
		"location": loc,
		"winner":   winner,
	}
	r.LastGameOver = payload
	r.mu.Unlock()

	r.broadcast(enc("game_over", payload))
}

func (r *Room) startAccuse(accuserID, targetID string) {
	r.mu.Lock()
	if r.State != "playing" {
		r.mu.Unlock()
		return
	}
	var accuser, target *Player
	for _, p := range r.Players {
		if p.ID == accuserID {
			accuser = p
		}
		if p.ID == targetID {
			target = p
		}
	}
	if accuser == nil || !accuser.Alive || target == nil || !target.Alive {
		r.mu.Unlock()
		return
	}
	r.State = "voting"
	acc := &AccuseState{
		TargetID:  targetID,
		AccuserID: accuserID,
		Votes:     make(map[string]bool),
		EndsAt:    time.Now().Add(voteDuration),
	}
	acc.timer = time.AfterFunc(voteDuration, r.finalizeAccuse)
	r.Accuse = acc
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
		"vote_seconds": int(voteDuration / time.Second),
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
	var voter *Player
	for _, p := range r.Players {
		if p.ID == voterID {
			voter = p
			break
		}
	}
	if voter == nil || !voter.Alive {
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
		if p.Alive && p.ID != acc.AccuserID && p.ID != acc.TargetID {
			eligible++
		}
	}
	if len(acc.Votes) < eligible {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	r.finalizeAccuse()
}

// finalizeAccuse resolves the current vote — either because everyone has voted
// or because the voteDuration timer expired. Any eligible voter who hasn't
// voted counts as "innocent". Idempotent: a second call after State flips back
// to "playing" is a no-op (handles race between last vote and timer).
func (r *Room) finalizeAccuse() {
	r.mu.Lock()
	acc := r.Accuse
	if acc == nil || r.State != "voting" {
		r.mu.Unlock()
		return
	}
	if acc.timer != nil {
		acc.timer.Stop()
	}

	// Auto-fill missing votes as "innocent".
	for _, p := range r.Players {
		if !p.Alive {
			continue
		}
		if p.ID == acc.AccuserID || p.ID == acc.TargetID {
			continue
		}
		if _, voted := acc.Votes[p.ID]; !voted {
			acc.Votes[p.ID] = false
		}
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
	var target *Player
	for _, p := range r.Players {
		if p.ID == targetID {
			target = p
			break
		}
	}
	targetName := ""
	targetIsSpy := false
	if target != nil {
		targetName = target.Name
		targetIsSpy = target.IsSpy
	}

	majority := yes > no
	targetDied := false
	var endWithSpyWins *bool
	var endReason string
	if majority && target != nil && target.Alive {
		target.Alive = false
		targetDied = true
		if targetIsSpy {
			if r.aliveSpies() == 0 {
				f := false
				endWithSpyWins = &f
				endReason = "all_spies_caught"
			}
		} else {
			if r.aliveCivilians() == 0 {
				t := true
				endWithSpyWins = &t
				endReason = "all_civilians_dead"
			}
		}
	}

	r.State = "playing"
	r.Accuse = nil
	snap := make([]*Player, len(r.Players))
	copy(snap, r.Players)
	hostID := r.HostID
	r.mu.Unlock()

	r.broadcast(enc("accuse_result", map[string]interface{}{
		"target_id":      targetID,
		"target_name":    targetName,
		"target_is_spy":  targetIsSpy,
		"target_died":    targetDied,
		"guilty_votes":   yes,
		"innocent_votes": no,
		"majority":       majority,
	}))
	if targetDied {
		for _, pl := range snap {
			pl.send(enc("player_list", map[string]interface{}{
				"players": toInfoList(snap),
				"host_id": hostID,
			}))
		}
	}

	if endWithSpyWins != nil {
		r.endGame(endReason, *endWithSpyWins)
	}
}

func (r *Room) handleSpyGuess(spyID, guess string) {
	r.mu.Lock()
	if r.State != "playing" {
		r.mu.Unlock()
		return
	}
	var spy *Player
	for _, p := range r.Players {
		if p.ID == spyID {
			spy = p
			break
		}
	}
	if spy == nil || !spy.IsSpy || !spy.Alive {
		r.mu.Unlock()
		return
	}
	correct := guess == r.Location
	spyName := spy.Name
	var endWithSpyWins *bool
	var endReason string
	if correct {
		t := true
		endWithSpyWins = &t
		endReason = "spy_guessed"
	} else {
		spy.Alive = false
		if r.aliveSpies() == 0 {
			f := false
			endWithSpyWins = &f
			endReason = "all_spies_caught"
		}
	}
	snap := make([]*Player, len(r.Players))
	copy(snap, r.Players)
	hostID := r.HostID
	r.mu.Unlock()

	r.broadcast(enc("spy_guess_result", map[string]interface{}{
		"spy_name": spyName,
		"location": guess,
		"correct":  correct,
	}))
	if !correct {
		for _, pl := range snap {
			pl.send(enc("player_list", map[string]interface{}{
				"players": toInfoList(snap),
				"host_id": hostID,
			}))
		}
	}

	if endWithSpyWins != nil {
		r.endGame(endReason, *endWithSpyWins)
	}
}

func (r *Room) removeDisconnected(playerID string) {
	r.mu.Lock()
	idx := -1
	for i, pl := range r.Players {
		if pl.ID == playerID {
			if pl.Connected {
				r.mu.Unlock()
				return
			}
			idx = i
			break
		}
	}
	if idx < 0 {
		r.mu.Unlock()
		return
	}
	removed := r.Players[idx]
	r.Players = append(r.Players[:idx], r.Players[idx+1:]...)
	n := len(r.Players)
	if n == 0 {
		r.mu.Unlock()
		roomsMu.Lock()
		delete(rooms, r.Code)
		roomsMu.Unlock()
		return
	}
	if r.HostID == removed.ID {
		r.HostID = r.Players[0].ID
		for _, pl := range r.Players {
			if pl.Connected {
				r.HostID = pl.ID
				break
			}
		}
	}

	// If we were mid-game, the departure may have caught the last spy or
	// civilian — check win conditions.
	var endWithSpyWins *bool
	var endReason string
	if (r.State == "playing" || r.State == "voting") && removed.Alive {
		if removed.IsSpy && r.aliveSpies() == 0 {
			f := false
			endWithSpyWins = &f
			endReason = "all_spies_caught"
		} else if !removed.IsSpy && r.aliveCivilians() == 0 {
			t := true
			endWithSpyWins = &t
			endReason = "all_civilians_dead"
		}
	}
	hostID := r.HostID
	snap := make([]*Player, n)
	copy(snap, r.Players)
	r.mu.Unlock()

	for _, pl := range snap {
		pl.send(enc("player_list", map[string]interface{}{
			"players": toInfoList(snap),
			"host_id": hostID,
		}))
	}
	if endWithSpyWins != nil {
		r.endGame(endReason, *endWithSpyWins)
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

	player := &Player{ID: randID(), Conn: conn, Connected: true}
	var room *Room

	defer func() {
		if room == nil {
			return
		}
		room.mu.Lock()
		// If the socket was never reconnected to an existing slot, `player`
		// is the original Player we added. Otherwise `player` points at the
		// existing slot. Either way, find the slot by ID.
		var p *Player
		for _, pl := range room.Players {
			if pl.ID == player.ID {
				p = pl
				break
			}
		}
		if p == nil {
			room.mu.Unlock()
			return
		}
		// This socket is still the active one? If a reconnect already swapped
		// the Conn, the new connection's defer shouldn't revoke it.
		p.mu.Lock()
		stillOurs := p.Conn == conn
		if stillOurs {
			p.Conn = nil
		}
		p.mu.Unlock()
		if !stillOurs {
			room.mu.Unlock()
			return
		}
		p.Connected = false
		grace := graceLobby
		if room.State == "playing" || room.State == "voting" {
			grace = graceGame
		}
		r := room
		pid := p.ID
		if p.graceTimer != nil {
			p.graceTimer.Stop()
		}
		p.graceTimer = time.AfterFunc(grace, func() {
			r.removeDisconnected(pid)
		})

		snap := make([]*Player, len(room.Players))
		copy(snap, room.Players)
		hostID := room.HostID
		room.mu.Unlock()

		for _, pl := range snap {
			pl.send(enc("player_list", map[string]interface{}{
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
				SpyCount int    `json:"spy_count"`
			}
			json.Unmarshal(msg.Payload, &p)
			if p.Name == "" {
				player.send(enc("error", map[string]string{"message": "Введите имя"}))
				continue
			}
			if p.SpyCount < 1 {
				p.SpyCount = 1
			}
			if p.SpyCount > maxSpies {
				p.SpyCount = maxSpies
			}
			player.Name = p.Name
			player.Token = randID() + randID()
			player.Connected = true
			player.Alive = true
			code := randCode()
			room = &Room{Code: code, Players: []*Player{player}, HostID: player.ID, State: "lobby", SpyCount: p.SpyCount}
			roomsMu.Lock()
			rooms[code] = room
			roomsMu.Unlock()
			player.send(enc("room_joined", map[string]interface{}{
				"code":      code,
				"your_id":   player.ID,
				"host_id":   player.ID,
				"players":   toInfoList(room.Players),
				"token":     player.Token,
				"spy_count": p.SpyCount,
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
			player.Token = randID() + randID()
			player.Connected = true
			player.Alive = true
			r.Players = append(r.Players, player)
			snap := make([]*Player, len(r.Players))
			copy(snap, r.Players)
			hostID := r.HostID
			spyCount := r.SpyCount
			r.mu.Unlock()
			room = r

			infos := toInfoList(snap)
			player.send(enc("room_joined", map[string]interface{}{
				"code":      r.Code,
				"your_id":   player.ID,
				"host_id":   hostID,
				"players":   infos,
				"token":     player.Token,
				"spy_count": spyCount,
			}))
			for _, pl := range snap {
				if pl.ID != player.ID {
					pl.send(enc("player_list", map[string]interface{}{
						"players": infos,
						"host_id": hostID,
					}))
				}
			}

		case "reconnect":
			var p struct {
				Code  string `json:"code"`
				Token string `json:"token"`
				Name  string `json:"name"`
			}
			json.Unmarshal(msg.Payload, &p)
			if p.Code == "" || p.Token == "" {
				player.send(enc("error", map[string]string{"message": "Сессия не найдена"}))
				continue
			}
			roomsMu.Lock()
			r, ok := rooms[p.Code]
			roomsMu.Unlock()
			if !ok {
				player.send(enc("error", map[string]string{"message": "Сессия не найдена"}))
				continue
			}
			r.mu.Lock()
			var existing *Player
			for _, pl := range r.Players {
				if pl.Token == p.Token && pl.Name == p.Name {
					existing = pl
					break
				}
			}
			if existing == nil {
				r.mu.Unlock()
				player.send(enc("error", map[string]string{"message": "Сессия не найдена"}))
				continue
			}
			if existing.graceTimer != nil {
				existing.graceTimer.Stop()
				existing.graceTimer = nil
			}
			existing.mu.Lock()
			existing.Conn = conn
			existing.mu.Unlock()
			existing.Connected = true

			// Swap the loop-local player to the existing slot so subsequent
			// messages on this socket (and the disconnect defer) act on it.
			player = existing
			room = r

			snap := make([]*Player, len(r.Players))
			copy(snap, r.Players)
			hostID := r.HostID
			state := r.State
			spyCount := r.SpyCount
			role := "civilian"
			var locVal interface{}
			if existing.IsSpy {
				role = "spy"
				locVal = nil
			} else if state != "lobby" {
				locVal = r.Location
			}
			alive := existing.Alive
			var accusePayload map[string]interface{}
			if r.Accuse != nil && state == "voting" {
				accuserName, targetName := "", ""
				for _, pl := range r.Players {
					if pl.ID == r.Accuse.AccuserID {
						accuserName = pl.Name
					}
					if pl.ID == r.Accuse.TargetID {
						targetName = pl.Name
					}
				}
				_, alreadyVoted := r.Accuse.Votes[existing.ID]
				voteLeft := int(time.Until(r.Accuse.EndsAt).Round(time.Second) / time.Second)
				if voteLeft < 0 {
					voteLeft = 0
				}
				accusePayload = map[string]interface{}{
					"accuser_id":    r.Accuse.AccuserID,
					"accuser_name":  accuserName,
					"target_id":     r.Accuse.TargetID,
					"target_name":   targetName,
					"already_voted": alreadyVoted,
					"vote_seconds":  voteLeft,
				}
			}
			var gameOverPayload map[string]interface{}
			if state == "over" {
				gameOverPayload = r.LastGameOver
			}
			r.mu.Unlock()

			existing.send(enc("reconnected", map[string]interface{}{
				"code":      r.Code,
				"your_id":   existing.ID,
				"host_id":   hostID,
				"state":     state,
				"players":   toInfoList(snap),
				"role":      role,
				"location":  locVal,
				"locations": allLocations,
				"alive":     alive,
				"spy_count": spyCount,
				"accuse":    accusePayload,
				"game_over": gameOverPayload,
				"token":     existing.Token,
			}))
			for _, pl := range snap {
				if pl.ID != existing.ID {
					pl.send(enc("player_list", map[string]interface{}{
						"players": toInfoList(snap),
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
			sc := room.SpyCount
			room.mu.Unlock()
			if !isHost {
				player.send(enc("error", map[string]string{"message": "Только хост может начать игру"}))
				continue
			}
			if n < sc+2 {
				player.send(enc("error", map[string]string{"message": "Нужно минимум 2 мирных игрока"}))
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
			room.handleSpyGuess(player.ID, p.Location)
		}
	}
}

func main() {
	http.Handle("/", http.FileServer(http.Dir("static")))
	http.HandleFunc("/ws", handleWS)
	log.Println("Сервер запущен на http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
