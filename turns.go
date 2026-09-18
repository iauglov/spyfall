package main

import "time"

// Turn IDs distinguish confirmations from earlier questions, including after reconnect.
type TurnState struct {
	ID         uint64          `json:"id"`
	AskerID    string          `json:"asker_id"`
	AnswererID string          `json:"answerer_id"`
	Confirmed  map[string]bool `json:"confirmed"`
}

type RoundVoteState struct {
	ID     uint64
	Votes  map[string]string
	EndsAt time.Time
	timer  *time.Timer
}

// These helpers require r.mu. Network writes always happen after unlocking.
func (r *Room) livingPlayerLocked(id string) *Player {
	for _, p := range r.Players {
		if p.ID == id && p.Alive {
			return p
		}
	}
	return nil
}

func (r *Room) beginRoundLocked() {
	r.Round++
	r.State = "playing"
	r.RoundVote = nil
	r.TurnOrder = nil
	for _, p := range r.Players {
		if p.Alive {
			r.TurnOrder = append(r.TurnOrder, p.ID)
		}
	}
	r.TurnIndex = 0
	r.Turn = nil
	r.prepareTurnLocked()
}

func (r *Room) prepareTurnLocked() {
	for r.TurnIndex < len(r.TurnOrder) {
		askerID := r.TurnOrder[r.TurnIndex]
		if r.livingPlayerLocked(askerID) == nil {
			r.TurnIndex++
			continue
		}
		answererID := ""
		for offset := 1; offset < len(r.TurnOrder); offset++ {
			id := r.TurnOrder[(r.TurnIndex+offset)%len(r.TurnOrder)]
			if r.livingPlayerLocked(id) != nil {
				answererID = id
				break
			}
		}
		if answererID == "" {
			r.Turn = nil
			return
		}
		if r.Turn == nil || r.Turn.AskerID != askerID || r.Turn.AnswererID != answererID {
			r.turnSequence++
			r.Turn = &TurnState{ID: r.turnSequence, AskerID: askerID, AnswererID: answererID, Confirmed: make(map[string]bool)}
		}
		return
	}
	r.Turn = nil
	r.State = "voting"
	r.turnSequence++
	vote := &RoundVoteState{ID: r.turnSequence, Votes: make(map[string]string), EndsAt: time.Now().Add(voteDuration)}
	r.RoundVote = vote
	vote.timer = time.AfterFunc(voteDuration, func() { r.finalizeRoundVote(vote) })
}

func (r *Room) confirmTurn(playerID string, turnID uint64) {
	r.mu.Lock()
	turn := r.Turn
	player := r.livingPlayerLocked(playerID)
	if r.State != "playing" || turn == nil || turn.ID != turnID || player == nil || !player.Connected ||
		(playerID != turn.AskerID && playerID != turn.AnswererID) || turn.Confirmed[playerID] {
		r.mu.Unlock()
		return
	}
	turn.Confirmed[playerID] = true
	if turn.Confirmed[turn.AskerID] && turn.Confirmed[turn.AnswererID] {
		r.TurnIndex++
		r.Turn = nil
		r.prepareTurnLocked()
	}
	r.mu.Unlock()
	r.broadcastRoundState()
}

// Temporary disconnections keep the question and acknowledgements intact.
// Death or removal after the grace period replaces only affected participants.
func (r *Room) syncTurn() {
	r.mu.Lock()
	if r.State == "playing" && r.Round > 0 {
		r.prepareTurnLocked()
	}
	vote := r.RoundVote
	r.mu.Unlock()
	r.broadcastRoundState()
	if vote != nil {
		r.finishRoundVoteIfReady(vote)
	}
}

func (r *Room) roundSnapshotLocked() map[string]interface{} {
	var votePayload interface{}
	if vote := r.RoundVote; vote != nil {
		seconds := int(time.Until(vote.EndsAt).Round(time.Second) / time.Second)
		if seconds < 0 {
			seconds = 0
		}
		voted := make([]string, 0, len(vote.Votes))
		// Do not disclose anyone's choice before the vote ends.
		for id := range vote.Votes {
			voted = append(voted, id)
		}
		votePayload = map[string]interface{}{"id": vote.ID, "voted": voted, "vote_seconds": seconds}
	}
	return map[string]interface{}{
		"revision": r.roundRevision, "state": r.State, "round": r.Round,
		"turn": r.Turn, "vote": votePayload, "players": toInfoList(r.Players),
	}
}

func (r *Room) broadcastRoundState() {
	r.mu.Lock()
	r.roundRevision++
	msg := enc("round_state", r.roundSnapshotLocked())
	r.mu.Unlock()
	r.broadcast(msg)
}

func (r *Room) submitRoundVote(voterID, targetID string, voteID uint64) {
	r.mu.Lock()
	vote := r.RoundVote
	voter := r.livingPlayerLocked(voterID)
	if r.State != "voting" || vote == nil || vote.ID != voteID || voter == nil || !voter.Connected ||
		time.Now().After(vote.EndsAt) || (targetID != "" && r.livingPlayerLocked(targetID) == nil) {
		r.mu.Unlock()
		return
	}
	if _, voted := vote.Votes[voterID]; voted {
		r.mu.Unlock()
		return
	}
	vote.Votes[voterID] = targetID
	r.mu.Unlock()
	r.broadcastRoundState()
	r.finishRoundVoteIfReady(vote)
}

func (r *Room) finishRoundVoteIfReady(vote *RoundVoteState) {
	r.mu.Lock()
	ready := r.State == "voting" && r.RoundVote == vote
	if ready {
		for _, p := range r.Players {
			if p.Alive {
				if _, voted := vote.Votes[p.ID]; !voted {
					ready = false
					break
				}
			}
		}
	}
	r.mu.Unlock()
	if ready {
		r.finalizeRoundVote(vote)
	}
}

func (r *Room) finalizeRoundVote(vote *RoundVoteState) {
	r.mu.Lock()
	if r.State != "voting" || vote == nil || r.RoundVote != vote {
		r.mu.Unlock()
		return
	}
	if vote.timer != nil {
		vote.timer.Stop()
	}
	counts := make(map[string]int)
	eligible := 0
	for _, p := range r.Players {
		if !p.Alive {
			continue
		}
		eligible++
		targetID := vote.Votes[p.ID]
		if r.livingPlayerLocked(targetID) != nil {
			counts[targetID]++
		}
	}
	targetID, targetName := "", ""
	targetIsSpy := false
	for id, count := range counts {
		if count > eligible/2 {
			target := r.livingPlayerLocked(id)
			targetID, targetName, targetIsSpy = id, target.Name, target.IsSpy
			target.Alive = false
			break
		}
	}
	result := enc("round_vote_result", map[string]interface{}{
		"round": r.Round, "vote_id": vote.ID, "target_id": targetID, "target_name": targetName,
		"target_is_spy": targetIsSpy, "counts": counts, "players": toInfoList(r.Players),
	})
	r.RoundVote = nil
	reason := ""
	if r.aliveSpies() == 0 {
		reason = "all_spies_caught"
	} else if r.aliveCivilians() == 0 {
		reason = "all_civilians_dead"
	}
	if reason == "" {
		r.beginRoundLocked()
	}
	r.mu.Unlock()
	r.broadcast(result)
	if reason != "" {
		r.endGame(reason, reason == "all_civilians_dead")
	} else {
		r.broadcastRoundState()
	}
}
