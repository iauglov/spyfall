package main

import (
	"sync"
	"testing"
	"time"
)

func turnTestRoom(t *testing.T) *Room {
	t.Helper()
	r := &Room{State: "playing", Players: []*Player{
		{ID: "a", Name: "А", Alive: true, Connected: true},
		{ID: "b", Name: "Б", Alive: true, Connected: true},
		{ID: "c", Name: "В", Alive: true, Connected: true, IsSpy: true},
		{ID: "d", Name: "Г", Alive: true, Connected: true},
	}}
	r.beginRoundLocked()
	t.Cleanup(func() {
		if r.RoundVote != nil && r.RoundVote.timer != nil {
			r.RoundVote.timer.Stop()
		}
	})
	return r
}

func finishTestCircle(r *Room) {
	for r.State == "playing" && r.Turn != nil {
		turn := r.Turn
		r.confirmTurn(turn.AskerID, turn.ID)
		r.confirmTurn(turn.AnswererID, turn.ID)
	}
}

func TestTurnsRequireBothPlayersAndCompleteCircle(t *testing.T) {
	r := turnTestRoom(t)
	for i, pair := range [][2]string{{"a", "b"}, {"b", "c"}, {"c", "d"}, {"d", "a"}} {
		turn := r.Turn
		if turn == nil || turn.AskerID != pair[0] || turn.AnswererID != pair[1] {
			t.Fatalf("turn %d: got %+v, want %v", i, turn, pair)
		}
		r.confirmTurn("outsider", turn.ID)
		r.confirmTurn(pair[0], turn.ID-1)
		if len(r.Turn.Confirmed) != 0 {
			t.Fatal("accepted unauthorized or stale confirmation")
		}
		r.confirmTurn(pair[0], turn.ID)
		r.confirmTurn(pair[0], turn.ID)
		if r.Turn.ID != turn.ID {
			t.Fatal("advanced before both players confirmed")
		}
		r.confirmTurn(pair[1], turn.ID)
		if i < 3 && r.State != "playing" {
			t.Fatal("voted before circle completed")
		}
	}
	if r.State != "voting" || r.RoundVote == nil || r.Turn != nil {
		t.Fatal("full circle must start voting")
	}
}

func TestRoundVoteValidationMajorityAndNewCircle(t *testing.T) {
	r := turnTestRoom(t)
	finishTestCircle(r)
	id := r.RoundVote.ID
	r.submitRoundVote("outsider", "a", id)
	r.submitRoundVote("a", "missing", id)
	r.submitRoundVote("a", "b", id-1)
	if len(r.RoundVote.Votes) != 0 {
		t.Fatal("accepted invalid vote")
	}
	r.submitRoundVote("a", "b", id)
	r.submitRoundVote("a", "c", id)
	if r.RoundVote.Votes["a"] != "b" {
		t.Fatal("allowed double voting")
	}
	r.submitRoundVote("b", "b", id)
	r.submitRoundVote("c", "b", id)
	if r.State != "voting" {
		t.Fatal("finished before all voters")
	}
	r.submitRoundVote("d", "", id)
	if r.Players[1].Alive || r.State != "playing" || r.Round != 2 {
		t.Fatal("majority must eliminate and start next circle")
	}
	if r.Turn.AskerID != "a" || r.Turn.AnswererID != "c" {
		t.Fatal("next circle includes eliminated player")
	}
}

func TestRoundVoteTieAndTimeoutDoNotEliminate(t *testing.T) {
	r := turnTestRoom(t)
	finishTestCircle(r)
	old := r.RoundVote
	r.submitRoundVote("a", "b", old.ID)
	r.submitRoundVote("b", "a", old.ID)
	r.finalizeRoundVote(old)
	for _, p := range r.Players {
		if !p.Alive {
			t.Fatal("tie eliminated a player")
		}
	}
	if r.Round != 2 {
		t.Fatal("timeout must start another circle")
	}
	finishTestCircle(r)
	current := r.RoundVote
	r.finalizeRoundVote(old)
	if r.RoundVote != current {
		t.Fatal("old timer finalized new vote")
	}
}

func TestTurnSurvivesDisconnectAndSkipsEliminatedPlayers(t *testing.T) {
	r := turnTestRoom(t)
	id := r.Turn.ID
	r.confirmTurn("a", id)
	r.Players[1].Connected = false
	r.syncTurn()
	if r.Turn.ID != id || !r.Turn.Confirmed["a"] {
		t.Fatal("temporary disconnect reset turn")
	}
	r.Players[1].Alive = false
	r.syncTurn()
	if r.Turn.AskerID != "a" || r.Turn.AnswererID != "c" || len(r.Turn.Confirmed) != 0 {
		t.Fatal("eliminated respondent was not replaced")
	}
	r.confirmTurn("c", id)
	if len(r.Turn.Confirmed) != 0 {
		t.Fatal("old confirmation affected replacement turn")
	}
	r.Players[0].Alive = false
	r.syncTurn()
	if r.Turn.AskerID != "c" || r.Turn.AnswererID != "d" {
		t.Fatal("eliminated asker was not skipped")
	}
}

func TestRoundVoteCatchesLastSpy(t *testing.T) {
	r := turnTestRoom(t)
	finishTestCircle(r)
	id := r.RoundVote.ID
	for _, voter := range []string{"a", "b", "c", "d"} {
		r.submitRoundVote(voter, "c", id)
	}
	if r.State != "over" || r.LastGameOver["winner"] != "civilians" {
		t.Fatal("catching last spy did not end game")
	}
}

func TestExpiredVoteCannotAcceptLateBallot(t *testing.T) {
	r := turnTestRoom(t)
	finishTestCircle(r)
	r.RoundVote.EndsAt = time.Now().Add(-time.Second)
	r.submitRoundVote("a", "b", r.RoundVote.ID)
	if len(r.RoundVote.Votes) != 0 {
		t.Fatal("accepted vote after deadline")
	}
}

func TestConcurrentConfirmationsAdvanceOnlyOneQuestion(t *testing.T) {
	r := turnTestRoom(t)
	rID := r.Turn.ID
	var done sync.WaitGroup
	for i := 0; i < 20; i++ {
		for _, playerID := range []string{"a", "b"} {
			done.Add(1)
			go func(id string) { defer done.Done(); r.confirmTurn(id, rID) }(playerID)
		}
	}
	done.Wait()
	if r.Turn.AskerID != "b" || r.Turn.AnswererID != "c" || len(r.Turn.Confirmed) != 0 {
		t.Fatal("concurrent duplicate acknowledgements skipped or confirmed next question")
	}
}

func TestRemovalRepairsQuestionWithoutSkippingAsker(t *testing.T) {
	r := turnTestRoom(t)
	r.confirmTurn("a", r.Turn.ID)
	r.Players[1].Connected = false
	r.removeDisconnected("b")
	if r.Turn.AskerID != "a" || r.Turn.AnswererID != "c" || len(r.Turn.Confirmed) != 0 {
		t.Fatal("removing respondent did not repair the current question")
	}
}
