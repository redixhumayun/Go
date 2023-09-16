package main

import (
	"fmt"
	"sync"
)

type Message string

const (
	Prepare    Message = "PREPARE"
	VoteCommit         = "VOTE_COMMIT"
	VoteAbort          = "VOTE_ABORT"
	Commit             = "COMMIT"
	Abort              = "ABORT"
)

type Coordinator struct {
	participants []*Participant
}

type Participant struct {
	index     int
	data      map[string]string
	locks     map[string]bool
	canCommit bool
}

func NewParticipant(index int) *Participant {
	return &Participant{
		index:     index,
		data:      make(map[string]string),
		locks:     make(map[string]bool),
		canCommit: true,
	}
}

func (c *Coordinator) initiateTransaction(oldValue string, newValue string) {
	// Phase 1: Prepare Phase
	for _, participant := range c.participants {
		msg := participant.prepare(oldValue, newValue)
		if msg == VoteAbort {
			c.abortTransaction(oldValue, newValue)
			return
		}
	}

	// Phase 2: Commit Phase
	c.commitTransaction(oldValue, newValue)
}

func (p *Participant) prepare(oldValue string, newValue string) Message {
	//	check if this participant's data contains the oldValue and lock the value if not already locked
	if _, exists := p.data[oldValue]; exists {
		if _, locked := p.locks[oldValue]; locked {
			return VoteAbort
		}
		p.locks[oldValue] = true
		return VoteCommit
	}
	return VoteCommit
}

func (c *Coordinator) commitTransaction(oldValue string, newValue string) {
	for _, p := range c.participants {
		p.commit(oldValue, newValue)
	}
	fmt.Println("Transaction Committed")
}

func (c *Coordinator) abortTransaction(oldValue string, newValue string) {
	for _, p := range c.participants {
		p.abort(oldValue, newValue)
	}
	fmt.Println("Transaction Aborted")
}

func (p *Participant) commit(oldValue string, newValue string) {
	p.data[newValue] = newValue
	if _, exists := p.data[oldValue]; exists {
		fmt.Printf("Deleted the old value of %s from participant %d\n", oldValue, p.index)
		delete(p.data, oldValue)
	}
	if _, locked := p.locks[oldValue]; locked {
		delete(p.locks, oldValue)
	}
	fmt.Println("Participant Committed")
}

func (p *Participant) abort(oldValue string, newValue string) {
	if _, locked := p.locks[oldValue]; locked {
		delete(p.locks, oldValue)
	}
	fmt.Println("Participant Aborted")
}

func main() {
	data := []string{"apple", "banana", "cherry", "date", "fig"}
	participants := make([]*Participant, len(data))
	for i, value := range data {
		participants[i] = NewParticipant(i)
		participants[i].data[value] = value
	}

	coordinator := &Coordinator{
		participants: participants,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		coordinator.initiateTransaction("apple", "APPLE")
	}()
	wg.Wait()
}
