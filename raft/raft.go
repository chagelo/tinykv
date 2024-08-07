// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"crypto/rand"
	"errors"
	"math/big"
	"sort"
	"sync"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu sync.Mutex
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v, _ := rand.Int(rand.Reader, big.NewInt(int64(n)))
	r.mu.Unlock()
	return int(v.Int64())
}

var globalRand = &lockedRand{}

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64

	Logger *log.Logger
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	if c.Logger == nil {
		c.Logger = log.New()
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type stepFunc func(r *Raft, m pb.Message) error

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	tick func()
	step stepFunc

	logger *log.Logger
	peers  []uint64
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	state, confstate, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	if c.peers == nil {
		c.peers = confstate.Nodes
	}
	raftlog := newLog(c.Storage)

	// Your Code Here (2A).
	raft := &Raft{
		id:      c.ID,
		Term:    state.Term,
		Vote:    state.Vote,
		State:   StateFollower,
		Lead:    None,
		RaftLog: raftlog,

		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		logger:           c.Logger,
		step:             stepFollower,
	}

	lastIndex := raft.RaftLog.LastIndex()
	for _, peer := range c.peers {
		raft.Prs[peer] = &Progress{Next: lastIndex + 1, Match: 0}
	}

	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}

	raft.tick = raft.electionTiker

	// error, because the hardstate may contain vote and lead info
	// raft.becomeFollower(raft.Term, None)
	return raft
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.trk.
func (r *Raft) bcastAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}
}

func (r *Raft) sendSnapshot(to uint64) bool {
	var snapshot pb.Snapshot
	var err error
	if !IsEmptySnap(r.RaftLog.pendingSnapshot) {
		snapshot = *r.RaftLog.pendingSnapshot // 挂起的还未处理的快照
	} else {
		snapshot, err = r.RaftLog.storage.Snapshot() // 生成一份快照
	}

	if err != nil {
		return false
	}

	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		Term:     r.Term,
		Snapshot: &snapshot,
		To:       to,
		From:     r.id,
	}
	r.msgs = append(r.msgs, msg)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
	return true
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		// return false
		panic("Only leader can send Append msg")
	}

	// send snapshot
	firstIndex := r.RaftLog.FirstIndex()
	if r.Prs[to].Next < firstIndex {
		return r.sendSnapshot(to)
	}

	// next > last index or < first index
	term, err := r.RaftLog.Term(r.Prs[to].Next - 1)
	if err != nil {
		return r.sendSnapshot(to)
	}

	entries := make([]*pb.Entry, 0)

	for i := r.Prs[to].Next; i <= r.RaftLog.LastIndex(); i++ {
		entries = append(entries, &r.RaftLog.entries[i-firstIndex])
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: term,
		Index:   r.Prs[to].Next - 1,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) bcastHeartbeat() {
	r.logger.Infof("[Peer %d term: %d], Hearbeat broadcast begin", r.id, r.Term)
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}

		r.logger.Infof("[Peer %d term: %d], Hearbeat broadcast send to %d", r.id, r.Term, peer)

		r.msgs = append(r.msgs, pb.Message{
			From: r.id,
			To:   peer,
			Term: r.Term,

			Commit: r.RaftLog.committed,

			MsgType: pb.MessageType_MsgHeartbeat,
		})
	}
}

// tick advances the internal logical clock by a single tick.
// set ticker to a member of raft struct
// func (r *Raft) tick() {
// 	// Your Code Here (2A).
// 	switch r.State {
// 	case StateCandidate:
// 		r.electionTiker()
// 	case StateLeader:
// 		r.heartbeatTiker()
// 	case StateFollower:
// 		panic("Current peer is follower, tick() do nothing")
// 	}
// }

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *Raft) reset(term uint64) {
	if r.Term > term {
		panic("Can not reset to smaller term")
	}

	r.Term = term
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
}

func (r *Raft) resetVotes() {
	r.votes = map[uint64]bool{}
	r.Vote = None
}

func (r *Raft) resetPrs() {

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// candidate -> follower, find leader or high term
	// leader -> follower, find higher term

	r.reset(term)
	r.resetVotes()
	r.step = stepFollower
	r.Lead = lead
	// r.logger.Infof("[Peer %d Term %d], %v became follower", r.id, r.Term, r.State)
	r.State = StateFollower
	r.tick = r.electionTiker
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		panic("Leader cannot convert to Candidate")
	}

	r.reset(r.Term)
	r.step = stepCandidate
	r.Term++
	r.Vote = r.id
	r.votes[r.id] = true

	// r.logger.Infof("[Peer %d Term %d], %v became Candidate", r.id, r.Term, r.State)

	r.State = StateCandidate
	r.tick = r.electionTiker
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("Cannot convert Follower to Leader")
	}

	r.reset(r.Term)
	r.step = stepLeader
	r.Lead = r.id
	r.tick = r.heartbeatTiker
	r.State = StateLeader

	// 更新每个节点的日志信息，最大匹配和下一条要发送的日志
	lastIndex := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		r.Prs[peer].Next = lastIndex + 1
		r.Prs[peer].Match = lastIndex
	}

	// noop entry?
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
	if len(r.Prs) == 1 {
		r.RaftLog.committed++
	}

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

	r.bcastAppend()

	r.logger.Infof("[Peer %d Term %d], Candidate became Leader", r.id, r.Term)
}

func stepFollower(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()

	case pb.MessageType_MsgAppend:
		r.Lead = m.From
		r.electionElapsed = 0
		r.handleAppendEntries(m)

	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)

	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)

	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)
	}
	return nil
}

func stepCandidate(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()

	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleAppendEntries(m)

	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleHeartbeat(m)

	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)

	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	}
	return nil
}

func stepLeader(r *Raft, m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()

	case pb.MessageType_MsgPropose:
		// MsgPropose with empty entries
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MsgPropose", r.id)
		}

		// If we are not currently a member of the range (i.e. this node
		// was removed from the configuration while serving as leader),
		// drop any new proposals.
		if r.Prs[r.id] == nil {
			return ErrProposalDropped
		}

		r.handlePropose(m)

	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)

	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)

	case pb.MessageType_MsgRequestVote:
		r.msgs = append(r.msgs, pb.Message{
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
			MsgType: pb.MessageType_MsgRequestVoteResponse,
		})
	}
	return nil
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		// Already vote to others, but receive a message from another candidate
		// if m.MsgType == pb.MessageType_MsgRequestVote {
		// 	inLease := r.Lead != None && r.electionElapsed < r.electionTimeout
		// 	if inLease {
		// 		// If a server receives a RequestVote request within the minimum election timeout
		// 		// of hearing from a current leader, it does not update its term or grant its vote
		// 		// r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
		// 		// 	r.id, last.term, last.index, r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
		// 		r.logger.Infof("[Peer %d term: %d] ignored %s from %x : lease is not expired (remaining ticks: %d)",
		// 			r.id, r.Term, m.MsgType, m.From, r.electionTimeout-r.electionElapsed)
		// 		return nil
		// 	}
		// }
		r.logger.Infof("[Peer %d term: %d] received a %s message with higher term from [Peer %d term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat {
			r.becomeFollower(m.Term, m.From)
		} else {
			// if r.lead != None but election timeout not regrantedvote
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
		if m.MsgType == pb.MessageType_MsgAppend {
			r.logger.Infof("[Peer %d term: %d], Reject a %s message with lower term from [Peer %d term: %d]",
				r.id, r.Term, m.MsgType, m.From, m.Term)

			r.msgs = append(r.msgs, pb.Message{From: r.id, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse})
		} else if m.MsgType == pb.MessageType_MsgRequestVote {
			r.logger.Infof("[Peer %d term: %d], Reject a %s message with lower term from [Peer %d term: %d]",
				r.id, r.Term, m.MsgType, m.From, m.Term)
			r.msgs = append(r.msgs, pb.Message{From: r.id, To: m.From, Term: r.Term, Reject: true, MsgType: pb.MessageType_MsgRequestVoteResponse})
		} else {
			r.logger.Infof("[Peer %d term: %d], Ignore a %s message with lower term from [Peer %d term: %d]",
				r.id, r.Term, m.MsgType, m.From, m.Term)
		}
		return nil
	}

	err := r.step(r, m)
	return err
}

// r.state must be Follower or Candidate
// When step, handle with MsgHup
func (r *Raft) startElection() {
	if r.State == StateLeader {
		log.Infof("[Peer %d term: %d], Peer has been leader, but receive %s, do nothing", r.id, r.Term, r.State)
		return
	}

	r.becomeCandidate()

	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	r.Vote = r.id
	r.votes[r.id] = true

	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)

	for peer := range r.Prs {
		if peer == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			From:    r.id,
			To:      peer,
			Term:    r.Term,
			Index:   lastIndex,
			LogTerm: lastTerm,
			MsgType: pb.MessageType_MsgRequestVote,
		})
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	li := r.RaftLog.LastIndex()
	for i, e := range m.Entries {
		entry := pb.Entry{
			Term:      r.Term,
			Index:     li + 1 + uint64(i),
			Data:      e.Data,
			EntryType: e.EntryType, // EntryType is confchange or normal
		}

		r.RaftLog.entries = append(r.RaftLog.entries, entry)

		// BUG: do not update log replicate progress of other node
		// until receive the append response
		// r.Prs[r.id].Match = r.RaftLog.LastIndex()
		// r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	}

	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	if len(r.Prs) == 1 {
		r.RaftLog.committed += uint64(len(m.Entries))

		if r.RaftLog.committed > r.Prs[r.id].Match {
			r.Prs[r.id].Match = r.RaftLog.committed
			r.Prs[r.id].Next = r.RaftLog.committed + 1

		}

		return
	}

	r.bcastAppend()
}

// Follower or Candidate to Candidate
func (r *Raft) handleRequestVoteResponse(m pb.Message) {

	r.votes[m.From] = !m.Reject
	voteCnt := 0
	denyVote := 0
	for k := range r.votes {
		if r.votes[k] {
			voteCnt += 1
		} else {
			denyVote += 1
		}
	}

	if voteCnt > len(r.Prs)/2 {
		r.becomeLeader()
	} else if denyVote > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}

// Candidate to Follower or Candidate
func (r *Raft) handleRequestVote(m pb.Message) {
	canVote := true

	// TODO: log check is up-to-date
	if r.Lead != None || (r.Lead == None && r.Vote != None && r.Vote != m.From) {
		canVote = false
	}

	if canVote {
		canVote = r.RaftLog.isMoreUptoDate(m.Index, m.LogTerm)
	}

	if canVote {
		r.Vote = m.From
		r.electionElapsed = 0
	}

	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  !canVote,
		MsgType: pb.MessageType_MsgRequestVoteResponse,
	})
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {

	if r.Lead != None && r.Lead != m.From {
		r.logger.Panicf("[Peer %d term: %d], Received a %s message from another Leader with [Peer %d term: %d]", r.id, r.Term, m.MsgType, m.From, m.Term)
	}
	if r.State != StateFollower {
		r.logger.Panicf("[Peer %d term: %d], Received a %s message from Leader with [Peer %d term: %d], but current state is %s", r.id, r.Term, m.MsgType, m.From, m.Term, r.State)
	}

	// m.Index + 1 may not equal m.Entries[0].Index in some corner testcase
	if len(m.Entries) > 0 && m.Index+1 != m.Entries[0].Index {
		r.logger.Panicf("[Peer %d term: %d], Received a %s, however index of first entry is not equal to m.Index minus 1", r.id, r.Term, m.MsgType)
	}

	// m.LogTerm, m.Index 是 leader 中当前 follower progress
	// 的匹配的最后一条日志的 term 和 index
	term, err := r.RaftLog.Term(m.Index)
	if err != nil {
		// 比如当前节点日志很久，很多日志都没有
		// 告知 leader，当前节点的最大日志 id
		r.msgs = append(r.msgs, pb.Message{
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Index:   r.RaftLog.committed,
			Reject:  true,
			MsgType: pb.MessageType_MsgAppendResponse,
		})
		return
	}

	// 截断
	// |--- raftlog ---|
	// 			|--- msg ---|

	// |--- raftlog ---|
	// 		|- msg -|

	// Hint: it's impossible that m.Index != m.Entries[0].Index
	if term == m.LogTerm {
		if len(m.Entries) != 0 {

			for _, en := range m.Entries {
				index := en.Index
				oldTerm, err := r.RaftLog.Term(index)
				if index-r.RaftLog.FirstIndex() > uint64(len(r.RaftLog.entries)) || index > r.RaftLog.LastIndex() {
					r.RaftLog.entries = append(r.RaftLog.entries, *en)
				} else if oldTerm != en.Term || err != nil {
					// 不匹配，删除从此往后的所有条目
					if index < r.RaftLog.FirstIndex() {
						r.RaftLog.entries = make([]pb.Entry, 0)
					} else {
						r.RaftLog.entries = r.RaftLog.entries[0 : index-r.RaftLog.FirstIndex()]
					}
					// 更新stable
					r.RaftLog.stabled = min(r.RaftLog.stabled, index-1)
					// 追加新条目
					r.RaftLog.entries = append(r.RaftLog.entries, *en)

				}
			}

		}

		// r.RaftLog.stabled = m.Entries[0].Index - 1

		lastIndex := m.Index + uint64(len(m.Entries))
		// m.Commit equals to Leader's commitIndex
		r.RaftLog.committed = min(lastIndex, m.Commit)

		r.msgs = append(r.msgs, pb.Message{
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Index:   lastIndex,
			MsgType: pb.MessageType_MsgAppendResponse,
		})
		return
	}

	matchedIdx := m.Index
	for ; ; matchedIdx-- {
		if matchedIdx <= 0 {
			break
		}
		newterm, err := r.RaftLog.Term(matchedIdx)
		if err != nil {
			panic(err)
		}
		if newterm != term {
			break
		}
	}

	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   matchedIdx,
		Reject:  true,
		MsgType: pb.MessageType_MsgAppendResponse,
	})

}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Reject {
		r.Prs[m.From].Next = m.Index + 1
		r.sendAppend(m.From)
	} else {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1

		matchInts := make([]uint64, 0)

		for id, p := range r.Prs {
			if id == r.id {
				continue
			}
			matchInts = append(matchInts, p.Match)
		}

		sort.Slice(matchInts, func(i, j int) bool {
			return matchInts[i] > matchInts[j]
		})

		// desc order,
		// [1, 2, 3, 4, cur], 4 -> 3 -> 2
		// [1, 2, 3, cur] 3 -> 2 -> 1

		// 具有当前日志的 term 被写到大多数节点
		if r.RaftLog.maybeCommit(matchInts[(len(matchInts)+1)/2-1], r.Term) {
			// once a follower learns that a log entry is committed,
			// it applies the entry to its local state machine (in log order).
			// Reference: section 5.3
			// FIXME:r.RaftLog.storage.apply

			r.RaftLog.committed = matchInts[(len(matchInts)+1)/2-1]

			if r.RaftLog.committed > r.Prs[r.id].Match {
				r.Prs[r.id].Match = r.RaftLog.committed
				r.Prs[r.id].Next = r.RaftLog.committed + 1
			}

			// it's necessary to broadcast to everyone else
			r.bcastAppend()
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
// Heartbeat from leader
// 1. have two leader
// 2.
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// TODO: ready to modify log related sign
	r.logger.Infof("[Peer %d term: %d], Received a Heartbeat message from %x [term: %d]", r.id, r.Term, m.From, m.Term)
	if r.Lead != None {
		if r.Lead != r.id && r.Lead != m.From {
			panic("There are two Leader at the same time")
		}

		r.logger.Infof("Notify, peer %d already has vote to a Leader %d", r.id, r.Lead)
	}

	r.msgs = append(r.msgs, pb.Message{
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	})
	r.logger.Infof("[Peer %d term: %d], Reply a HeartbeatResponse message to %x [term: %d]", r.id, r.Term, m.From, m.Term)
}

// handleHeartbeat handle Heartbeat RPC request response
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// Your Code Here (2A).

	if m.Commit < r.RaftLog.committed {
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata

	if meta.Index <= r.RaftLog.committed {
		return
	}

	if meta.Index+1 < r.RaftLog.FirstIndex() {
		r.logger.Panicf("[Peer %d term: %d], receive %s message from %x [term: %d] snapshot lastIndex + 1 < raftlog firstIndex", r.id, r.Term, m.MsgType, m.From, m.Term)
	}

	r.Lead = m.From
	r.RaftLog.applied = m.Snapshot.Metadata.Index
	r.RaftLog.committed = m.Snapshot.Metadata.Index
	r.RaftLog.stabled = m.Snapshot.Metadata.Index
	r.Term = m.Term

	if len(r.RaftLog.entries) > 0 {
		if meta.Index > r.RaftLog.LastIndex() {
			r.RaftLog.entries = nil
		} else if meta.Index >= r.RaftLog.FirstIndex() {
			r.RaftLog.entries = r.RaftLog.entries[meta.Index-r.RaftLog.FirstIndex():]
		}
	}

	nds := m.Snapshot.Metadata.ConfState.Nodes

	r.Prs = make(map[uint64]*Progress)
	for _, nd := range nds {
		r.Prs[nd] = &Progress{Match: 0, Next: r.RaftLog.LastIndex() + 1}
	}

	r.RaftLog.pendingSnapshot = m.Snapshot
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// current peer is candidate or follower
func (r *Raft) electionTiker() {
	r.electionElapsed++

	if r.electionElapsed >= r.randomizedElectionTimeout {
		err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id})
		if err != nil {
			r.logger.Infof("Error occurred during election: %v", err)
			return
		}
	}
}

func (r *Raft) heartbeatTiker() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		// r.logger.Infof("Error, in leader election timeout first that heartbeat")
		// r.electionElapsed = 0
		// panic("Maybe network error")
	}

	if r.State != StateLeader {
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id, To: r.id}); err != nil {
			r.logger.Infof("Error when sending heartbeat start notify to leader: %v", err)
		}
	}

}
