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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	// TODO: what's this mean
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	totalEntries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}

	hardState, _, _ := storage.InitialState()

	return &RaftLog{
		storage:   storage,
		committed: hardState.Commit,

		applied:         firstIndex - 1,
		stabled:         lastIndex,
		entries:         totalEntries,
		pendingSnapshot: nil,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
// discard raft log
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	remainedIndex, _ := l.storage.FirstIndex()
	if len(l.entries) > 0 {
		// |--raftlogentry--|
		//                   |------storage------|
		if remainedIndex > l.LastIndex() {
			l.entries = nil
			// |----raftlogentry----|
			//           |------storage------|
		} else if remainedIndex >= l.FirstIndex() {
			l.entries = l.entries[remainedIndex-l.FirstIndex():]
		}
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return []pb.Entry{}
	}

	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled+1 < l.FirstIndex() {
		panic("Stable Index must be not less than raftlog firstIndex")
	}

	if l.stabled > l.LastIndex() {
		panic("Stable Index must be not greater than raftlog firstIndex")
	}

	if l.stabled == l.LastIndex() {
		return []pb.Entry{}
	}

	//
	// fisrt-------------stable-----------last
	offset := l.stabled + 1 - l.FirstIndex()

	return l.entries[offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.committed < l.applied {
		panic("CommitIndex < ApplyIndex")
	}
	if l.committed == l.applied {
		return
	}
	// |--- apply ---|----commit---|
	// l.appy must be greater to l.entries[0].Index
	// otherwise there might be some entry with index < l.entries[0].Index but index > l.appy
	offset := l.entries[0].Index
	ents = append(ents, l.entries[l.applied+1-offset:l.committed+1-offset]...)
	// TODO: compact entry with index <= l.apply
	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).'
	if len(l.entries) != 0 {
		return l.entries[0].Index + uint64(len(l.entries)) - 1
	}

	lastIndex, _ := l.storage.LastIndex()

	return lastIndex
}

//  apply|commited entries|stable entries|unstable entries

// Term return the term of the entry in the given index
// check the unstable entry first
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		firstIndex := l.FirstIndex()
		lastIndex := l.LastIndex()
		if i >= firstIndex && i <= lastIndex {
			return l.entries[i-firstIndex].Term, nil
		}
	}

	term, err := l.storage.Term(i)
	if err == nil {
		return term, nil
	}
	return 0, err
}

func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[0].Index
	}

	i, _ := l.storage.FirstIndex()
	return i
}

func (l *RaftLog) StorageFirstIndex() uint64 {
	i, _ := l.storage.FirstIndex()
	return i
}

func (l *RaftLog) matchTerm(idx, term uint64) bool {
	targeTerm, err := l.Term(idx)
	if err != nil {
		return false
	}
	return targeTerm == term
}

// findConflict finds the index of the conflict.
// It returns the first pair of conflicting entries between the existing
// entries and the given entries, if there are any.
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// If there is no conflicting entries, but the given entries contains new
// entries, the index of the first new entry will be returned.
// An entry is considered to be conflicting if it has the same index but
// a different term.
// The index of the given entries MUST be continuously increasing.
func (l *RaftLog) findConflict(ents []*pb.Entry) uint64 {
	for i := range ents {
		if !l.matchTerm(ents[i].Index, ents[i].Term) {
			return ents[i].Index
		}
	}
	return 0
}

// Only be called in StateLeader.
func (l *RaftLog) maybeCommit(commitIndex, commitTerm uint64) bool {
	if commitIndex != 0 && commitIndex > l.committed && l.matchTerm(commitIndex, commitTerm) {
		l.committed = commitIndex
		return true
	}
	return false
}

// If the logs have last entries with different terms, then the log with the
// later term is more up-to-date. If the logs end with the same term, then
// whichever log has the larger lastIndex is more up-to-date. If the logs are
// the same, the given log is up-to-date.
func (l *RaftLog) isMoreUptoDate(candidateIndex, candidateTerm uint64) bool {
	lastIndex := l.LastIndex()
	lastTerm, err := l.Term(lastIndex)
	if err != nil {
		panic(err)
	}

	if candidateTerm != lastTerm {
		return candidateTerm > lastTerm
	}
	return candidateIndex >= lastIndex
}


func (l *RaftLog) findConflictByTerm(index uint64, term uint64) (uint64, uint64) {
	for ; index > 0; index-- {
		// If there is an error (likely ErrCompacted or ErrUnavailable), we don't
		// know whether it's a match or not, so assume a possible match and return
		// the index, with 0 term indicating an unknown term.
		if ourTerm, err := l.Term(index); err != nil {
			return index, 0
		} else if ourTerm <= term {
			return index, ourTerm
		}
	}
	return 0, 0
}

func (l *RaftLog) commitTo(tocommit uint64, r *Raft) {
	// never decrease commit
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			r.logger.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex())
		}
		l.committed = tocommit
	}
}