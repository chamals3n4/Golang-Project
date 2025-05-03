package raft

import (
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"mini_etcd/config"
	"mini_etcd/internal/transport"
)

// ------------------------------------------------------------
// Raft node state & constructor
// ------------------------------------------------------------

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string { return [...]string{"Follower", "Candidate", "Leader"}[s] }

type Node struct {
	mu sync.RWMutex

	// identity & topology
	id    string
	peers map[string]string // peerID -> addr

	// persistent state and caching variables
	currentTerm int
	votedFor    string
	db          *bolt.DB
	log         StableLog
	store       StableStore

	// volatile state
	commitIndex int
	lastApplied int

	// leader-only state
	nextIndex  map[string]int
	matchIndex map[string]int

	// runtime plumbing
	state          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan ApplyMsg
	stopCh         chan struct{}

	// transport
	trans *transport.HTTPTransport
}

func NewNode(id string, peers map[string]string, applyCh chan ApplyMsg, db *bolt.DB) *Node {
	n := &Node{
		id:         id,
		peers:      peers,
		db:         db,
		log:        NewBoltLog(db),
		store:      NewBoltStore(db),
		state:      Follower,
		applyCh:    applyCh,
		stopCh:     make(chan struct{}),
		nextIndex:  make(map[string]int),
		matchIndex: make(map[string]int),
	}

	n.currentTerm = n.store.Term()
	n.votedFor = n.store.VotedFor()
	n.lastApplied = n.store.LastApplied()
	n.commitIndex = n.lastApplied // Initialize commitIndex to lastApplied

	// Initialize nextIndex and matchIndex for all peers
	lastIdx := n.log.LastIndex()
	for pid := range peers {
		if pid != id {
			n.nextIndex[pid] = lastIdx + 1
			n.matchIndex[pid] = 0
		}
	}

	n.resetElectionTimer()
	n.trans = transport.New(n.handleInbound)
	return n
}

// ------------------------------------------------------------
// Public API
// ------------------------------------------------------------

func (n *Node) Serve(addr string) error {
	svr := &http.Server{Addr: addr, Handler: n.trans}
	go n.ticker()
	return svr.ListenAndServe()
}

func (n *Node) Start() {
	go n.ticker()
}

func (n *Node) Stop() {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	
	if n.stopCh != nil {
		close(n.stopCh)
		n.stopCh = nil
	}
	
	// Stop timers
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTimer != nil {
		n.heartbeatTimer.Stop()
	}
	
	// Ensure all state is persisted
	n.store.SetTerm(n.currentTerm)
	n.store.SetVotedFor(n.votedFor)
	n.store.SetLastApplied(n.lastApplied)
	
	// Close the database
	if n.db != nil {
		n.db.Close()
	}
}

// Propose replicates a command **only the leader**.
func (n *Node) Propose(cmd any) (idx int, ok bool) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return -1, false
	}
	idx = n.log.Append(LogEntry{Term: n.currentTerm, Command: cmd})

	// ---------- single-node fast commit ----------------
	if len(n.peers) == 0 {
		n.commitIndex = idx
		n.applyCommitted() // apply locally
		n.maybePrune()

		n.mu.Unlock()

		return idx, true
	}
	// ---------------------------------------------------

	n.mu.Unlock()

	go n.broadcastAppendEntries()
	return idx, true
}

// ------------------------------------------------------------
// Ticker goroutine – drives elections & heart-beats
// ------------------------------------------------------------

func (n *Node) ticker() {
	n.resetElectionTimer()
	n.startHeartbeatTimer()
	
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.electionTimer.C:
			n.mu.RLock()
			isLeader := n.state == Leader
			n.mu.RUnlock()

			if !isLeader {
				go n.startElection()
			}
			n.resetElectionTimer()
		case <-n.heartbeatTimerC():
			n.mu.RLock()
			leader := n.state == Leader
			n.mu.RUnlock()
			if leader {
				go n.broadcastAppendEntries()
			}
			n.heartbeatTimer.Reset(config.HeartbeatInterval)
		}
	}
}

func (n *Node) startHeartbeatTimer() {
	if n.heartbeatTimer == nil {
		n.heartbeatTimer = time.NewTimer(config.HeartbeatInterval)
	} else {
		n.heartbeatTimer.Reset(config.HeartbeatInterval)
	}
}

func (n *Node) heartbeatTimerC() <-chan time.Time {
	if n.heartbeatTimer == nil {
		n.heartbeatTimer = time.NewTimer(config.HeartbeatInterval)
		if !n.heartbeatTimer.Stop() {
			<-n.heartbeatTimer.C
		}
	}
	return n.heartbeatTimer.C
}

func (n *Node) resetElectionTimer() {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(randomElectionTimeout())
	} else {
		if !n.electionTimer.Stop() {
			select {
			case <-n.electionTimer.C:
			default:
			}
		}
		n.electionTimer.Reset(randomElectionTimeout())
	}
}

func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// ------------------------------------------------------------
// Elections
// ------------------------------------------------------------

func (n *Node) startElection() {
	n.mu.Lock()
	if n.state == Leader {
		n.mu.Unlock()
		return
	}
	
	// Step up to candidate & bump term
	n.state = Candidate
	n.currentTerm++
	n.store.SetTerm(n.currentTerm)
	n.votedFor = n.id
	n.store.SetVotedFor(n.id)
	
	term := n.currentTerm
	lastIdx, lastTerm := n.log.LastIndexTerm()
	n.resetElectionTimer()
	
	// Copy peers map so we can iterate after releasing the lock
	peerAddrs := make(map[string]string, len(n.peers))
	for id, addr := range n.peers {
		if id != n.id {
			peerAddrs[id] = addr
		}
	}
	n.mu.Unlock()
	
	// Vote counting
	voteCh := make(chan bool, len(peerAddrs)+1) // +1 for self vote
	voteCh <- true // self vote
	
	// Request votes from all peers
	for pid, paddr := range peerAddrs {
		go func(id, addr string) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID: n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			var reply RequestVoteReply
			
			if err := n.trans.Call(addr, transport.RPCRequestVote, &args, &reply); err != nil {
				voteCh <- false
				return
			}
			
			n.mu.Lock()
			if reply.Term > term {
				n.becomeFollower(reply.Term)
				n.mu.Unlock()
				voteCh <- false
				return
			}
			n.mu.Unlock()
			
			voteCh <- reply.VoteGranted && reply.Term == term
		}(pid, paddr)
	}
	
	// Count votes
	votes := 1 // self vote
	needed := len(n.peers)/2 + 1 // majority of all nodes (including self)
	timeout := time.After(config.ElectionTimeoutMax)
	
	for votes < needed {
		select {
		case granted := <-voteCh:
			if granted {
				votes++
				if votes >= needed {
					n.mu.Lock()
					if n.state == Candidate && n.currentTerm == term {
						n.becomeLeader()
					}
					n.mu.Unlock()
					return
				}
			}
		case <-timeout:
			n.mu.Lock()
			if n.state == Candidate {
				n.becomeFollower(n.currentTerm)
			}
			n.mu.Unlock()
			return
		case <-n.stopCh:
			return
		}
	}
}

// ------------------------------------------------------------
// Leader transition helpers
// ------------------------------------------------------------

func (n *Node) becomeFollower(term int) {
	n.state = Follower
	n.currentTerm = term
	n.store.SetTerm(term)
	n.votedFor = ""
	n.store.SetVotedFor("")
	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.votedFor = ""
	n.store.SetVotedFor("")
	
	// Initialize leader state
	lastIdx := n.log.LastIndex()
	for pid := range n.peers {
		if pid != n.id {
			n.nextIndex[pid] = lastIdx + 1
			n.matchIndex[pid] = 0
		}
	}
	
	// Stop election timer and start heartbeat timer
	if !n.electionTimer.Stop() {
		select {
		case <-n.electionTimer.C:
		default:
		}
	}
	n.startHeartbeatTimer()
	
	// Send initial empty AppendEntries
	go n.broadcastAppendEntries()
}

func (n *Node) broadcastAppendEntries() {
	n.mu.RLock()
	if n.state != Leader {
		n.mu.RUnlock()
		return
	}

	// Prepare common fields
	term := n.currentTerm
	commitIndex := n.commitIndex
	n.mu.RUnlock()

	// Send AppendEntries to each peer
	var wg sync.WaitGroup
	for pid, paddr := range n.peers {
		if pid == n.id {
			continue
		}
		wg.Add(1)
		go func(id, addr string) {
			defer wg.Done()
			for {
				n.mu.RLock()
				if n.state != Leader {
					n.mu.RUnlock()
					return
				}

				prevIndex := n.nextIndex[id] - 1
				var prevTerm int
				if prevIndex > 0 {
					if entry, ok := n.log.At(prevIndex); ok {
						prevTerm = entry.Term
					}
				}

				// Get entries to send
				entries := make([]LogEntry, 0)
				for i := n.nextIndex[id]; i <= n.log.LastIndex(); i++ {
					if entry, ok := n.log.At(i); ok {
						entries = append(entries, entry)
					}
				}
				n.mu.RUnlock()

				args := AppendEntriesArgs{
					Term:         term,
					LeaderID:     n.id,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  prevTerm,
					Entries:      entries,
					LeaderCommit: commitIndex,
				}

				var reply AppendEntriesReply
				if err := n.trans.Call(addr, transport.RPCAppendEntries, &args, &reply); err != nil {
					return
				}

				n.mu.Lock()
				if reply.Term > n.currentTerm {
					n.becomeFollower(reply.Term)
					n.mu.Unlock()
					return
				}

				if reply.Success {
					// Update matchIndex and nextIndex on success
					if len(entries) > 0 {
						n.matchIndex[id] = prevIndex + len(entries)
						n.nextIndex[id] = n.matchIndex[id] + 1

						// Try to advance commitIndex
						matches := make([]int, 0, len(n.peers))
						for _, match := range n.matchIndex {
							matches = append(matches, match)
						}
						matches = append(matches, n.log.LastIndex()) // leader's log
						
						// Sort in descending order
						sort.Sort(sort.Reverse(sort.IntSlice(matches)))
						
						// Get the index that has been replicated to a majority
						majorityIdx := matches[len(matches)/2]
						
						// Commit if the entry is from current term or if it's from a previous term
						// and all previous entries are committed
						if majorityIdx > n.commitIndex {
							if entry, ok := n.log.At(majorityIdx); ok {
								if entry.Term == n.currentTerm || 
								   (entry.Term < n.currentTerm && n.commitIndex >= entry.Index-1) {
									n.commitIndex = majorityIdx
									n.applyCommitted()
								}
							}
						}
					}
					n.mu.Unlock()
					return
				}

				// If AppendEntries fails, decrement nextIndex and retry
				if reply.ConflictIndex > 0 {
					n.nextIndex[id] = reply.ConflictIndex
				} else {
					n.nextIndex[id] = max(1, n.nextIndex[id]-1)
				}
				n.mu.Unlock()
			}
		}(pid, paddr)
	}
	wg.Wait()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (n *Node) onRequestVote(args *RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{Term: n.currentTerm}

	// 1. Reply false if term < currentTerm
	if args.Term < n.currentTerm {
		return reply
	}

	// If we see a newer term, update our term
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
		reply.Term = n.currentTerm
	}

	// 2. Reply false if we already voted for someone else in this term
	if n.votedFor != "" && n.votedFor != args.CandidateID && n.currentTerm == args.Term {
		return reply
	}

	// 3. Reply false if candidate's log is not at least as up-to-date as our log
	lastIdx, lastTerm := n.log.LastIndexTerm()
	if args.LastLogTerm < lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex < lastIdx) {
		return reply
	}

	// Grant vote
	n.votedFor = args.CandidateID
	n.store.SetVotedFor(args.CandidateID)
	reply.VoteGranted = true
	reply.Term = n.currentTerm
	
	// Reset election timer since we granted our vote
	n.resetElectionTimer()
	
	return reply
}

func (n *Node) onAppendEntries(args *AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{Term: n.currentTerm}

	// 1. Reply false if term < currentTerm
	if args.Term < n.currentTerm {
		reply.Success = false
		return reply
	}

	// 2. If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	// Reset election timer since we got a valid RPC
	n.resetElectionTimer()

	// 3. Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex > 0 {
		entry, ok := n.log.At(args.PrevLogIndex)
		if !ok || entry.Term != args.PrevLogTerm {
			reply.Success = false
			reply.ConflictIndex = args.PrevLogIndex
			return reply
		}
	}

	// 4. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	if len(args.Entries) > 0 {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)

		// Delete conflicting entries
		for i := args.PrevLogIndex + 1; i <= n.log.LastIndex(); i++ {
			entry, ok := n.log.At(i)
			if !ok || entry.Term != args.Entries[i-args.PrevLogIndex-1].Term {
				n.log.TruncateSuffix(i)
				break
			}
		}

		// Append any new entries not already in the log
		for i := args.PrevLogIndex + 1; i <= lastNewIndex; i++ {
			if i > n.log.LastIndex() {
				n.log.Append(args.Entries[i-args.PrevLogIndex-1])
			}
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > n.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		n.commitIndex = min_(args.LeaderCommit, lastNewIndex)
		n.applyCommitted()
	}

	reply.Success = true
	return reply
}

func (n *Node) Trans() http.Handler { return n.trans }

func min_(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		if entry, ok := n.log.At(n.lastApplied); ok {
			n.store.SetLastApplied(n.lastApplied)
			n.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:     entry.Command,
				CommandIndex: n.lastApplied,
			}
		}
	}
	n.maybePrune()
}

func (n *Node) maybePrune() {
	if n.commitIndex-n.log.FirstIndex() > config.PruneEvery {
		cutoff := n.commitIndex - config.RetainTail
		if cutoff > n.log.FirstIndex() {
			n.log.TruncateBefore(cutoff)
		}
	}
}

// ------------------------------------------------------------
// Inbound RPC handlers (HTTP callbacks)
// ------------------------------------------------------------

func (n *Node) handleInbound(method transport.RPC, body io.Reader, w http.ResponseWriter) {
	switch method {
	case transport.RPCRequestVote:
		var args RequestVoteArgs
		_ = json.NewDecoder(body).Decode(&args)
		transport.ReplyJSON(w, n.onRequestVote(&args))
	case transport.RPCAppendEntries:
		var args AppendEntriesArgs
		_ = json.NewDecoder(body).Decode(&args)
		transport.ReplyJSON(w, n.onAppendEntries(&args))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// ------------------------------------------------------------
// *Testing helpers* – read-only accessors use RLock
// ------------------------------------------------------------

func (n *Node) State() State {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

func (n *Node) ID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.id
}

func (n *Node) LastApplied() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastApplied
}

func (n *Node) Log() StableLog { return n.log }

func (n *Node) GetDB() *bolt.DB { return n.db }

func (n *Node) Peers() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.peers
}

func (n *Node) PeersCopy() map[string]string {
	out := make(map[string]string, len(n.peers))
	for k, v := range n.peers {
		out[k] = v
	}
	return out
}

func (n *Node) ApplyCh() <-chan ApplyMsg { return n.applyCh }


