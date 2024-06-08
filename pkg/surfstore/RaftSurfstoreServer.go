package surfstore

import (
	context "context"
	"fmt"
	"log"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)
// **************** RAFT_SURFSTORE_SERVER.go ******************
type RaftSurfstore struct {
	serverStatus      ServerStatus
	serverStatusMutex *sync.RWMutex
	term              int64
	log               []*UpdateOperation
	id                int64
	metaStore         *MetaStore
	commitIndex       int64

	raftStateMutex *sync.RWMutex

	rpcConns   []*grpc.ClientConn
	grpcServer *grpc.Server

	//New Additions
	peers           []string
	pendingRequests []*chan PendingRequest
	lastApplied     int64
	/*--------------- Chaos Monkey --------------*/
	unreachableFrom map[int64]bool
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// Ensure that the majority of servers are up
	fmt.Println("GetFileInfoMap")


	if s.serverStatus != ServerStatus_LEADER {
		return nil, ErrNotLeader
	}
	for {
		success, _ := s.SendHeartbeat(ctx, empty)
		if success.Flag {
			break
		}
		time.Sleep(100 * time.Millisecond) 
	}
	return s.metaStore.GetFileInfoMap(ctx, empty)
}

// √
func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// Ensure that the majority of servers are up
	fmt.Println("GetBlockStoreMap")
	if s.serverStatus != ServerStatus_LEADER {
		return nil, ErrNotLeader
	}
	for {
		success, _ := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if success.Flag {
			break
		}
		time.Sleep(100 * time.Millisecond) 
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)

}

// √
func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// Ensure that the majority of servers are up
	fmt.Println("GetBlockStoreAddrs")


	if s.serverStatus != ServerStatus_LEADER {
		return nil, ErrNotLeader
	}
	for {
		success, _ := s.SendHeartbeat(ctx, empty)
		if success.Flag {
			break
		}
		time.Sleep(100 * time.Millisecond) 
	}
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)

}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// Ensure that the request gets replicated on majority of the servers.
	// Commit the entries and then apply to the state machine
	fmt.Println("UpdateFile")
	if err := s.checkStatus(); err != nil {
		return nil, err
	}

	pendingReq := make(chan PendingRequest)
	s.raftStateMutex.Lock()
	entry := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, &entry)

	s.pendingRequests = append(s.pendingRequests, &pendingReq)

	//TODO: Think whether it should be last or first request
	reqId := len(s.pendingRequests) - 1
	s.raftStateMutex.Unlock()

	go s.sendPersistentHeartbeats(ctx, int64(reqId))

	// fmt.Println("response := <-pendingReq, ", reqId)
	response := <- pendingReq
	fmt.Println("continue UpdateFile response: ", response.success)
	if response.err != nil {
		return nil, response.err
	}


	if response.success {
		s.raftStateMutex.Lock()

		// apply to state machine 
		for s.lastApplied < s.commitIndex+1 {
			fmt.Println("in updatefile loop")
			// fmt.Println("s.log length = ", len(s.log))
			// fmt.Println("s.lastApplied = ", s.lastApplied)
			// fmt.Println("s.commitIndex = ", s.commitIndex)
			// for i, log := range s.log{
			// 	fmt.Println("i: ", i," log: ", log.Term, " ", log.FileMetaData)
			// }
			// fmt.Println("test: ", s.log[s.lastApplied+1].FileMetaData)
			entry := s.log[s.lastApplied+1]
			if entry.FileMetaData == nil {
				s.lastApplied += 1
				continue
			}
			_, err := s.metaStore.UpdateFile(ctx, entry.FileMetaData)
			if err != nil {
				s.raftStateMutex.Unlock()
				return nil, err
			}
			s.lastApplied += 1
		}

		s.commitIndex += 1
		s.raftStateMutex.Unlock()
		if filemeta == nil {
			return nil, nil
		}
		version := &Version{
			Version: filemeta.Version,
		}
		

		return version, nil
	}

	//TODO:
	// Ensure that leader commits first and then applies to the state machine
	return nil, fmt.Errorf("fail updating")
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex or whose term
// doesn't match prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	fmt.Println("AppendEntries, ",s.id)
	//check the status
	s.raftStateMutex.RLock()
	peerTerm := s.term
	peerId := s.id
	input.PrevLogIndex = s.commitIndex
	if input.PrevLogIndex >= 0 {
		input.PrevLogTerm = s.log[input.PrevLogIndex].Term
	}
	nextIndex := s.commitIndex + 1
	if int64(len(input.Entries)) > nextIndex {
		input.Entries = input.Entries[nextIndex:]
	} else {
		input.Entries = make([]*UpdateOperation, 0)
	}
	s.raftStateMutex.RUnlock()
	fmt.Println("server ", s.id," EntriesInput:LeaderId ",input.LeaderId," PrevLogTerm ",input.PrevLogTerm," PrevLogIndex ",input.PrevLogIndex, " LeaderCommit ", input.LeaderCommit, " eln entries ", len(input.Entries))
	for i,entry:= range input.Entries {
		fmt.Println(i, " term ",entry.Term, " filemeta ", entry.FileMetaData)
	}
	success := true
	// fmt.Println("AppendEntries00000, ",s.id)
	s.serverStatusMutex.RLock()
	isCrashed := s.serverStatus == ServerStatus_CRASHED
	s.serverStatusMutex.RUnlock()
	if isCrashed {
		fmt.Println("AppendEntries isCrashed , ",s.id)
		return nil, ErrServerCrashed
	}
	if s.unreachableFrom[input.LeaderId] {
		fmt.Println("AppendEntries unreachableFrom , ",s.id)
		return nil, ErrServerCrashedUnreachable
	}
	if peerTerm < input.Term {
		fmt.Println("peerTerm", peerTerm," < input.Term,", input.Term)
		s.serverStatusMutex.Lock()
		s.serverStatus = ServerStatus_FOLLOWER
		s.serverStatusMutex.Unlock()

		s.raftStateMutex.Lock()
		s.term = input.Term
		s.raftStateMutex.Unlock()

		peerTerm = input.Term
	}

	//TODO: Change per algorithm
	output := &AppendEntryOutput{
		Term:         peerTerm,
		ServerId:     peerId,
		Success:      success,
		MatchedIndex: -1,
	}

	// fmt.Println("AppendEntries22222, ",s.id)

	//TODO: Change this per algorithm
	
	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < peerTerm {
		return output, fmt.Errorf("term < currentTerm")
	}
	// fmt.Println("AppendEntries33333, ",s.id)

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	s.raftStateMutex.RLock()
	if input.PrevLogIndex >= 0 {
	    if len(s.log) <= int(input.PrevLogIndex) || (input.PrevLogIndex >= 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm) {
	        return output, fmt.Errorf("log does not contain an entry at prevLogIndex or has wrong PrevLogTerm")
	    }
	}
	s.raftStateMutex.RUnlock()

	// fmt.Println("AppendEntries4444, ",s.id)
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	s.raftStateMutex.RLock()
	for i, entry := range s.log {
		if int64(i) > input.PrevLogIndex {
			index := int64(i) - input.PrevLogIndex - 1
			if index < int64(len(input.Entries)) && entry.Term != input.Entries[index].Term {
				s.log = s.log[:i]
				input.Entries = input.Entries[index:]
				break
			}
		}
	}
	s.raftStateMutex.RUnlock()
	// fmt.Println("AppendEntries55555, ",s.id)

	// 4. Append any new entries not already in the log
	s.raftStateMutex.Lock()
	for i, entry := range input.Entries {
		index := input.PrevLogIndex + 1 + int64(i)
		if int(index) >= len(s.log) {
			s.log = append(s.log, entry)
		} 
	}
	s.raftStateMutex.Unlock()

	// fmt.Println("AppendEntries66666, ",s.id)
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	s.raftStateMutex.RLock()
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = min(input.LeaderCommit, int64(len(s.log)-1))
	}
	s.raftStateMutex.RUnlock()
	// fmt.Println("s.log length = ", len(s.log))
	// fmt.Println("s.lastApplied = ", s.lastApplied)
	// fmt.Println("s.commitIndex = ", s.commitIndex)
	// for i, log := range s.log{
	// 	fmt.Println("i: ", i," log: ", log.Term, " ", log.FileMetaData)
	// }
	// apply to state machine 
	fmt.Println("AppendEntries s.id ",s.id, "s.log length = ", len(s.log), " s.lastApplied = ", s.lastApplied, " s.commitIndex = ", s.commitIndex)
	s.raftStateMutex.Lock()
	for s.lastApplied < s.commitIndex {
		fmt.Println("in loop")
		entry := s.log[s.lastApplied+1]
		if entry.FileMetaData == nil {
			s.lastApplied += 1
			continue
		}
		_, err := s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		if err != nil {
			s.raftStateMutex.Unlock()
			return nil, err
		}
		s.lastApplied += 1
	}
	s.raftStateMutex.Unlock()
	fmt.Println("after AppendEntries s.id ",s.id, "s.log length = ", len(s.log), " s.lastApplied = ", s.lastApplied, " s.commitIndex = ", s.commitIndex)
	for i,entry:= range s.log {
		fmt.Println(i, " term ",entry.Term, " filemeta ", entry.FileMetaData)
	}
	// fmt.Println("Server", s.id, ": Sending output:", "Term", output.Term, "Id", output.ServerId, "Success", output.Success, "Matched Index", output.MatchedIndex)

	return output, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Println("SetLeader: ", s.id," s.commitIndex ", s.commitIndex," s.lastApplied ",s.lastApplied, " len(s.log) ", len(s.log))
	for i,entry:= range s.log {
		fmt.Println(i, " term ",entry.Term, " filemeta ", entry.FileMetaData)
	}
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()

	if serverStatus == ServerStatus_CRASHED {
		return &Success{Flag: false}, ErrServerCrashed
	}

	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_LEADER
	log.Printf("Server %d has been set as a leader", s.id)
	s.commitIndex = int64(len(s.log) - 1)
	fmt.Println("s.commitIndex is set as ", s.commitIndex)
	fmt.Println("in SetLeader: ", s.id," s.commitIndex ", s.commitIndex," s.lastApplied ",s.lastApplied, " len(s.log) ", len(s.log))
	s.serverStatusMutex.Unlock()

	s.raftStateMutex.Lock()
	s.serverStatus = ServerStatus_LEADER
	s.term += 1
	s.raftStateMutex.Unlock()

	//TODO: update the state
	s.UpdateFile(ctx, nil)
	fmt.Println("after SetLeader: ", s.id," s.commitIndex ", s.commitIndex," s.lastApplied ",s.lastApplied, " len(s.log) ", len(s.log))
	for i,entry:= range s.log {
		fmt.Println(i, " term ",entry.Term, " filemeta ", entry.FileMetaData)
	}

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Println("SendHeartbeat")
	if err := s.checkStatus(); err != nil {
		return nil, err
	}

	s.raftStateMutex.RLock()
	reqId := len(s.pendingRequests) - 1
	s.raftStateMutex.RUnlock()

	s.sendPersistentHeartbeats(ctx, int64(reqId))

	return &Success{Flag: true}, nil
}
// ========== DO NOT MODIFY BELOW THIS LINE =====================================
func (s *RaftSurfstore) MakeServerUnreachableFrom(ctx context.Context, servers *UnreachableFromServers) (*Success, error) {
	s.raftStateMutex.Lock()
	for _, serverId := range servers.ServerIds {
		s.unreachableFrom[serverId] = true
	}
	log.Printf("Server %d is unreachable from", s.unreachableFrom)
	s.raftStateMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Println("Crash: ", s.id)
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_CRASHED
	log.Printf("Server %d is crashed", s.id)
	s.serverStatusMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Println("Restore: ", s.id)
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_FOLLOWER
	s.serverStatusMutex.Unlock()

	s.raftStateMutex.Lock()
	s.unreachableFrom = make(map[int64]bool)
	s.raftStateMutex.Unlock()

	log.Printf("Server %d is restored to follower and reachable from all servers", s.id)

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.serverStatusMutex.RLock()
	s.raftStateMutex.RLock()
	state := &RaftInternalState{
		Status:      s.serverStatus,
		Term:        s.term,
		CommitIndex: s.commitIndex,
		Log:         s.log,
		MetaMap:     fileInfoMap,
	}
	s.raftStateMutex.RUnlock()
	s.serverStatusMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)