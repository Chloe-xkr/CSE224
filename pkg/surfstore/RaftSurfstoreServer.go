package surfstore

import (
	context "context"
	"log"
	"sync"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	serverStatus      ServerStatus
	serverStatusMutex *sync.RWMutex
	term              int64
	log               []*UpdateOperation
	id                int64
	metaStore         *MetaStore
	commitIndex       int64
	appliedIndex      int64
	wailList []*chan bool

	raftStateMutex *sync.RWMutex

	rpcConns   []*grpc.ClientConn
	grpcServer *grpc.Server

	/*--------------- Chaos Monkey --------------*/
	unreachableFrom map[int64]bool
	UnimplementedRaftSurfstoreServer
}
// √
func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// Ensure that the majority of servers are up
	s.serverStatusMutex.RLock()
	defer s.serverStatusMutex.RUnlock()

	if s.serverStatus != ServerStatus_LEADER {
		return nil, ErrNotLeader
	}
	for {
		success, _ := s.SendHeartbeat(ctx, empty)
		if success.Flag {
			break
		}
	}
	return s.metaStore.GetFileInfoMap(ctx, empty)
}
// √
func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// Ensure that the majority of servers are up
	s.serverStatusMutex.RLock()
	defer s.serverStatusMutex.RUnlock()

	if s.serverStatus != ServerStatus_LEADER {
		return nil, ErrNotLeader
	}
	for {
		success, _ := s.SendHeartbeat(ctx,  &emptypb.Empty{})
		if success.Flag {
			break
		}
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)

}
// √
func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// Ensure that the majority of servers are up
	s.serverStatusMutex.RLock()
	defer s.serverStatusMutex.RUnlock()

	if s.serverStatus != ServerStatus_LEADER {
		return nil, ErrNotLeader
	}
	for {
		success, _ := s.SendHeartbeat(ctx, empty)
		if success.Flag {
			break
		}
	}
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)

}
// // √
// func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
// 	// Ensure that the request gets replicated on majority of the servers.
// 	// Commit the entries and then apply to the state machine

// 	// Check if the current node is the leader
// 	s.serverStatusMutex.RLock()
// 	if s.serverStatus != ServerStatus_LEADER {
// 		s.serverStatusMutex.RUnlock()
// 		return nil, ErrNotLeader
// 	} else {
// 		fmt.Println("UpdateFile is leader")
// 	}
// 	s.serverStatusMutex.RUnlock()
// 	s.raftStateMutex.Lock()
// 	defer s.raftStateMutex.Unlock()

// 	// Append the new update to the log
// 	s.raftStateMutex.Lock()
// 	newEntry := &UpdateOperation{
// 		Term:    s.term,
// 		FileMetaData: filemeta,
// 	}
// 	s.log = append(s.log, newEntry)
// 	targetIndex := int64(len(s.log) - 1)
//     s.raftStateMutex.Unlock()

// 	// Send the new entry to all followers in parallel
// 	responses := make(chan bool, len(s.rpcConns)-1)
//     for idx, conn := range s.rpcConns {
//         if int64(idx) == s.id {
//             continue
//         }
//         go s.sendToFollower(ctx, targetIndex, conn, responses)
//     }

// 	// Wait for majority of the followers to replicate the entry
//     totalAppends := 1
//     for {
//         if s.serverStatus == ServerStatus_CRASHED {
//             return nil, ErrServerCrashed
//         }
//         result := <-responses
//         if result {
//             totalAppends++
//         }
//         if totalAppends > len(s.rpcConns)/2 {
//             break
//         }
//     }

// 	// Update commitIndex and apply the entry to the state machine
//     s.raftStateMutex.Lock()
//     s.commitIndex = targetIndex
//     for s.appliedIndex < s.commitIndex {
//         s.appliedIndex++
//         entry := s.log[s.appliedIndex]
//         s.metaStore.UpdateFile(ctx, entry.FileMetaData)
//     }
//     s.raftStateMutex.Unlock()

// 	// Return the version number of the file
// 	version := &Version{
// 		Version: filemeta.Version,
// 	}

// 	return version, nil
// }

// func (s *RaftSurfstore) sendToFollower(ctx context.Context, targetInd int64, conn *grpc.ClientConn, responses chan bool) {
//     count := 0
//     for {
//         AppendEntriesInput := AppendEntryInput{
//             Term: s.term,
//             PrevLogIndex: targetInd - 1,
//             PrevLogTerm:  -1,
//             Entries:      s.log[targetInd:],
//             LeaderCommit: s.commitIndex,
//         }
//         if AppendEntriesInput.PrevLogIndex >= 0 {
//             AppendEntriesInput.PrevLogTerm = s.log[AppendEntriesInput.PrevLogIndex].Term
//         }

//         client := NewRaftSurfstoreClient(conn)

//         ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//         defer cancel()

//         output, err := client.AppendEntries(ctx, &AppendEntriesInput)
//         if err != nil {
//             if count == 0 {
//                 fmt.Println("Fail sending to follower: ", err.Error())
//             }
//             count += 1
//             continue
//         }

//         if output != nil && output.Success {
//             responses <- true
//             return
//         }
//     }
// }

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	s.serverStatusMutex.RLock()
	if s.serverStatus != ServerStatus_LEADER {
		s.serverStatusMutex.RUnlock()
		return nil, ErrNotLeader
	}
	s.serverStatusMutex.RUnlock()

	// Append the new update to the log
	s.raftStateMutex.Lock()
	newEntry := &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, newEntry)
	targetIndex := int64(len(s.log) - 1)
	s.raftStateMutex.Unlock()

	affair := make(chan bool)
	s.wailList = append(s.wailList, &affair)

	// Send the new entry to all followers in parallel
	go s.waitResponses(ctx, targetIndex, len(s.wailList)-1)

	// Update commitIndex and apply the entry to the state machine
	finished := <-affair

	if finished {
		s.raftStateMutex.Lock()
		for s.appliedIndex < s.commitIndex {
			s.appliedIndex++
			entry := s.log[s.appliedIndex]
			s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		}
		s.raftStateMutex.Unlock()

		version := &Version{
			Version: filemeta.Version,
		}
		return version, nil
	}

	return nil, fmt.Errorf("fail updating")
}

func (s *RaftSurfstore) waitResponses(ctx context.Context, targetInd int64, listId int) {
	responses := make(chan bool, len(s.rpcConns)-1)
	for idx, conn := range s.rpcConns {
		if int64(idx) == s.id {
			continue
		}
		go s.connectFollower(ctx, targetInd, conn, responses)
	}

	cnt := 1
	for {
		if s.serverStatus == ServerStatus_CRASHED {
			*s.wailList[listId] <- false

		}
		result := <-responses
		if result {
			cnt++
		}
		if cnt > len(s.rpcConns)/2 {
			*s.wailList[listId] <- true
			s.raftStateMutex.Lock()
			s.commitIndex = targetInd
			s.raftStateMutex.Unlock()
			break
		}
	}
}
func (s *RaftSurfstore) connectFollower(ctx context.Context, targetInd int64, conn *grpc.ClientConn, responses chan bool) {
	count := 0
	for {
		AppendEntriesInput := AppendEntryInput{
			Term: s.term,
			PrevLogIndex: targetInd - 1,
			PrevLogTerm:  -1,
			Entries:      s.log[targetInd:],
			LeaderCommit: s.commitIndex,
		}
		if AppendEntriesInput.PrevLogIndex >= 0 {
			AppendEntriesInput.PrevLogTerm = s.log[AppendEntriesInput.PrevLogIndex].Term
		}

		client := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		output, err := client.AppendEntries(ctx, &AppendEntriesInput)
		if err != nil {
			if count == 0 {
				fmt.Println("Fail sending to follower: ", err.Error())
			}
			count++
			continue
		}

		if output != nil && output.Success {
			responses <- true
			return
		}
	}
}

// √
// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex or whose term
// doesn't match prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
    s.raftStateMutex.Lock()
    defer s.raftStateMutex.Unlock()

    output := &AppendEntryOutput{
        Term:         s.term,
        Success:      false,
        MatchedIndex: -1,
    }

    s.serverStatusMutex.RLock()
    isCrashed := s.serverStatus == ServerStatus_CRASHED
    s.serverStatusMutex.RUnlock()
    if isCrashed {
        return output, ErrServerCrashed
    }

    if s.unreachableFrom[input.LeaderId] {
        return output, ErrServerCrashedUnreachable
    }

    if input.Term > s.term {
        s.term = input.Term
        s.serverStatusMutex.Lock()
        s.serverStatus = ServerStatus_FOLLOWER
        s.serverStatusMutex.Unlock()
    }

    // 1. Reply false if term < currentTerm (§5.1)
    if input.Term < s.term {
        return output, fmt.Errorf("term < currentTerm")
    }

    // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
    if input.PrevLogIndex >= 0 {
        if len(s.log) <= int(input.PrevLogIndex) || (input.PrevLogIndex >= 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm) {
            return output, fmt.Errorf("log does not contain an entry at prevLogIndex or has wrong PrevLogTerm")
        }
    }

    // 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
    // s.log 123   -> 123      12345 -> 123
	// input 2345 1-> 2345     123   -> []
	for i, entry := range s.log {
		s.appliedIndex = int64(i - 1)
        if int64(i) > input.PrevLogIndex {
            if i < len(input.Entries) && entry.Term != input.Entries[i].Term {
                s.log = s.log[:i]
                break
            }
        }
    }

    // 4. Append any new entries not already in the log
    for i, entry := range input.Entries {
        index := input.PrevLogIndex + 1 + int64(i)
        if int(index) >= len(s.log) {
            s.log = append(s.log, entry)
        }
    }

    // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
    if input.LeaderCommit > s.commitIndex {
        s.commitIndex = min(input.LeaderCommit, int64(len(s.log)-1))
    }

    // Apply to state machine
    for s.appliedIndex < s.commitIndex {
        s.appliedIndex++
        entry := s.log[s.appliedIndex]
        s.metaStore.UpdateFile(ctx, entry.FileMetaData)
    }

    output.Success = true
    output.Term = s.term
    output.MatchedIndex = s.commitIndex

    return output, nil
}


func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Println("SetLeader")
	s.serverStatusMutex.RLock()
    if s.serverStatus == ServerStatus_CRASHED {
        s.serverStatusMutex.RUnlock()
        return &Success{Flag: false}, ErrServerCrashed
    }
    s.serverStatusMutex.RUnlock()

    s.serverStatusMutex.Lock()
    defer s.serverStatusMutex.Unlock()
    s.serverStatus = ServerStatus_LEADER
    s.term++
    
    fmt.Println("set new leader:", s.id)

    return &Success{Flag: true}, nil
}
//  if the leader can query a majority quorum of the nodes, it will reply back to the client with the correct answer.  
// When a majority of nodes are in a crashed state, clients will not receive responses until a majority are restored.  
// Sends a round of AppendEntries to all other nodes. The leader will attempt to replicate logs to all other nodes when this is called. It can be called even when there are no entries to replicate. If a node is not in the leader state it should do nothing
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.RLock()
	if s.serverStatus == ServerStatus_CRASHED {
		s.serverStatusMutex.RUnlock()
		return &Success{Flag: false}, ErrServerCrashed
	}
	if s.serverStatus != ServerStatus_LEADER {
		s.serverStatusMutex.RUnlock()
		return nil, ErrNotLeader
	}
	s.serverStatusMutex.RUnlock()
	AppendEntriesInput := AppendEntryInput{
		Term:         s.term,
		LeaderId:     s.id,
		PrevLogTerm:  0,
		PrevLogIndex: s.commitIndex,
		Entries:      s.log, 
		LeaderCommit: s.commitIndex,
	}
	if AppendEntriesInput.PrevLogIndex >= 0 {
		AppendEntriesInput.PrevLogTerm = s.log[AppendEntriesInput.PrevLogIndex].Term
	}

	aliveServers := 0

	for i, conn := range s.rpcConns {
		if int64(i) == s.id{
			continue
		}
		// Check if the server is unreachable
		s.raftStateMutex.RLock()
		if s.unreachableFrom[int64(i)] {
			s.raftStateMutex.RUnlock()
			continue
		}
		s.raftStateMutex.RUnlock()

		client := NewRaftSurfstoreClient(conn)
		
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, &AppendEntriesInput)
		if err != nil {
			continue
		}
		if output != nil {
			aliveServers++
		}
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		if output != nil {
			aliveServers++
		}
	}
	if aliveServers <= len(s.rpcConns) / 2 {
		return &Success{Flag: false}, fmt.Errorf("fail sending heartbeats to majority")
	}

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
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_CRASHED
	log.Printf("Server %d is crashed", s.id)
	s.serverStatusMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
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
