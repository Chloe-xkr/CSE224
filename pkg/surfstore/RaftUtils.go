package surfstore

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"context"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here
	conns := make([]*grpc.ClientConn, 0)
	for _, addr := range config.RaftAddrs {
		// fmt.Println(addr)
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
	}

	serverStatusMutex := sync.RWMutex{}
	raftStateMutex := sync.RWMutex{}

	server := RaftSurfstore{
		serverStatus:      ServerStatus_FOLLOWER,
		serverStatusMutex: &serverStatusMutex,
		term:              0,
		metaStore:         NewMetaStore(config.BlockAddrs),
		log:               make([]*UpdateOperation, 0),

		id:          id,
		commitIndex: -1,

		unreachableFrom: make(map[int64]bool),
		grpcServer:      grpc.NewServer(),
		rpcConns:        conns,

		raftStateMutex: &raftStateMutex,

		//New Additions
		peers:           config.RaftAddrs,
		pendingRequests: make([]*chan PendingRequest, 0),
		lastApplied:     -1,
	}

	return &server, nil
}


func (s *RaftSurfstore) sendPersistentHeartbeats(ctx context.Context, reqId int64) {
	fmt.Println("sendPersistentHeartbeats")
	numServers := len(s.peers)
	fmt.Println("numServers = ",numServers)
	peerResponses := make(chan bool, numServers-1)

	for idx := range s.peers {
		fmt.Println("idx = ",idx)
		entriesToSend := s.log
		idx := int64(idx)

		if idx == s.id {
			continue
		}

		//TODO: Utilize next index
		fmt.Println("s.log length = ", len(s.log))
		// fmt.Println("s.lastApplied = ", s.lastApplied)
		fmt.Println("s.commitIndex = ", s.commitIndex)
		for i, log := range s.log{
			fmt.Println("i: ", i," log: ", log.Term, " ", log.FileMetaData)
		}
		s.raftStateMutex.RLock()
		nextIndex := s.commitIndex + 1
		if int64(len(s.log)) > nextIndex {
			entriesToSend = s.log[nextIndex:]
		} else {
			entriesToSend = make([]*UpdateOperation, 0)
		}
		s.raftStateMutex.RUnlock()


		go s.sendToFollower(ctx, idx, entriesToSend, peerResponses)
	}

	totalResponses := 1
	numAliveServers := 1
	for totalResponses < numServers {
		fmt.Println("response := <-peerResponses, ",totalResponses)
		response := <-peerResponses
		fmt.Println("after response := <-peerResponses, ",totalResponses)
		totalResponses += 1
		if response {
			numAliveServers += 1
		}
	}

	if numAliveServers > numServers/2 {
		fmt.Println("numAliveServers > numServers/2")
		s.raftStateMutex.RLock()

		requestLen := int64(len(s.pendingRequests))
		s.raftStateMutex.RUnlock()

		if reqId >= 0 && reqId < requestLen {
			s.raftStateMutex.Lock()
			fmt.Println("s.pendingRequests[reqId] <- true, ",reqId)
			*s.pendingRequests[reqId] <- PendingRequest{success: true, err: nil}
			s.pendingRequests = append(s.pendingRequests[:reqId], s.pendingRequests[reqId+1:]...)
			s.raftStateMutex.Unlock()
		}
		fmt.Println("end numAliveServers > numServers/2")
	} else {
		fmt.Println("numAliveServers < numServers/2")

	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, peerId int64, entries []*UpdateOperation, peerResponses chan<- bool) {
	fmt.Println("sendToFollower")
	client := NewRaftSurfstoreClient(s.rpcConns[peerId])

	s.raftStateMutex.RLock()
	appendEntriesInput := AppendEntryInput{
		Term:         s.term,
		LeaderId:     s.id,
		PrevLogTerm:  0,
		PrevLogIndex: s.commitIndex,
		Entries:      entries,
		LeaderCommit: s.commitIndex,
	}
	if appendEntriesInput.PrevLogIndex >= 0 {
		appendEntriesInput.PrevLogTerm = s.log[appendEntriesInput.PrevLogIndex].Term
	}
	s.raftStateMutex.RUnlock()
	fmt.Println("EntriesInput:LeaderId ",appendEntriesInput.LeaderId," PrevLogTerm ",appendEntriesInput.PrevLogTerm," PrevLogIndex ",appendEntriesInput.PrevLogIndex, " LeaderCommit ", appendEntriesInput.LeaderCommit, " eln entries ", len(appendEntriesInput.Entries))
	for i,entry:= range appendEntriesInput.Entries {
		fmt.Println(i, " term ",entry.Term, " filemeta ", entry.FileMetaData)
	}

	_, err := client.AppendEntries(ctx, &appendEntriesInput)
	// fmt.Println("Server", s.id, ": Receiving output:", "Term", reply.Term, "Id", reply.ServerId, "Success", reply.Success, "Matched Index", reply.MatchedIndex)
	if err != nil {
		fmt.Println("peerResponses <- false")
		peerResponses <- false
	} else {
		fmt.Println("peerResponses <- true")
		peerResponses <- true
	}

}

func (s *RaftSurfstore) checkStatus() error {
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()

	if serverStatus == ServerStatus_CRASHED {
		return ErrServerCrashed
	}

	if serverStatus != ServerStatus_LEADER {
		return ErrNotLeader
	}

	return nil
}
// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	RegisterRaftSurfstoreServer(server.grpcServer, server)
	log.Println("Successfully started the RAFT server with id:", server.id)
	listener, err := net.Listen("tcp", server.rpcConns[server.id].Target())

	if err != nil {
		return fmt.Errorf("Fail listening: %v", err.Error())
	}

	err = server.grpcServer.Serve(listener)
	return err
}
