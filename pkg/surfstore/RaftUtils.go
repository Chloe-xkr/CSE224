package surfstore

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
	"net"
	"fmt"
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
		appliedIndex: -1,

		unreachableFrom: make(map[int64]bool),
		grpcServer:      grpc.NewServer(),
		rpcConns:        conns,

		raftStateMutex: &raftStateMutex,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	fmt.Println(server.rpcConns[server.id].Target())
	listener, err := net.Listen("tcp", server.rpcConns[server.id].Target())

	if err != nil {
		return fmt.Errorf("Fail listening: %v", err.Error())
	}
	// fmt.Println("2222")
	grpcServer := grpc.NewServer()
	RegisterRaftSurfstoreServer(grpcServer, server)
	err = grpcServer.Serve(listener)
	return err
}
