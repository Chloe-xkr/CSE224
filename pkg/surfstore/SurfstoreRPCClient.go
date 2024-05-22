package surfstore

import (
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	panic("todo")
}

func (surfClient *RPCClient) MissingBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	panic("todo")
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	panic("todo")
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	panic("todo")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	panic("todo")
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	panic("todo")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	panic("todo")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
