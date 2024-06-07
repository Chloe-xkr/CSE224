package surfstore

import (
	context "context"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir       string
	BlockSize     int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize
	

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	fmt.Println("PutBlock connect to the server:" , blockStoreAddr)
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := NewBlockStoreClient(conn)
	fmt.Println("PutBlock connected")
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	response, err := c.PutBlock(ctx, block)
	if err != nil {
		return err
	}
	*succ = response.Flag
	return nil
}

func (surfClient *RPCClient) MissingBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	response, err := c.MissingBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		return err
	}

	*blockHashesOut = response.Hashes
	return nil
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		// fmt.Println("metaStoreAddr ", metaStoreAddr)
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			// fmt.Println("Fail in GetFileInfoMap: ",err.Error())
			continue
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		response, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			// fmt.Println("Fail in GetFileInfoMap: ",err.Error())
			conn.Close()
			continue
		}

		*serverFileInfoMap = response.FileInfoMap
		err  = conn.Close()
		return err
	}
	return ErrNotLeader

}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)
	
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
	
		response, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			fmt.Println("Error in UpdateFile after connection: ", err.Error())
			conn.Close()
			continue
		}
		*latestVersion = response.Version
	
		return nil
	}
	return ErrNotLeader
	
}

// func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
// 	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
// 	if err != nil {
// 		return err
// 	}
// 	defer conn.Close()
// 	c := NewMetaStoreClient(conn)

// 	// perform the call
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
// 	defer cancel()

// 	response, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
// 	fmt.Println("response: ",response)
// 	if err != nil {
// 		return err
// 	}

// 	*blockStoreAddr = response.Addr
// 	return nil
// }

// Given a list of block hashes, find out which block server they belong to. Returns a mapping from block server address to block hashes.
func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)
	
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		// in := make([]int, 0)
		// in = append(in, 1)
		// in = append(in, 2)
		// fmt.Println("hashes: ", blockHashesIn)
		storeMap, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if err != nil {
			conn.Close()
			continue
		}
		// tempMap := make(map[string][]string)
		for server := range storeMap.BlockStoreMap {
			// fmt.Println("server: ", server)
			(*blockStoreMap)[server] = storeMap.BlockStoreMap[server].Hashes
			// fmt.Println(storeMap.BlockStoreMap[server].Hashes)
		}
		return nil
	}
	return ErrNotLeader
	
}

// Returns all the BlockStore addresses
func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(metaStoreAddr,grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		bsa, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			continue
		}
		*blockStoreAddrs = bsa.BlockStoreAddrs
		return nil
	}
	return ErrNotLeader
	

}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(hostPorts []string, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddrs: hostPorts,
		BaseDir:       baseDir,
		BlockSize:     blockSize,
	}
}
// Returns a list containing all block hashes on this block server
func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	c :=  NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	*blockHashes = h.Hashes
	return conn.Close()
	
}