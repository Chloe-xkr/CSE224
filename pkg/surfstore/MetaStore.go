package surfstore

import (
	context "context"
	// "fmt"
	// "fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddrs []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// fmt.Println("1111")
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	_, exist := m.FileMetaMap[fileMetaData.Filename]
	version_local := fileMetaData.Version
	if !exist{
		// new file
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	} else {
		version_store := m.FileMetaMap[fileMetaData.Filename].Version
		if version_local != version_store+1 {
			return &Version{Version: -1}, nil
		} else {
			m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		}
	}
	return &Version{Version: version_local}, nil
}

// func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
// 	fmt.Println(" GetBlockStoreAddr")
// 	return &BlockStoreAddr{Addr : m.BlockStoreAddr}, nil
	
// }
// Given a list of block hashes, find out which block server they belong to. Returns a mapping from block server address to block hashes. 
func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (blockStoreMap *BlockStoreMap, err error) {
	// file hashes -> -> blocks

	mapContent := make(map[string]*BlockHashes)
	blockStoreMap = &BlockStoreMap{BlockStoreMap: mapContent}
	for _, hash := range blockHashesIn.GetHashes() {
		// fmt.Println("GetBlockStoreMap, GetResponsibleServer")
		blockServer := m.ConsistentHashRing.GetResponsibleServer(hash)
		// fmt.Println(blockServer)
		server2hashes, exist := blockStoreMap.BlockStoreMap[blockServer]
		if !exist{
			server2hashes = &BlockHashes{Hashes: make([]string, 0)}
			server2hashes.Hashes = append(server2hashes.Hashes, hash)
		} else {
			server2hashes.Hashes = append(server2hashes.Hashes, hash)
		}
		(*blockStoreMap).BlockStoreMap[blockServer] = server2hashes
	}
	// fmt.Println(blockStoreMap.BlockStoreMap["localhost:8081"])
	return blockStoreMap, err
}
func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs [] string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
