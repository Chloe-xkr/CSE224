package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	panic("todo")
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	panic("todo")
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	panic("todo")
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	panic("todo")
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
