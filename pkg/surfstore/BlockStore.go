package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {

	return nil, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	panic("todo")
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are NOT stored in the key-value store
func (bs *BlockStore) MissingBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	panic("todo")
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	panic("todo")
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
