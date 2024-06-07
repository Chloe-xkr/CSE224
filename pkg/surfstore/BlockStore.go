package surfstore

import (
	context "context"
	"fmt"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	block, exist:= bs.BlockMap[blockHash.Hash]
	if !exist {
		return &Block{}, fmt.Errorf("no block found")
	}
	// fmt.Println("Get Block func, " ,block)
	return block, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// fmt.Println("PutBlock func, ", block)
	hash := GetBlockHashString(block.GetBlockData())
	bs.BlockMap[hash] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// hashes that are not stored in the key-value store
func (bs *BlockStore) MissingBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	res := make([]string, 0)
	for _, hash := range blockHashesIn.Hashes {
		_, exist := bs.BlockMap[hash]
		if !exist{
			res = append(res, hash)
		}
	}
	return &BlockHashes{Hashes: res}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore(blockStoreAddrs [] string) *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
// Returns a list containing all block hashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	hashes := make([]string, 0)

	for hash := range bs.BlockMap {
		hashes = append(hashes, hash)
	}
	return &BlockHashes{Hashes: hashes}, nil
}
