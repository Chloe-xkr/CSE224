package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	// "fmt"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {

	hashes := make([]string, 0)
	for h := range c.ServerMap {
		// fmt.Printf("%v ",h)
		hashes = append(hashes, h)
	}
	// fmt.Println()

	sort.Strings(hashes)
	for _, hashKey := range hashes {
		if blockId < hashKey {
			return c.ServerMap[hashKey]
		}
	}
	return c.ServerMap[hashes[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	consistentHashRing := &ConsistentHashRing{ServerMap: make(map[string]string)}
	for _, addr := range serverAddrs{
		addrName := "blockstore"+addr
		hashAddr := consistentHashRing.Hash(addrName)
		consistentHashRing.ServerMap[hashAddr] = addr

	}
	return consistentHashRing
}
