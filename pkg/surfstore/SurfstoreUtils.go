package surfstore

import (
	"fmt"
	"os"
	"io"
	// "bufio"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	fmt.Print(client.BaseDir)
	// local index.db
	LocalIndexDB, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		fmt.Println("Fail loading data from meta file", err.Error())
		os.Exit(1)
	}
	fmt.Printf("Sync\n")
	// if (LocalIndexDB == nil) {
	// 	fmt.Println("LocalIndexDB is nil")
	// }
	// fmt.Println(LocalIndexDB)
	PrintMetaMap(LocalIndexDB)
	// scan the base directory, and for each file, compute that file’s hash list.
	files, err := os.ReadDir(client.BaseDir)
	if err != nil {
		fmt.Println("Fail reading directory ", err.Error())
		os.Exit(1)
	}
	existingFiles := make([]string, 0)
	for _, file := range files{
		fmt.Printf("%s\n", file.Name())
		if file.Name() == DEFAULT_META_FILENAME || file.IsDir() {
			continue
		}
		existingFiles = append(existingFiles, file.Name())
		fileData, err := os.Open(client.BaseDir + "/" + file.Name())
		if err != nil {
			fmt.Println("Fail opening file " + file.Name())
			os.Exit(1)
		}
		empty_flag := true
		currentBlockHashList := make([]string, 0)
		//  compute that file’s hash list.
		for i:=0;;i++ {
			blockData := make([]byte, client.BlockSize)
			n_bytes, err := fileData.Read(blockData)
			block := blockData[:n_bytes]
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				fmt.Println("Fail reading file " + file.Name() + "with error" + err.Error())
				os.Exit(1)
			}
			if n_bytes == 0 {
				if empty_flag {
					currentBlockHashList[i] = "-1"  // empty file
				} else {
					break
				}
			} else {
				// fmt.Println("GetBlockHashString(blockData): ",GetBlockHashString(block))
				// 0653e362ea69c4dc7ebde3358bb76e70b124a723ce5b3a0a84d553145e0b0590
				currentBlockHashList = append(currentBlockHashList, GetBlockHashString(block))
				empty_flag = false
			}
		}
		// consult the local index file and compare the results
		indexBlockHashList, exist := LocalIndexDB[file.Name()]
		if !exist {
			// add file to local index.db
			indexMetaData := &FileMetaData{Filename: file.Name(), Version: 1, BlockHashList: currentBlockHashList} 
			LocalIndexDB[file.Name()] = indexMetaData
			fmt.Println("Added ",file.Name(), " to index.db." )
		} else if !equalStringSlices(indexBlockHashList.BlockHashList, currentBlockHashList) {
			fmt.Println("block changed")
			// some block changed
			LocalIndexDB[file.Name()].Version += 1
			LocalIndexDB[file.Name()].BlockHashList = currentBlockHashList
			fmt.Println("Updated ",file.Name(), " to index.db." )
		}
	}
	PrintMetaMap(LocalIndexDB)
	// handel deleted files
	for fn, metadata := range LocalIndexDB {
		if !Contains(existingFiles, fn) {
			if !isEmptyFile(metadata) {
				// delete file in index
				metadata.Version += 1
				metadata.BlockHashList = []string{"0"}
			}
		}
	}
	
	// connect to the server and download an updated FileInfoMap  “remote index.”
	remoteFileInfoMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteFileInfoMap)
	if err != nil {
		fmt.Println("Fail getting remote index info", err.Error())
		os.Exit(1)
	}
	var addrs []string
	err = client.GetBlockStoreAddrs(&addrs)
	// fmt.Println("Adrress: ",addr)
	if err != nil {
		fmt.Println("Error getting remote block store addrs", err.Error())
		os.Exit(1)
	}

	// compare the local index (and any changes to local files not reflected in the local index) with the remote index.
	// existingRemoteFiles := make([]string, 0)
	// download
	for filename, remoteMetaData := range remoteFileInfoMap {
		// existingRemoteFiles = append(existingRemoteFiles, filename)
		if localMetaData, exist := LocalIndexDB[filename]; exist {  //exist
			fmt.Println("exist when download")
			if localMetaData.Version < remoteMetaData.Version {
				err := download(client, remoteMetaData, localMetaData, addrs)
				if err != nil {
					fmt.Println("Fail download existing file:", err.Error())
					os.Exit(1)
				}
			}
		} else {
			fmt.Println("not exist when download")
			// new file -> index+download

			localMetaData = &FileMetaData{}  
			err = download(client, remoteMetaData, localMetaData, addrs)
			LocalIndexDB[filename] = localMetaData
			if err != nil {
				fmt.Println("Fail download inexisting file:", err.Error(), "// filename: ", filename)
				os.Exit(1)
			}
		}

	}
	// upload

	for filename, localMetaData := range LocalIndexDB {


		remoteMetaData, exist := remoteFileInfoMap[filename]
		if exist {  //exist
			fmt.Println("exist when upload, local version ", localMetaData.Version, " reomote version, ",remoteMetaData.Version)
			if localMetaData.Version == remoteMetaData.Version + 1 {
				err := upload(client, localMetaData, addrs)
				if err != nil {
					fmt.Println("Fail uploading existing file:", err.Error())
					os.Exit(1)
				}
			} else if localMetaData.Version <= remoteMetaData.Version {
				err := download(client,remoteMetaData, localMetaData, addrs)
				if err != nil {
					fmt.Println("Fail downloading existing file:", err.Error())
					os.Exit(1)
				}
			}
		} else {
			fmt.Println("not exist when upload")
			// new file -> index+download
			err = upload(client, localMetaData, addrs)
			if err != nil {
				fmt.Println("Fail uploading inexisting file:", err.Error())
				os.Exit(1)
			}
			// var block Block
			// client.GetBlock("0653e362ea69c4dc7ebde3358bb76e70b124a723ce5b3a0a84d553145e0b0590", addr, &block)
		}

	}

	
	//          GetFileInfoMap()[]          getBlock()
	// filename ->                 hashlist -> blockAddrs
	// filename -> hashlist -> blockAddr
	// download the blocks
	//  upload the blocks
	fmt.Println("Writng index.db")

	err = WriteMetaFile(LocalIndexDB, client.BaseDir)
	if err != nil {
		fmt.Println("Error WriteMetaFile: ", err.Error())
		os.Exit(1)
	}
	fmt.Println("Writng finished")
}
func getBlockServerAddress (block string, addrs []string, blockStoreMap map[string][]string) (addr string, err error){
	for server := range blockStoreMap {
		if Contains(blockStoreMap[server], block) {
			return server, nil
		}
	}
	return "", fmt.Errorf("Fail getting server address")
	
}
func download (client RPCClient, remoteMetaData *FileMetaData, localMetaData *FileMetaData, addrs []string) (error) {
	remoteBlockList := remoteMetaData.GetBlockHashList()
	localBlockList := localMetaData.GetBlockHashList()
	
	*localMetaData = *remoteMetaData
	path := ConcatPath(client.BaseDir, remoteMetaData.Filename)
	fmt.Println(path)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("1111111 ")
		return err
	}
	// download blocks
	defer file.Close()
	if isEmptyFile(remoteMetaData) {
		err = os.Remove(file.Name())
		if err != nil {
			return err
		} else {
			return nil
		}
	}
	blockStoreMap := make(map[string][]string)
	err = client.GetBlockStoreMap(remoteBlockList, &blockStoreMap)
	if err != nil {
		return err
	}
	for index, hash := range remoteBlockList {
		addr, err := getBlockServerAddress(hash, addrs,blockStoreMap)
		if err != nil {
			return err
		}
		if !Contains(localBlockList, hash) {
			var block Block
			fmt.Println("hash: ",hash)
			err := client.GetBlock(hash, addr, &block)

			if err != nil {
				return err
			}
			n , _:= file.WriteAt(block.BlockData, int64(index*client.BlockSize))
			err = file.Sync()
			if err != nil {
				return err
			}
			// fmt.Println(block.BlockData)
			// fmt.Println(n)
			if index == len(remoteBlockList)-1 {
				err := os.Truncate(path, int64(index*client.BlockSize+n))
				if err != nil {
					return err
				}
			}
		}

	}
	return nil
}
func upload(client RPCClient, localMetaData *FileMetaData, addrs []string) (error){
	// fmt.Println("1")
	localBlockList := localMetaData.GetBlockHashList()
	// remoteBlockList := remoteMetaData.GetBlockHashList()
	// *remoteMetaData = *localMetaData
	file, err := os.Open(ConcatPath(client.BaseDir, localMetaData.Filename))
	
	var latestVersion int32
	if err != nil {
		// deleted
		if os.IsNotExist(err) {
			client.UpdateFile(localMetaData, &latestVersion)
			localMetaData.Version = latestVersion
			return nil
		} else {
			fmt.Println("Fail opening file " + localMetaData.Filename)
			os.Exit(1)
		}
	}
	defer file.Close()
	blockStoreMap := make(map[string][]string)
	// fmt.Println("222222")
	err = client.GetBlockStoreMap(localBlockList, &blockStoreMap)
	if err != nil {

		return fmt.Errorf("Fail GetBlockStoreMap: ", err.Error())
	}
	// fmt.Println("333333")
	for _, hash := range localBlockList {
		addr, err := getBlockServerAddress(hash, addrs, blockStoreMap)
		fmt.Println(addr)
		if err != nil {
			return err
		}
		fmt.Println(hash)    
		blockData := make([]byte, client.BlockSize)
		n, err := file.Read(blockData)
		if err != nil {
			return fmt.Errorf("Fali reading file: %v", err.Error())
		}
		block := Block{BlockData: blockData[:n], BlockSize: int32(n)}
		var success bool
		client.PutBlock(&block, addr, &success)
		// client.GetBlock(GetBlockHashString(block.GetBlockData()), addr, &block)
		// fmt.Println("1111112222",GetBlockHashString(block.GetBlockData()))
		if !success {
			return fmt.Errorf("Fail PutBlock")
		}
	}
	// fmt.Println("hhhhhhhhh")
	err = client.UpdateFile(localMetaData, &latestVersion)
	if err != nil {
		return err
	} else if latestVersion == -1 {       //dele
		localMetaData.Version = latestVersion
	}
	return nil

}
func fileToBlockHashList(filename string, serverFileInfoMap map[string]*FileMetaData) ([]string, error) {
	metaData, exist := serverFileInfoMap[filename]
	if !exist {
		return nil, fmt.Errorf("Remote doesn't contain this file")
	}
	blockList := metaData.GetBlockHashList()
	return blockList, nil
}
func equalStringSlices(a, b []string) bool {
    if len(a) != len(b) {
        return false
    }
    for i := range a {
        if a[i] != b[i] {
            return false
        }
    }
    return true
}
func Contains(set []string, target string) bool {
	for _, element := range set {
		if element == target {
			return true
		}
	}
	return false
}
func isEmptyFile(metadata *FileMetaData)(bool) {
	return len(metadata.BlockHashList) == 1 && metadata.BlockHashList[0] == "0"
}