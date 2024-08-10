package SurfTest

import (
	"fmt"
	"log"
	"os"
	"testing"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
	//	"time"
)

//// A creates and syncs with a file. B creates and syncs with same file. A syncs again.
//func TestSyncTwoClientsSameFileLeaderFailure(t *testing.T) {
//	t.Logf("client1 syncs with file1. client2 syncs with file1 (different content). client1 syncs again.")
//	cfgPath := "./config_files/3nodes.txt"
//	test := InitTest(cfgPath)
//	defer EndTest(test)
//	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
//	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
//
//	worker1 := InitDirectoryWorker("test0", SRC_PATH)
//	worker2 := InitDirectoryWorker("test1", SRC_PATH)
//	defer worker1.CleanUp()
//	defer worker2.CleanUp()
//
//	//clients add different files
//	file1 := "multi_file1.txt"
//	file2 := "multi_file1.txt"
//	err := worker1.AddFile(file1)
//	if err != nil {
//		t.FailNow()
//	}
//	err = worker2.AddFile(file2)
//	if err != nil {
//		t.FailNow()
//	}
//	err = worker2.UpdateFile(file2, "update text")
//	if err != nil {
//		t.FailNow()
//	}
//
//	//client1 syncs
//	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
//	if err != nil {
//		log.Println(err)
//		t.Fatalf("Sync failed")
//	}
//	log.Println("Sync1 finished!")
//	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
//
//	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
//	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
//	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
//
//	log.Println("Sync2 Start!")
//	//client2 syncs
//	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
//	if err != nil {
//		t.Fatalf("Sync failed")
//	}
//	log.Println("Sync2 finished!")
//	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
//
//	//client1 syncs
//	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
//	if err != nil {
//		t.Fatalf("Sync failed")
//	}
//	log.Println("Sync1 finished!")
//	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
//	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
//	log.Println("Start Checking!")
//	workingDir, _ := os.Getwd()
//
//	//check client1
//	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
//	if err != nil {
//		t.Fatalf("Could not find meta file for client1")
//	}
//
//	fileMeta1, err := LoadMetaFromDB(workingDir + "/test0/")
//	if err != nil {
//		t.Fatalf("Could not load meta file for client1")
//	}
//	if len(fileMeta1) != 1 {
//		t.Fatalf("Wrong number of entries in client1 meta file")
//	}
//	if fileMeta1 == nil || fileMeta1[file1].Version != 1 {
//		t.Fatalf("Wrong version for file1 in client1 metadata.")
//	}
//
//	c, e := SameFile(workingDir+"/test0/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
//	if e != nil {
//		t.Fatalf("Could not read files in client base dirs.")
//	}
//	if !c {
//		t.Fatalf("file1 should not change at client1")
//	}
//
//	//check client2
//	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
//	if err != nil {
//		t.Fatalf("Could not find meta file for client2")
//	}
//
//	fileMeta2, err := LoadMetaFromDB(workingDir + "/test1/")
//	if err != nil {
//		t.Fatalf("Could not load meta file for client2")
//	}
//	if len(fileMeta2) != 1 {
//		t.Fatalf("Wrong number of entries in client2 meta file")
//	}
//	if fileMeta2 == nil || fileMeta2[file1].Version != 1 {
//		t.Fatalf("Wrong version for file1 in client2 metadata.")
//	}
//
//	c, e = SameFile(workingDir+"/test1/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
//	if e != nil {
//		t.Fatalf("Could not read files in client base dirs.")
//	}
//	if !c {
//		t.Fatalf("wrong file2 contents at client2")
//	}
//}

// leader1 gets a request while a minority of the cluster is down. leader1 crashes before sending a heartbeat. the other crashed nodes are restored. leader2 gets a request. leader1 is restored.
func TestRaftLogsConsistentLeaderCrashesBeforeHeartbeat(t *testing.T) {
	t.Logf("client1 syncs with file1. client2 syncs with file1 (different content). client1 syncs again.")
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	//clients add different files
	file1 := "multi_file1.txt"
	file2 := "multi_file2.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}
	err = worker2.AddFile(file2)
	if err != nil {
		t.FailNow()
	}
	err = worker2.UpdateFile(file2, "update text")
	if err != nil {
		t.FailNow()
	}

	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	//client1 syncs
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		log.Println(err)
		t.Fatalf("Sync failed")
	}
	log.Println("Sync1 finished!")
	//test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	log.Println("Sync2 Start!")
	//client2 syncs
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}
	log.Println("Sync2 finished!")
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	//
	////client1 syncs
	//err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	//if err != nil {
	//	t.Fatalf("Sync failed")
	//}
	//log.Println("Sync1 finished!")
	//test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	//test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	log.Println("Start Checking!")
	workingDir, _ := os.Getwd()

	//check client1
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}

	fileMeta1, err := LoadMetaFromDB(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		// t.Fatalf("Wrong number of entries in client1 meta file")
		t.Fatalf(fmt.Sprint(len(fileMeta1)))
	}
	if fileMeta1 == nil || fileMeta1[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}

	c, e := SameFile(workingDir+"/test0/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("file1 should not change at client1")
	}

	//check client2
	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}

	fileMeta2, err := LoadMetaFromDB(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 2 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta2 == nil || fileMeta2[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}

	c, e = SameFile(workingDir+"/test1/multi_file1.txt", SRC_PATH+"/multi_file1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("wrong file2 contents at client2")
	}

	state0, _ := test.Clients[0].GetInternalState(test.Context, &emptypb.Empty{})
	log.Println("Server 0: Status:", state0.Status, "Term:", state0.Term, "CommitIndex:", state0.CommitIndex)
	log.Println("Server0: Log: ")
	for index, logg := range state0.Log {
		log.Println("Server0: Log: Index:", index, "Term", logg.Term)
	}
	state1, _ := test.Clients[1].GetInternalState(test.Context, &emptypb.Empty{})
	log.Println("Server 1: Status:", state1.Status, "Term:", state1.Term, "CommitIndex:", state1.CommitIndex)
	state2, _ := test.Clients[2].GetInternalState(test.Context, &emptypb.Empty{})
	log.Println("Server 2: Status:", state2.Status, "Term:", state2.Term, "CommitIndex:", state2.CommitIndex)
}
