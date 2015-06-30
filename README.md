Lab1

Part 1
cd src/main
./test-wc.sh

Part 2
cd src/mapreduce
./lab1_part2_test.sh

Part 3
cd src/mapreduce
./lab1_part3_test.sh


Lab2

Part A
cd src/viewservice
go test

Part B
cd src/pbservice
export GOPATH=$(pwd)/../../
go test


Lab3
Part A
cd src/paxos
go test

PartB
cd src/kvpaxos
export GOPATH=$(pwd)/../../
go test


Lab4
Part A
cd src/shardmaster
export GOPATH=$(pwd)/../../
go test

Part B
cd src/shardkv
export GOPATH=$(pwd)/../../
go test


Lab5
cd src/diskv
export GOPATH=$(pwd)/../../
go test
