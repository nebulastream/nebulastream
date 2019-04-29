make all:
#	g++ -std=c++0x  -g -O3 -march=native -mtune=native noQueueGlobal.cpp -fopenmp -lpthread -o queuedApproachGlobalRel
#	g++ -std=c++0x  -g -O3 -march=native -mtune=native noQueueLocal.cpp -fopenmp -lpthread -o queuedApproachLocalRel
#	g++ -std=c++0x  -g -O3 -march=native -mtune=native flink_approach.cpp -fopenmp -lpthread -ltbb -o flinkApproachRel
	g++ -std=c++0x  -g -O3 -march=native -mtune=native -L ../rdma_lib/build/ flink_approach_rdma.cpp -lrdma_lib -fopenmp -lpthread -ltbb -o flinkApproachRDMA
	