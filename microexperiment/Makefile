make all:
	g++ -std=c++0x  -g -O3 -march=native -mtune=native noQueueGlobal.cpp -fopenmp -lpthread -o queuedApproachGlobalRel
	g++ -std=c++0x  -g -O3 -march=native -mtune=native noQueueLocal.cpp -fopenmp -lpthread -o queuedApproachLocalRel
	g++ -std=c++0x  -g -O3 -march=native -mtune=native flink_approach.cpp -fopenmp -lpthread -ltbb -o flinkApproachRel
