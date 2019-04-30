#ifndef MAMPI_JOINPARAMETERS_H
#define MAMPI_JOINPARAMETERS_H


#define ITERATIONS 3
#define BUILD_TUPLES 128000000
#define PROBE_TUPLES 256000000
#define SELECTIVITY 0.5
#define B_PAYLOAD_SIZE 16
#define P_PAYLOAD_SIZE 16


#define JOIN_TYPES \
new MultithreadedCPUJoin(),\
new UnifiedMemoryGPUJoin(),\
new SingleGPUJoin(),\
(AbstractVerbsJoin*) new ShipAllJoin(connections),\
(AbstractVerbsJoin*) new ShipAllGPUJoin(connections),\
(AbstractVerbsJoin*) new ShipPerfectJoin(connections),\
(AbstractVerbsJoin*) new ShipPerfectGPUJoin(connections),\
(AbstractVerbsJoin*) new ShipNoneJoin(connections),\
(AbstractVerbsJoin*) new ShipNoneGPUJoin(connections),\
(AbstractVerbsJoin*) new ShipAll2GPUJoin(connections),\
(AbstractVerbsJoin*) new ShipPerfect2GPUJoin(connections),\


/*
new ETHNoPartitioningJoinSingleThreaded(),\
new ETHNoPartitioningJoin(),\
new ETHParallelRadixJoinHistogrambased(),\
new ETHParallelRadixJoinHistogrambasedOptimized(),\
new ETHParallelRadixJoinOptimized(),\
new ETHRadixJoin(),\
(AbstractVerbsJoin*) new ShipAllJoin(connections),\
(AbstractVerbsJoin*) new ShipAllGPUJoin(connections),\
(AbstractVerbsJoin*) new ShipPerfectJoin(connections),\
(AbstractVerbsJoin*) new ShipPerfectGPUJoin(connections),\
(AbstractVerbsJoin*) new ShipNoneJoin(connections),\
(AbstractVerbsJoin*) new ShipNoneGPUJoin(connections),\
new SingleGPUJoin(),\
new UnifiedMemoryGPUJoin(),\
new SingleCPUJoin(),\
new MultithreadedCPUJoin(),\

 */

#define MOVE_ALL_TUPLES_BUFFER_SIZE 1024*512
#define MOVE_ALL_TUPLES_BUFFER_COUNT 10

#define HASH_MAP_TYPE ETHHashmap

#define EXCHANGE_OPERATOR ReadWriteOperator

//SendReceiveOperator
#define JOIN_SEND_BUFFER_COUNT 5
#define JOIN_SEND_BUFFER_SIZE 1024*1024*64

//ReadWriteOperator
#define WRITE_SEND_BUFFER_COUNT 20
#define WRITE_RECEIVE_BUFFER_COUNT 20
#define JOIN_WRITE_BUFFER_SIZE 1024*1024*8


#define GPU_JOIN_BUILD_SUB_ITERATION_COUNT 4
#define GPU_JOIN_BUILD_BLOCK_COUNT 32
#define GPU_JOIN_BUILD_THREAD_COUNT 1024

#define GPU_JOIN_PROBE_SUB_ITERATION_COUNT 7
#define GPU_JOIN_PROBE_BLOCK_COUNT 32
#define GPU_JOIN_PROBE_THREAD_COUNT 1024



#endif //MAMPI_JOINPARAMETERS_H

/*
new SingleCPUJoin(),
(AbstractVerbsJoin*) new ShipAllJoin(connections),
(AbstractVerbsJoin*) new ShipNoneJoin(connections),
(AbstractVerbsJoin*) new ShipPerfectJoin(connections),
new MultithreadedCPUJoin(),
new SingleGPUJoin()
new UnifiedMemoryGPUJoin(),
 */




