cmake_minimum_required(VERSION 3.8)

project(MAMPI CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -ansi -pthread -lpthread -ltbb -fopenmp")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -pg -pthread -lpthread -ltbb -fopenmp")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -pthread -lpthread -ltbb -fopenmp")
# set(CUDA_TOOLKIT_ROOT_DIR /usr/local/cuda/)
# set(CMAKE_CUDA_COMPILER /usr/local/cuda/bin/nvcc)

# find_package(OpenMP REQUIRED)
# if (OPENMP_FOUND)
#     set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
#     set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
#     set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -Xcompiler ${OpenMP_CXX_FLAGS}")
#     message(STATUS "${CMAKE_CUDA_FLAGS}")
# endif()
OPTION (USE_OpenMP "Use OpenMP" ON)
IF(USE_OpenMP)
  FIND_PACKAGE(OpenMP)
  IF(OPENMP_FOUND)
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
  ENDIF()
ENDIF()

# Custome CMake find instructions and macros
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")
include(cmake/macros.cmake)
include(${CMAKE_ROOT}/Modules/ExternalProject.cmake)


find_package(TBB REQUIRED tbb)

find_package(Boost REQUIRED filesystem system)
link_directories(${Boost_LIBRARY_DIRS})
include_directories(${Boost_INCLUDE_DIRS})

set(INFINITY_SOURCE external/infinity/src/infinity)
set(INFINITY_INCLUDES external/infinity/src/)
add_library(infinity ${INFINITY_SOURCE}/core/Context.cpp
        ${INFINITY_SOURCE}/memory/Atomic.cpp
        ${INFINITY_SOURCE}/memory/Buffer.cpp
        ${INFINITY_SOURCE}/memory/Region.cpp
        ${INFINITY_SOURCE}/memory/RegionToken.cpp
        ${INFINITY_SOURCE}/memory/RegisteredMemory.cpp
        ${INFINITY_SOURCE}/queues/QueuePair.cpp
        ${INFINITY_SOURCE}/queues/QueuePairFactory.cpp
        ${INFINITY_SOURCE}/requests/RequestToken.cpp
        ${INFINITY_SOURCE}/utils/Address.cpp)

include_directories(SYSTEM ${INFINITY_INCLUDES})

set(JSON11_SOURCE external/json11)
add_library(json11 ${JSON11_SOURCE}/json11.cpp)

include_directories(rdma_lib/Utilities)
include_directories(SYSTEM external/)
include_directories(src/)

# if(CUDA_FOUND)
#     set(MemoryAllocator
#             src/Utilities/DeviceMemoryAllocator/GPUMemoryAllocator.cu
#             src/operators.cu
#             src/SingleNodeJoin/GPU/SingleGPUJoin.cu
#             src/SingleNodeJoin/GPU/UnifiedMemoryGPUJoin.cu
#             src/GlobalJoin/ShipNone/ShipNoneGPUJoin.cu
#             src/GlobalJoin/ShipAll/ShipAllGPUJoin.cu
#             src/GlobalJoin/ShipPerfect/ShipPerfectGPUJoin.cu
#             src/AbstractJoins/AbstractGPUJoin.cu
#             src/GlobalJoin/ShipAll/ShipAll2GPUJoin.cu
#             src/GlobalJoin/ShipPerfect/ShipPerfect2GPUJoin.cu
#             src/Utilities/DataExchangeOperators/ReadWriteGPUOperator.cu
#             )
# else()
    
# endif()

add_executable(flinkApproachRDMA
        src/flink_approach_rdma.cpp
#         src/Benchmarks/Verbs/verbs_gpu_send_latency.cpp
        rdma_lib/Utilities/MPI_Helper_Fake.cpp
        rdma_lib/Utilities/ConnectionInfoProvider/SimpleInfoProvider.cpp
        rdma_lib/Utilities/VerbsConnection.cpp
        rdma_lib/Utilities/TimeTools.cpp
        rdma_lib/Utilities/BenchmarkTools.cpp
        rdma_lib/Utilities/ComputationParameters.cpp
        rdma_lib/Utilities/DeviceMemoryAllocator/CPUMemoryAllocator.cpp
        rdma_lib/Utilities/ConnectionCollection.cpp
        rdma_lib/Utilities/ConnectionInfoProvider/ConnectionUtilities.cpp
        rdma_lib/Utilities/DeviceMemoryAllocator/GPUMemoryAllocator.cu
    )
# target_link_libraries(GPUSendLatency ${CUDA_LIBS} infinity ibverbs numa json11)
target_link_libraries(flinkApproachRDMA infinity ibverbs numa json11 tbb)


add_executable(flinkApproachSingleNode
        src/flink_approach.cpp)
target_link_libraries(flinkApproachSingleNode tbb)

add_executable(noQueueGlobalSingleNode
        src/noQueueGlobal.cpp)
target_link_libraries(noQueueGlobalSingleNode tbb)

# add_library(rdma_lib SHARED
#     src/Benchmarks/Verbs/verbs_gpu_send_latency.cpp
#     src/Utilities/MPI_Helper_Fake.cpp
#     src/Utilities/ConnectionInfoProvider/SimpleInfoProvider.cpp
#     src/Utilities/VerbsConnection.cpp
#     src/Utilities/TimeTools.cpp
#     src/Utilities/BenchmarkTools.cpp
#     src/Utilities/ComputationParameters.cpp
#     src/Utilities/DeviceMemoryAllocator/CPUMemoryAllocator.cpp
#     src/Utilities/ConnectionCollection.cpp
#     src/Utilities/ConnectionInfoProvider/ConnectionUtilities.cpp
#     src/Utilities/DeviceMemoryAllocator/GPUMemoryAllocator.cu
# )
# set_target_properties(rdma_lib PROPERTIES SOVERSION 1)
# target_include_directories(rdma_lib PRIVATE include)
# target_include_directories(rdma_lib PRIVATE src)