cmake_minimum_required(VERSION 3.4)

project(MAMPI CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3 -Wall -ansi -pthread -lpthread -ltbb -fopenmp -lboost_program_options")
#set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -pg -pthread -lpthread -ltbb -fopenmp -lboost_program_options")
#set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -pthread -lpthread -ltbb -fopenmp -lboost_program_options")

# Compiler should produce specific code for system architecture
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-march=native -mtune=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
	message("use mtune")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -mtune=native -mavx2")
endif()

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

find_package(Boost REQUIRED filesystem system program_options)
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


set( CMAKE_VERBOSE_MAKEFILE on )

add_executable(LM_PC
        src/LM_PC.cpp
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
target_link_libraries(LM_PC infinity ibverbs numa json11 tbb boost_program_options)

add_executable(LM_Ponly
        src/LM_Ponly.cpp
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
target_link_libraries(LM_Ponly infinity ibverbs numa json11 tbb boost_program_options)


add_executable(YSB_PART
        src/YSB_PART.cpp
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
target_link_libraries(YSB_PART infinity ibverbs numa json11 tbb boost_program_options)

add_executable(numatest
        src/numa.cpp
    )
target_link_libraries(numatest infinity ibverbs numa json11 tbb boost_program_options)

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