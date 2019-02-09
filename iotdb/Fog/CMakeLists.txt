cmake_minimum_required(VERSION 3.1)
project(zmqtest)

set (CMAKE_CXX_STANDARD 11)
find_package(PkgConfig)
pkg_check_modules(PC_LIBZMQ QUIET libzmq)

find_library(zmq_lib NAMES zmq
             PATHS ${PC_LIBZMQ_LIBDIR} ${PC_LIBZMQ_LIBRARY_DIRS})

SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread")

if(zmq_lib)
    set(ZeroMQ_FOUND ON)
endif()

if(NOT ZeroMQ_FOUND)
    message(FATAL_ERROR "ZeroMQ was NOT found!")
endif()

add_executable(
  node.bin
  node.cpp
  metrics.cpp
  )

add_executable(
  master.bin
  master.cpp
  metrics.cpp
  )

target_link_libraries(node.bin
  ${zmq_lib}
  )
target_link_libraries(master.bin
  ${zmq_lib}
  )
