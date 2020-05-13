cmake_minimum_required(VERSION 3.16.0)
project (NES CXX)

if(POLICY CMP0004)
  cmake_policy(SET CMP0004 OLD)
endif()

# Custome CMake find instructions and macros
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")
include(cmake/macros.cmake)
include(${CMAKE_ROOT}/Modules/ExternalProject.cmake)

# C++ Standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set NES Flags
set(NES_SPECIFIC_FLAGS)
if(CMAKE_NES_DEBUG_TUPLE_BUFFER_LEAKS)
    set(NES_SPECIFIC_FLAGS "-DNES_DEBUG_TUPLE_BUFFER_LEAKS=1")
endif()

# Set Optimization Flags
set(CMAKE_CXX_FLAGS "-Wall -g ${NES_SPECIFIC_FLAGS}")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DNO_RACE_CHECK -DNES_DEBUG_MODE=1")
set(CMAKE_CXX_FLAGS_RELEASE "-Wextra -O3")
#-Wno-unused-parameter -Wno-ignored-qualifiers -Wno-sign-compare -Wno-extra -Wno-reorder

# Compiler should produce specific code for system architecture
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -mtune=native -mavx")
endif()

message(STATUS "CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG}")
message(STATUS "CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE}")

# lcryto is required for asio to communicate
set(LIBRARIES ${LIBRARIES} -ldl -lcrypto -lssl grpc++)

# LLVM
find_package(LLVM 3 REQUIRED)
find_package(clang REQUIRED)

message("-- LLVM version ${LLVM_VERSION_STRING}")

# GRPC and protobuf

## Fetch and build source for grpc and protobuf and all dependencies
include(FetchContent)
FetchContent_Declare(
        gRPC
        GIT_REPOSITORY https://github.com/grpc/grpc
        GIT_TAG v1.28.0
)

set(FETCHCONTENT_QUIET OFF)
FetchContent_MakeAvailable(gRPC)

# Since FetchContent uses add_subdirectory under the hood, we can use
# the grpc targets directly from this build.
set(DgRPC_BUILD_TESTS:BOOL OFF)
set(_PROTOBUF_LIBPROTOBUF libprotobuf)
set(_REFLECTION grpc++_reflection)
set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc>)
set(_GRPC_GRPCPP grpc++)
if (CMAKE_CROSSCOMPILING)
    find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else ()
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)
endif ()

# Proto file
set(PROTO_PATH "${PROJECT_SOURCE_DIR}/grpc")
set(PROTO_FILE_PATH "${PROTO_PATH}/*.proto")

file(GLOB PROTO_FILES "${PROTO_FILE_PATH}")
foreach (PROTO_FILE ${PROTO_FILES})
    get_filename_component(FILENAME ${PROTO_FILE} "NAME")
    string(REGEX REPLACE "[.]proto$" ".pb.cc" OUTPUT_SOURCE ${CMAKE_CURRENT_BINARY_DIR}/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
    string(REGEX REPLACE "[.]proto$" ".pb.h" OUTPUT_SOURCE ${CMAKE_CURRENT_BINARY_DIR}/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
    string(REGEX REPLACE "[.]proto$" ".grpc.pb.cc" OUTPUT_SOURCE ${CMAKE_CURRENT_BINARY_DIR}/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
    string(REGEX REPLACE "[.]proto$" ".grpc.pb.h" OUTPUT_SOURCE ${CMAKE_CURRENT_BINARY_DIR}/${FILENAME})
    list(APPEND OUTPUT_SOURCES ${OUTPUT_SOURCE})
endforeach ()

message(STATUS "Found the following Proto files: " ${OUTPUT_SOURCES})

add_custom_command(
        OUTPUT ${OUTPUT_SOURCES}
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${PROTO_PATH}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${PROTO_FILE_PATH}"
        DEPENDS "${PROTO_FILE_PATH}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")
#FetchContent_Declare(
#        protobuf
#        GIT_REPOSITORY https://github.com/protocolbuffers/protobuf
#        GIT_TAG        v3.10.0
#)
#FetchContent_MakeAvailable(protobuf)
##find_package(Protobuf REQUIRED)

#file(GLOB PROTOBUF_DEFINITION_FILES "${PROTOBUF_INPUT_DIRECTORY}/*.proto")


#foreach(file ${PROTOBUF_DEFINITION_FILES})
#     message("file" ${file})
#
#    set(${example}_SRCS ${example}.cc)
#    set(${example}_PROTOS addressbook.proto)
#
#    #Code Generation
#    if(protobuf_MODULE_COMPATIBLE) #Legacy Support
#        protobuf_generate_cpp(${example}_PROTO_SRCS ${example}_PROTO_HDRS ${${example}_PROTOS})
#        list(APPEND ${example}_SRCS ${${example}_PROTO_SRCS} ${${example}_PROTO_HDRS})
#    endif()
#endforeach()
#include_directories(${Protobuf_INCLUDE_DIRS})
##include_directories(${CMAKE_CURRENT_BINARY_DIR})
##protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROJECT_SOURCE_DIR}/impl/Proto/CoordinatorRPCService.proto)
##protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS EXPORT_MACRO DLL_EXPORT ${PROJECT_SOURCE_DIR}/impl/Proto/CoordinatorRPCService.proto)
#protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS DESCRIPTORS PROTO_DESCS ${PROJECT_SOURCE_DIR}/impl/Proto/CoordinatorRPCService.proto)
##
#message(STATUS "Using protobuf ${Protobuf_VERSION}")
#
#message("_PROTOBUF_PROTOC=" << ${protobuf::protoc})
#set(PROTOBUF_INPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/impl/Proto")
#set(PROTOBUF_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/impl/Proto/")
#file(GLOB PROTOBUF_DEFINITION_FILES "${PROTOBUF_INPUT_DIRECTORY}/*.proto")
#message("PROTOBUF_DEFINITION_FILES=" "${PROTOBUF_DEFINITION_FILES}")
#
#foreach(file ${PROTOBUF_DEFINITION_FILES})
#    set(PROTOBUF_ARGUMENTS "/usr/bin/protoc --proto_path=\"${PROTOBUF_INPUT_DIRECTORY}\" --cpp_out=\"${PROTOBUF_OUTPUT_DIRECTORY}\" \"${file}\"")
#    message("PROTOBUF_ARGUMENTS" ${PROTOBUF_ARGUMENTS})
##    add_custom_command(COMMAND ${PROTOBUF_ARGUMENTS})
#    execute_process(COMMAND ${PROTOBUF_OUTPUT_DIRECTORY}
#            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
#            RESULT_VARIABLE PROTOBUF_RESULT
#            OUTPUT_VARIABLE PROTOBUF_OUTPUT_VARIABLE)
#
##
##    execute_process(COMMAND ${PROTOBUF_OUTPUT_DIRECTORY}
##            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
##            RESULT_VARIABLE PROTOBUF_RESULT
##            OUTPUT_VARIABLE PROTOBUF_OUTPUT_VARIABLE)
#endforeach()

#file(GLOB PROTOBUF_MODELS_INCLUDES "Models/Proto/*.pb.cc" "Models/Proto/*.hpp")

# ZMQ
find_package(ZMQ REQUIRED)
if(ZMQ_FOUND)
    message(STATUS "ZMQ found ${ZMQ_INCLUDE_DIRS}")
else(ZMQ_FOUND)
    message(ERROR "ZMQ not found")
endif(ZMQ_FOUND)
include_directories(${ZMQ_INCLUDE_DIRS})
set(LIBRARIES ${LIBRARIES} ${ZMQ_LIBRARIES})

# Log4cc
find_package(Log4cxx REQUIRED)
include_directories(${Log4cxx_INCLUDE_DIR})
set(LIBRARIES ${LIBRARIES} ${Log4cxx_LIBRARY})

if(LOG4CXX_FOUND)
    message(STATUS "LOG4CXX found")
else(LOG4CXX_FOUND)
    message(STATUS "LOG4CXX not found")
    message(STATUS "On Ubuntu, install the package by the following command!")
    message(STATUS "sudo apt-get install liblog4cxx10-dev")
endif(LOG4CXX_FOUND)


# TRAVIS-CI
if(TRAVIS_INCLUDE_DIR)
  include_directories(${TRAVIS_INCLUDE_DIR})
  message( STATUS "TRAVIS_INCLUDE_DIR:" ${TRAVIS_INCLUDE_DIR} )
endif()

# Boost Libraries
set(Boost_USE_MULTITHREADED TRUE)
find_package(Boost 1.65.1 REQUIRED system thread program_options filesystem serialization chrono) # Only check if lib is available on system for generated code.
link_directories(${Boost_LIBRARY_DIRS})
include_directories(${Boost_INCLUDE_DIRS})
set(LIBRARIES ${LIBRARIES} ${Boost_LIBRARIES})

# Backward

include(./cmake/BackwardConfig.cmake)
include_directories(${BACKWARD_INCLUDE_DIRS})
add_compile_definitions(${BACKWARD_DEFINITIONS})
link_directories(${BACKWARD_LIBRARIES})
set(LIBRARIES ${LIBRARIES} ${BACKWARD_LIBRARIES})


# Library containing dlopen and dlcose.
set(LIBRARIES ${LIBRARIES} ${CMAKE_DL_LIBS})

# C++ REST SDK
set(cpprestsdk_DIR ${CPPRESTSDK_DIR})
message("Found cpprestsdk in ${cpprestsdk_DIR}")
find_package(cpprestsdk REQUIRED)
# Library containing rest dependencies
set(LIBRARIES ${LIBRARIES} cpprestsdk::cpprest)

# Library containing actor framework
find_package(CAF COMPONENTS core io)
set(LIBRARIES ${LIBRARIES} ${CAF_EXTRA_LDFLAGS} ${CAF_LIBRARIES} ${PTHREAD_LIBRARIES})

# Library containing Kafka client
find_package(CppKafka REQUIRED)
set(LIBRARIES ${LIBRARIES} ${CppKafka_LIBRARIES})

# Print all used include directories
message(STATUS "INCLUDE_DIRS:")
get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
foreach(dir ${dirs})
    message(STATUS "  - ${dir}")
endforeach()

message(STATUS " Libraries: ${LIBRARIES}")

# Definition of runtime variables ######################################################################################

# Set definition values.
set(CLANG_EXECUTABLE ${LLVM_ROOT_DIR}/bin/clang++)
set(CLANG_FORMAT_EXECUTABLE ${LLVM_ROOT_DIR}/bin/clang-format)
set(GENERATED_CODE_DIRECTORY ${PROJECT_BINARY_DIR}/generated-code)
set(PATH_TO_NES_SOURCE_CODE ${PROJECT_SOURCE_DIR})

# Print definition values.
message( STATUS "Clang Executable: ${CLANG_EXECUTABLE}" )
message( STATUS "Clang-Format Executable: ${CLANG_FORMAT_EXECUTABLE}" )
message( STATUS "Generated Code Directory: ${GENERATED_CODE_DIRECTORY}" )

# Add definitions as compile flag.
add_definitions( -D 'CLANG_EXECUTABLE=\"${CLANG_EXECUTABLE}\"' )
add_definitions( -D 'CLANG_FORMAT_EXECUTABLE=\"${CLANG_FORMAT_EXECUTABLE}\"' )
add_definitions( -D 'GENERATED_CODE_DIRECTORY=\"${GENERATED_CODE_DIRECTORY}\"' )
add_definitions( -D 'PATH_TO_NES_SOURCE_CODE=\"${PATH_TO_NES_SOURCE_CODE}\"' )

add_definitions( -D 'QUERY_COMPILATION_CC=\"${QUERY_COMPILATION_CC}\"' )
add_definitions( -D 'CLANG_INCLUDE_DIRS=\"${CLANG_INCLUDE_DIRS}\"' )

# fix compile error with llvm (https://stackoverflow.com/a/18966387)
add_definitions( -D '__STDC_CONSTANT_MACROS')
add_definitions( -D '__STDC_LIMIT_MACROS' )


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
# $ORIGIN is to read the shared object from the installation directory
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath='$ORIGIN'")
#https://cmake.org/cmake/help/latest/variable/CMAKE_INSTALL_RPATH_USE_LINK_PATH.html
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH "true")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS}")

###########################################################################################

# Add Source Code
add_subdirectory(impl)

# Add Library
get_source_iotdb(iotdb_SOURCE_FILES)
get_header_iotdb(iotdb_HEADER_FILES)
add_library(iotdb SHARED ${iotdb_SOURCE_FILES} ${iotdb_HEADER_FILES} ${OUTPUT_SOURCES} ${OUTPUT_HEADERS})
target_include_directories(iotdb PUBLIC "include")
target_link_libraries(iotdb ${LIBRARIES} grpc++_reflection)
#target_link_libraries(iotdb ${LIBRARIES} ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})

# Documentation
add_subdirectory(docs)

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/QueryLib)

add_executable(nesCoordinator nesCoordinatorStarter.cpp)
target_link_libraries(nesCoordinator ${LIBRARIES} iotdb)
target_include_directories(nesCoordinator PUBLIC "include")

add_executable(nesWorker nesWorkerStarter.cpp)
target_link_libraries(nesWorker ${LIBRARIES} iotdb)
target_include_directories(nesWorker PUBLIC "include")

# Add tests with command
enable_testing()
add_subdirectory(tests)

configure_file(include/QueryLib/WindowManagerLib.hpp ${CMAKE_CURRENT_BINARY_DIR}/tests)
configure_file(include/QueryLib/WindowManagerLib.hpp ${CMAKE_CURRENT_BINARY_DIR})

install(TARGETS nesCoordinator nesWorker iotdb
        COMPONENT NebulaStream
        RUNTIME DESTINATION "/opt/local/nebula-stream"
        LIBRARY DESTINATION "/opt/local/nebula-stream"
        DESTINATION "/opt/local/nebula-stream"
        )

install(DIRECTORY "include" DESTINATION "/iotdb/" COMPONENT NebulaStream)

SET(CPACK_GENERATOR "DEB")
set(VERSION "0.0.1")
set(CPACK_PACKAGE_VERSION ${VERSION})
set(CPACK_PACKAGE_NAME "NebulaStream")
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_CONTACT "Team NebulaStream")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Team NebulaStream") #required
set(CPACK_PACKAGE_VENDOR "Team NebulaStream")
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}.${CMAKE_SYSTEM_PROCESSOR}")
INCLUDE(CPack)

project_enable_clang_format()

