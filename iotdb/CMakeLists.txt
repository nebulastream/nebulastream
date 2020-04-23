cmake_minimum_required(VERSION 3.13.4)
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
set(LIBRARIES ${LIBRARIES} -ldl -lcrypto -lssl)

# LLVM
find_package(LLVM 3 REQUIRED)
find_package(clang REQUIRED)


message("-- LLVM version ${LLVM_VERSION_STRING}")

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
add_library(iotdb SHARED ${iotdb_SOURCE_FILES} ${iotdb_HEADER_FILES} impl/Optimizer/QueryRewrite/FilterPushDownRule.cpp include/Optimizer/QueryRewrite/FilterPushDownRule.hpp impl/Optimizer/QueryRewrite/BaseRewriteRule.cpp include/Optimizer/QueryRewrite/BaseRewriteRule.hpp)
target_include_directories(iotdb PUBLIC "include")
target_link_libraries(iotdb ${LIBRARIES})

# Documentation
add_subdirectory(docs)

#file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/QueryLib)

add_executable(nesCoordinator nesCoordinatorStarter.cpp)
target_link_libraries(nesCoordinator ${LIBRARIES} iotdb)
target_include_directories(nesCoordinator PUBLIC "include")

add_executable(nesWorker nesWorkerStarter.cpp)
target_link_libraries(nesWorker ${LIBRARIES} iotdb)
target_include_directories(nesWorker PUBLIC "include")

# Add tests with command
enable_testing()
add_subdirectory(tests)

configure_file(include/QueryLib/WindowManagerLib.hpp  ${CMAKE_CURRENT_BINARY_DIR}/tests)
configure_file(include/QueryLib/WindowManagerLib.hpp  ${CMAKE_CURRENT_BINARY_DIR})

project_enable_clang_format()

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
