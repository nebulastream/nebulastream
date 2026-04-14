cmake_minimum_required(VERSION 3.14)
project(datagen LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_definitions(-DASIO_STANDALONE)

set(ASIO_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/asio-1.30.2/include")
set(MOSQUITTO_LIB_DIR "${CMAKE_SOURCE_DIR}/include/mosquitto-2.0.21/lib")
set(MOSQUITTO_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/include/mosquitto-2.0.21/include")
set(YAML_CPP_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/include")

#find_package(yaml-cpp QUIET)
#if(NOT yaml-cpp_FOUND)
#    message(STATUS "yaml-cpp not found system-wide; using headers in ${CMAKE_SOURCE_DIR}/include")
#    add_library(yaml-cpp INTERFACE)
#    target_include_directories(yaml-cpp INTERFACE ${CMAKE_SOURCE_DIR}/include)
#endif()

include_directories(${ASIO_INCLUDE_DIR} ${YAML_CPP_INCLUDE_DIR} ${MOSQUITTO_INCLUDE_DIR})

add_executable(datagen datagen_batch_new.cpp
        configLoader.hpp
        configLoader.cpp
        datagen_batch_new.cpp)

add_executable(datagen_shared datagen_batch_shared.cpp
        configLoader.hpp
        configLoader.cpp
        datagen_batch_shared.cpp)

target_compile_definitions(datagen PRIVATE ASIO_STANDALONE)
target_compile_definitions(datagen_shared PRIVATE ASIO_STANDALONE)
target_include_directories(datagen PRIVATE
        ${ASIO_INCLUDE_DIR}
        ${MOSQUITTO_INCLUDE_DIR}
)
target_include_directories(datagen_shared PRIVATE
        ${ASIO_INCLUDE_DIR}
        ${MOSQUITTO_INCLUDE_DIR}
)

target_link_directories(datagen PRIVATE ${MOSQUITTO_LIB_DIR})
target_link_directories(datagen_shared PRIVATE ${MOSQUITTO_LIB_DIR})

target_link_libraries(datagen PRIVATE
        pthread
        yaml-cpp
        mosquitto)
target_link_libraries(datagen_shared PRIVATE
        pthread
        yaml-cpp
        mosquitto)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(datagen PRIVATE -Wall -Wextra -pedantic)
    target_compile_options(datagen_shared PRIVATE -Wall -Wextra -pedantic)
elseif (MSVC)
    target_compile_options(datagen PRIVATE /W4)
    target_compile_options(datagen_shared PRIVATE /W4)
endif()
