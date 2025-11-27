cmake_minimum_required(VERSION 3.15)
project(nes_kubernetes_operator)

set(CMAKE_CXX_STANDARD 17)

find_package(yaml-cpp REQUIRED)

include_directories(include)

add_executable(k8s_operator_test src/K8sQueryBuilder.cpp src/K8sTopologyBuilder.cppBuilder.cpp src/K8sYAMLSubmitter.cpp)

target_link_libraries(k8s_operator_test PRIVATE yaml-cpp::yaml-cpp)
