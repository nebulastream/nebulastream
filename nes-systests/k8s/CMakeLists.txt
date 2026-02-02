find_package(kubernetes CONFIG REQUIRED)
find_package(cjson CONFIG REQUIRED)
find_package(libwebsockets CONFIG REQUIRED)

add_executable(k8s_operator_test src/K8sQueryBuilder.cpp src/K8sTopologyBuilder.cpp src/K8sJSONSubmitter.cpp)

target_include_directories(k8s_operator_test
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(k8s_operator_test
        PRIVATE
        kubernetes::kubernetes
        cJSON
        websockets
)