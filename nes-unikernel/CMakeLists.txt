add_subdirectory(src)

get_source(unikernel-lib NES_UNIKERNEL_LIB_SOURCE_FILES)
set(UNIKERNEL_COMPILE_OPTIONS "-O0" "-std=c++20" "-Wno-error=deprecated" "-DUNIKERNEL_LIB=1" "-DWINDOW_OPERATOR=1")
set(UNIKERNEL_LINK_OPTIONS "-fuse-ld=lld")
if (UNIKERNEL_USE_TSAN)
    set(UNIKERNEL_LINK_OPTIONS ${UNIKERNEL_LINK_OPTIONS} "-fsanitize=thread")
    SET(UNIKERNEL_COMPILE_OPTIONS ${UNIKERNEL_COMPILE_OPTIONS} "-fsanitize=thread" "-g" "-fno-omit-frame-pointer")
endif ()

if(UNIKERNEL_USE_LIB_DWARF)
    set(UNIKERNEL_LINK_OPTIONS ${UNIKERNEL_LINK_OPTIONS} "-ldw")
    SET(UNIKERNEL_COMPILE_OPTIONS ${UNIKERNEL_COMPILE_OPTIONS} "-DUSE_LIB_DWARF")
endif ()

add_library(unikernel-lib STATIC ${NES_UNIKERNEL_LIB_SOURCE_FILES})
target_include_directories(unikernel-lib PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-catalogs/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-configurations/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>)
target_compile_options(unikernel-lib PRIVATE ${UNIKERNEL_COMPILE_OPTIONS})
target_link_libraries(unikernel-lib PUBLIC spdlog::spdlog zmq-fork Folly::folly cppkafka::cppkafka RdKafka::rdkafka RdKafka::rdkafka++)


add_executable(pipeline-test tests/pipelineTest.cpp)
target_compile_options(pipeline-test PRIVATE ${UNIKERNEL_COMPILE_OPTIONS})
target_link_options(pipeline-test PRIVATE ${UNIKERNEL_LINK_OPTIONS})
target_link_libraries(pipeline-test PRIVATE unikernel-lib)
target_include_directories(pipeline-test PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-catalogs/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-configurations/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>
)

SET(UNIKERNEL_WORKER_ID 0)

add_executable(unikernel0 src/main.cpp)
set_target_properties(unikernel0 PROPERTIES OUTPUT_NAME "unikernel${UNIKERNEL_WORKER_ID}")
target_compile_options(unikernel0 PRIVATE ${UNIKERNEL_COMPILE_OPTIONS} "-include${CMAKE_CURRENT_SOURCE_DIR}/pipelines/Pipeline0.hpp")

target_link_options(unikernel0 PRIVATE ${UNIKERNEL_LINK_OPTIONS})
file(GLOB OPERATOR_FILES ${CMAKE_CURRENT_SOURCE_DIR}/operators/*.o)
target_link_libraries(unikernel0 PRIVATE ${OPERATOR_FILES} unikernel-lib)
target_include_directories(unikernel0 PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-catalogs/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-configurations/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>
)

SET(UNIKERNEL_WORKER_ID 1)

add_executable(unikernel1 src/main.cpp)
set_target_properties(unikernel1 PROPERTIES OUTPUT_NAME "unikernel${UNIKERNEL_WORKER_ID}")
target_compile_options(unikernel1 PRIVATE ${UNIKERNEL_COMPILE_OPTIONS} "-include${CMAKE_CURRENT_SOURCE_DIR}/pipelines/Pipeline1.hpp")

target_link_options(unikernel1 PRIVATE ${UNIKERNEL_LINK_OPTIONS})
file(GLOB OPERATOR_FILES ${CMAKE_CURRENT_SOURCE_DIR}/operators/*.o)
target_link_libraries(unikernel1 PRIVATE ${OPERATOR_FILES} unikernel-lib)
target_include_directories(unikernel1 PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-catalogs/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-configurations/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>
)

SET(UNIKERNEL_WORKER_ID 2)

add_executable(unikernel2 src/main.cpp)
set_target_properties(unikernel2 PROPERTIES OUTPUT_NAME "unikernel${UNIKERNEL_WORKER_ID}")
target_compile_options(unikernel2 PRIVATE ${UNIKERNEL_COMPILE_OPTIONS} "-include${CMAKE_CURRENT_SOURCE_DIR}/pipelines/Pipeline2.hpp")

target_link_options(unikernel2 PRIVATE ${UNIKERNEL_LINK_OPTIONS})
file(GLOB OPERATOR_FILES ${CMAKE_CURRENT_SOURCE_DIR}/operators/*.o)
target_link_libraries(unikernel2 PRIVATE ${OPERATOR_FILES} unikernel-lib)
target_include_directories(unikernel2 PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-catalogs/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-configurations/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>
)