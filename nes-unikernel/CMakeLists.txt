add_subdirectory(src)

get_source(unikernel-lib NES_UNIKERNEL_LIB_SOURCE_FILES)
set(UNIKERNEL_COMPILE_OPTIONS "-O3" "-std=c++20" "-Wno-error=deprecated" "-DUNIKERNEL_LIB=1" "-DWINDOW_OPERATOR=1")
set(UNIKERNEL_LINK_OPTIONS "-fuse-ld=lld")
if (UNIKERNEL_USE_TSAN)
    set(UNIKERNEL_LINK_OPTIONS ${UNIKERNEL_LINK_OPTIONS} "-fsanitize=thread")
    SET(UNIKERNEL_COMPILE_OPTIONS ${UNIKERNEL_COMPILE_OPTIONS} "-fsanitize=thread" "-g" "-fno-omit-frame-pointer")
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
target_link_libraries(unikernel-lib PUBLIC spdlog::spdlog zmq-fork Folly::folly)

SET(UNIKERNEL_WORKER_ID 0)
SET(UNIKERNEL_CONFIGURATION_FILE /home/ls/DIMA/nebulastream/cmake-build-unikernelexport-clang-16-debug/nes-unikernel-export/export.yaml)

add_executable(unikernel src/main.cpp)
set_target_properties(unikernel PROPERTIES OUTPUT_NAME "unikernel${UNIKERNEL_WORKER_ID}")
target_compile_options(unikernel PRIVATE ${UNIKERNEL_COMPILE_OPTIONS} "-include${CMAKE_CURRENT_SOURCE_DIR}/include/Pipeline.h")

target_link_options(unikernel PRIVATE ${UNIKERNEL_LINK_OPTIONS})
file(GLOB OPERATOR_FILES ${CMAKE_CURRENT_SOURCE_DIR}/operators/*.o)
target_link_libraries(unikernel PRIVATE ${OPERATOR_FILES} unikernel-lib)
target_include_directories(unikernel PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-catalogs/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-configurations/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>
)