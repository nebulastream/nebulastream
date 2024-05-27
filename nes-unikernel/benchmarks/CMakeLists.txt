function(unikernel_benchmark arg1)
    MESSAGE(STATUS "Unikernel Benchmark ${arg1}")
    set(TARGET_NAME "${arg1}-in-memory")
    file(GLOB OPERATOR_SOURCES ${arg1}/operators/*.cpp)
    add_executable(${TARGET_NAME} main.cpp ${OPERATOR_SOURCES})

    if (UNIKERNEL_USE_LTO)
        set_property(TARGET ${TARGET_NAME} PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    endif ()

    target_compile_options(${TARGET_NAME} PRIVATE ${UNIKERNEL_COMPILE_OPTIONS})
    target_compile_definitions(${TARGET_NAME} PRIVATE NES_LOGLEVEL_INFO=1 IN_MEMORY=1 INPUT_FILE_NAME="/home/ls/dima/nes-test-queries/data/bid.bin")
    target_link_options(${TARGET_NAME} PRIVATE ${UNIKERNEL_LINK_OPTIONS})
    target_link_libraries(${TARGET_NAME} PRIVATE unikernel-lib)
    target_compile_options(${TARGET_NAME} PRIVATE ${UNIKERNEL_COMPILE_OPTIONS} "-include${CMAKE_CURRENT_SOURCE_DIR}/${arg1}/pipelines/PipelineInMemory.hpp")
    target_include_directories(${TARGET_NAME} PRIVATE
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-data-types/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-core/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-runtime/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-configurations/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-common/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-operators/include>
    )

    file(GLOB OPERATOR_SOURCES ${arg1}/operators/*.cpp)
    set(TARGET_NAME "${arg1}-with-io")
    add_executable(${TARGET_NAME} main.cpp ${OPERATOR_SOURCES})

    if (UNIKERNEL_USE_LTO)
        set_property(TARGET ${TARGET_NAME} PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    endif ()

    target_compile_options(${TARGET_NAME} PRIVATE ${UNIKERNEL_COMPILE_OPTIONS})
    target_link_options(${TARGET_NAME} PRIVATE ${UNIKERNEL_LINK_OPTIONS})
    target_link_libraries(${TARGET_NAME} PRIVATE unikernel-lib)
    target_compile_options(${TARGET_NAME} PRIVATE ${UNIKERNEL_COMPILE_OPTIONS} "-include${CMAKE_CURRENT_SOURCE_DIR}/${arg1}/pipelines/PipelineWithIo.hpp")
    target_include_directories(${TARGET_NAME} PRIVATE
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-data-types/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-core/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-runtime/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-configurations/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-common/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-operators/include>
    )
endfunction()


#unikernel_benchmark(nexmark-q0)
unikernel_benchmark(nexmark-q5)
unikernel_benchmark(nexmark-q2)
