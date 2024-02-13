function(unikernel_benchmark arg1)
    MESSAGE(STATUS "Unikernel Benchmark ${arg1}")
    file(GLOB OPERATOR_SOURCES ${arg1}/operators/*.cpp)
    add_executable(${arg1} main.cpp ${OPERATOR_SOURCES})

    if (UNIKERNEL_USE_LTO)
        set_property(TARGET ${arg1} PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    endif ()

    target_compile_options(${arg1} PRIVATE ${UNIKERNEL_COMPILE_OPTIONS})
    target_link_options(${arg1} PRIVATE ${UNIKERNEL_LINK_OPTIONS})
    target_link_libraries(${arg1} PRIVATE unikernel-lib)
    target_compile_options(${arg1} PRIVATE ${UNIKERNEL_COMPILE_OPTIONS} "-include${CMAKE_CURRENT_SOURCE_DIR}/${arg1}/pipelines/Pipeline.hpp")
    target_include_directories(${arg1} PRIVATE
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-data-types/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-core/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-runtime/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-configurations/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-common/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../nes-operators/include>
    )
endfunction()


unikernel_benchmark(nexmark-q0)
unikernel_benchmark(nexmark-q5)
