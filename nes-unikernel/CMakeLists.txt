
OPTION(UNIKERNEL_USE_UBSAN "OFF")
OPTION(UNIKERNEL_TRACE_PROXY_FUNCTIONS "OFF")
OPTION(UNIKERNEL_TRACE_PIPELINE_EXECUTION "OFF")
OPTION(UNIKERNEL_USE_LIB_DWARF "OFF")
OPTION(UNIKERNEL_USE_LTO "ON")
OPTION(UNIKERNEL_USE_CUSTOM_DEPS "OFF")
OPTION(UNIKERNEL_USE_KAFKA "OFF")

set(UNIKERNEL_COMPILE_OPTIONS "-DUNIKERNEL_LIB=1 -gdwarf-4")
set(UNIKERNEL_LINK_OPTIONS "-fuse-ld=lld")
set(UNIKERNEL_LIBS "")

if (UNIKERNEL_USE_TSAN)
    set(UNIKERNEL_LINK_OPTIONS ${UNIKERNEL_LINK_OPTIONS} "-fsanitize=thread")
    SET(UNIKERNEL_COMPILE_OPTIONS ${UNIKERNEL_COMPILE_OPTIONS} "-fsanitize=thread" "-g" "-fno-omit-frame-pointer")
elseif (UNIKERNEL_USE_UBSAN)
    set(UNIKERNEL_LINK_OPTIONS ${UNIKERNEL_LINK_OPTIONS} "-fsanitize=undefined")
    SET(UNIKERNEL_COMPILE_OPTIONS ${UNIKERNEL_COMPILE_OPTIONS} "-fsanitize=undefined" "-g" "-fno-omit-frame-pointer")
endif ()

if (UNIKERNEL_TRACE_PROXY_FUNCTIONS)
    message("-- Tracing Proxy functions")
    set(UNIKERNEL_COMPILE_OPTIONS ${UNIKERNEL_COMPILE_OPTIONS} "-DUNIKERNEL_TRACE_PROXY_FUNCTIONS")
endif ()

if (UNIKERNEL_TRACE_PIPELINE_EXECUTION)
    message("-- Tracing Pipeline Execution")
    set(UNIKERNEL_COMPILE_OPTIONS ${UNIKERNEL_COMPILE_OPTIONS} "-DUNIKERNEL_TRACE_PIPELINE_EXECUTION")
endif ()

if (UNIKERNEL_USE_LIB_DWARF)
    set(UNIKERNEL_LINK_OPTIONS ${UNIKERNEL_LINK_OPTIONS} "-ldw")
    SET(UNIKERNEL_COMPILE_OPTIONS ${UNIKERNEL_COMPILE_OPTIONS} "-DUSE_LIB_DWARF")
endif ()

if (UNIKERNEL_USE_KAFKA)
    SET(UNIKERNEL_LIBS ${UNIKERNEL_LIBS} cppkafka::cppkafka RdKafka::rdkafka RdKafka::rdkafka++)
    SET(UNIKERNEL_COMPILE_OPTIONS ${UNIKERNEL_COMPILE_OPTIONS} "-DUSE_KAFKA")
endif ()

if (UNIKERNEL_USE_CUSTOM_DEPS)
    find_package(scn CONFIG REQUIRED)
    find_package(smallfolly REQUIRED)
    find_package(double-conversion REQUIRED)
    set(UNIKERNEL_LIBS ${UNIKERNEL_LIBS} smallfolly)
    message("Unikernel use custom libs")
else ()
    FetchContent_Declare(
            scnlib
            GIT_REPOSITORY https://github.com/eliaskosunen/scnlib.git
            GIT_TAG bca530899335775139d866855b4c59eb0c22abc9 # 2.0.0
    )
    FetchContent_MakeAvailable(scnlib)
    message("Unikernel does not use custom libs")
    set(UNIKERNEL_LIBS ${UNIKERNEL_LIBS} Folly::folly)
endif ()

add_subdirectory(src)
get_source(unikernel-lib NES_UNIKERNEL_LIB_SOURCE_FILES)
add_library(unikernel-lib STATIC ${NES_UNIKERNEL_LIB_SOURCE_FILES})
if (UNIKERNEL_USE_LTO)
    SET(UNIKERNEL_COMPILE_OPTIONS ${UNIKERNEL_COMPILE_OPTIONS} "-flto")
endif ()

target_include_directories(unikernel-lib PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-catalogs/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-configurations/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>)
target_compile_options(unikernel-lib PRIVATE ${UNIKERNEL_COMPILE_OPTIONS}
        -Wno-error=unused-parameter
)
target_link_libraries(unikernel-lib PUBLIC spdlog::spdlog zmq-fork scn::scn ${UNIKERNEL_LIBS})


add_executable(pipeline-test tests/pipelineTest.cpp)
target_compile_options(pipeline-test PRIVATE ${UNIKERNEL_COMPILE_OPTIONS})
target_link_options(pipeline-test PRIVATE ${UNIKERNEL_LINK_OPTIONS})
target_link_libraries(pipeline-test PRIVATE unikernel-lib)
target_include_directories(pipeline-test PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-configurations/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>
)

add_subdirectory(benchmarks)

# Get a list of files matching the pattern
file(GLOB PIPELINE_FILES ${CMAKE_CURRENT_SOURCE_DIR}/pipelines/Pipeline*.hpp)
# Loop over the files
foreach (PIPELINE_FILE ${PIPELINE_FILES})
    # Extract the pipeline number from the filename
    string(REGEX MATCH "Pipeline([0-9]+)\\.hpp" PIPELINE_NUMBER ${PIPELINE_FILE})

    # Get the matched pipeline number
    set(PIPELINE_NUMBER ${CMAKE_MATCH_1})

    file(GLOB OPERATOR_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/operators/node${PIPELINE_NUMBER}/*.cpp)
    # Create an executable for each pipeline
    add_executable(unikernel${PIPELINE_NUMBER} src/main.cpp ${OPERATOR_SOURCES})

    if (UNIKERNEL_USE_LTO)
        SET(UNIKERNEL_COMPILE_OPTIONS ${UNIKERNEL_COMPILE_OPTIONS} "-flto")
    endif ()

    set_target_properties(unikernel${PIPELINE_NUMBER} PROPERTIES OUTPUT_NAME "unikernel${PIPELINE_NUMBER}")
    target_compile_options(unikernel${PIPELINE_NUMBER} PRIVATE ${UNIKERNEL_COMPILE_OPTIONS} "-include${CMAKE_CURRENT_SOURCE_DIR}/pipelines/Pipeline${PIPELINE_NUMBER}.hpp")
    target_link_options(unikernel${PIPELINE_NUMBER} PRIVATE ${UNIKERNEL_LINK_OPTIONS})
    target_compile_definitions(unikernel${PIPELINE_NUMBER} PRIVATE NES_LOGLEVEL_INFO)

    # Link libraries and include directories
    target_link_libraries(unikernel${PIPELINE_NUMBER} PRIVATE ${OPERATOR_FILES} unikernel-lib)
    target_include_directories(unikernel${PIPELINE_NUMBER} PRIVATE
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-catalogs/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-configurations/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>
    )
endforeach ()

add_executable(compile-time-schema-test tests/compileTimeSchemaTest.cpp)
target_link_libraries(compile-time-schema-test PRIVATE UnikernelTest UnikernelCommon)
target_include_directories(compile-time-schema-test PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-configurations/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>
)

find_package(benchmark REQUIRED)
add_executable(csv-parsing-benchmark tests/csvParsing.cpp)
target_link_libraries(csv-parsing-benchmark PRIVATE unikernel-lib benchmark::benchmark)
target_compile_options(csv-parsing-benchmark PRIVATE ${UNIKERNEL_COMPILE_OPTIONS})
target_link_options(csv-parsing-benchmark PRIVATE ${UNIKERNEL_LINK_OPTIONS})
set_property(TARGET csv-parsing-benchmark PROPERTY INTERPROCEDURAL_OPTIMIZATION ON)
target_include_directories(csv-parsing-benchmark PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-data-types/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-core/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-runtime/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-configurations/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-common/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../nes-operators/include>
)
