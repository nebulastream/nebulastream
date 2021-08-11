# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Google Benchmark Framework ----------------------------------------------------

if (NES_BUILD_BENCHMARKS)
    message("---Building benchmarks...")
    include(ExternalProject)
    ExternalProject_Add(
            gbenchmark
            URL https://github.com/google/benchmark/archive/v1.5.1.zip
            PREFIX ${CMAKE_CURRENT_BINARY_DIR}/gbenchmark
            # Disable install step
            INSTALL_COMMAND ""
            URL_HASH SHA256=3e266b49f73ee08625837ea5b1fabc4056b7f5e809b29c49670527326f4f4379
            #Line below is only necessary if someone wants to run the provided tests by gtests
            #CMAKE_ARGS -DBENCHMARK_DOWNLOAD_DEPENDENCIES=ON -DCMAKE_BUILD_TYPE=Release
            CMAKE_ARGS -DBENCHMARK_ENABLE_GTEST_TESTS=OFF -DCMAKE_BUILD_TYPE=Release
    )

#    find_package(OpenMP)
#    if (OPENMP_FOUND)
#        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
#        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
#        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
#    endif()

    # Get GBenchmark source and binary directories from CMake project
    ExternalProject_Get_Property(gbenchmark source_dir binary_dir)
    message("gbenchmark ${source_dir} ${binary_dir}")

    #Create a libgbenchmark target as a dependency by benchmark programs
    add_library(libgbenchmark IMPORTED STATIC GLOBAL)
    add_dependencies(libgbenchmark gbenchmark gbenchmark_main)

    set_target_properties(libgbenchmark PROPERTIES
            "IMPORTED_LOCATION" "${binary_dir}/src/libbenchmark.a"
            "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
            )

    #Create a libgbenchmark_main target as a dependency by benchmark programs
    add_library(libgbenchmark_main IMPORTED STATIC GLOBAL)
    add_dependencies(libgbenchmark_main gbenchmark_main)

    set_target_properties(libgbenchmark_main PROPERTIES
            "IMPORTED_LOCATION" "${binary_dir}/src/libbenchmark_main.a"
            "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
            )

    set(GBENCHMARK_LIBRARIES ${LIBRARIES} ${GRPC_LIBRARIES} libgbenchmark libgbenchmark_main)
    include_directories("${source_dir}/include/")
    include_directories("include/")

    #-------------------------------##################------------------------------
    #-------------------------------# Micro Benchmark Suite#-----------------------------

    ### Statemanager Benchmark ###
#    add_executable(statemanager-benchmark "src/Micro-Benchmarks/StatemanagerBenchmark.cpp")
#    target_link_libraries(statemanager-benchmark nes ${GBENCHMARK_LIBRARIES})
#    add_test(NAME statemanager-benchmark COMMAND statemanager-benchmark --benchmark_out=statemanager-benchmark.log --benchmark_out_format=json)

    ### MemorySource Benchmark ###
    add_executable(memorysource-benchmark "src/Micro-Benchmarks/BenchmarkMemorySource.cpp")
    target_link_libraries(memorysource-benchmark nes ${GBENCHMARK_LIBRARIES})
    add_test(NAME memorysource-benchmark COMMAND memorysource-benchmark --benchmark_out=memorysource-benchmark.log --benchmark_out_format=json)

#
#    ### Benchmark Uniform Data Benchmark ###
#    add_executable(uniformdata-benchmark "src/Micro-Benchmarks/BenchmarkCreateUniformData.cpp")
#    target_link_libraries(uniformdata-benchmark nes ${GBENCHMARK_LIBRARIES})
#    target_sources(uniformdata-benchmark PUBLIC "src/util/BenchmarkUtils.cpp")
#    add_test(NAME uniformdata-benchmark COMMAND uniformdata-benchmark --benchmark_out=uniformdata-benchmark.log --benchmark_out_format=json)
#
#    ### Tuple Buffer Benchmark ###
#    add_executable(tuplebuffer-benchmark "src/Micro-Benchmarks/TupleBufferBenchmark.cpp")
#    target_link_libraries(tuplebuffer-benchmark nes ${GBENCHMARK_LIBRARIES})
#    add_test(NAME tuplebuffer-benchmark COMMAND tuplebuffer-benchmark --benchmark_out=tuplebuffer-benchmark.log --benchmark_out_format=json)
#
#    ### Filter Queries Benchmark ###
#    add_executable(filter-query-benchmark "src/Micro-Benchmarks/FilterQueryBenchmarks.cpp")
#    target_link_libraries(filter-query-benchmark nes)
#    target_sources(filter-query-benchmark PUBLIC "src/util/BenchmarkUtils.cpp")
#    add_test(NAME filter-query-benchmark COMMAND filter-query-benchmark --benchmark_out=filter-query-benchmark.log --benchmark_out_format=json)
#
#    ### Filter Queries Benchmark ###
#    add_executable(ysb-query "src/Micro-Benchmarks/YSBSelect.cpp")
#    target_link_libraries(ysb-query nes)
#    target_sources(ysb-query PUBLIC "src/util/BenchmarkUtils.cpp")
#    add_test(NAME ysb-query COMMAND ysb-query --benchmark_out=filter-query-benchmark.log --benchmark_out_format=json)
#
#    ### Map Queries Benchmark ###
#    add_executable(map-query-benchmark "src/Micro-Benchmarks/MapQueryBenchmarks.cpp")
#    target_link_libraries(map-query-benchmark nes)
#    target_sources(map-query-benchmark PUBLIC "src/util/BenchmarkUtils.cpp")
#    add_test(NAME map-query-benchmark COMMAND map-query-benchmark --benchmark_out=map-query-benchmark.log --benchmark_out_format=json)
#
#    ### BufferSize Query Benchmark ###
#    add_executable(buffer-size-query-benchmark "src/Micro-Benchmarks/QueryBufferSizesBenchmark.cpp")
#    target_link_libraries(buffer-size-query-benchmark nes)
#    target_sources(buffer-size-query-benchmark PUBLIC "src/util/BenchmarkUtils.cpp")
#    add_test(NAME buffer-size-query-benchmark COMMAND buffer-size-query-benchmark --benchmark_out=buffer-size-query-benchmark.log --benchmark_out_format=json)
#
#    ### TupleSize Query Benchmark ###
#    add_executable(tuple-size-query-benchmark "src/Micro-Benchmarks/QueryTupleSizesBenchmark.cpp")
#    target_link_libraries(tuple-size-query-benchmark nes)
#    target_sources(tuple-size-query-benchmark PUBLIC "src/util/BenchmarkUtils.cpp")
#    add_test(NAME tuple-size-query-benchmark COMMAND tuple-size-query-benchmark --benchmark_out=tuple-size-query-benchmark.log --benchmark_out_format=json)

    ### Memory Layout Benchmark ###
    #add_executable(memory-layout-benchmark "src/Micro-Benchmarks/BenchmarkMemoryLayout.cpp")
    #target_link_libraries(memory-layout-benchmark nes ${GBENCHMARK_LIBRARIES})
    #add_test(NAME memory-layout-benchmark COMMAND memory-layout-benchmark --benchmark_out=memory-layout-benchmark.log --benchmark_out_format=json)

    ### Dynamic Memory Layout Benchmark ###
    #add_executable(dynamic-memory-layout-benchmark "src/Micro-Benchmarks/BenchmarkDynamicMemoryLayout.cpp")
    #target_link_libraries(dynamic-memory-layout-benchmark nes ${GBENCHMARK_LIBRARIES})
    #add_test(NAME dynamic-memory-layout-benchmark COMMAND dynamic-memory-layout-benchmark --benchmark_out=dynamic-memory-layout-benchmark.log --benchmark_out_format=json)

    ### Query Merger Benchmark ###
#    add_executable(query-merger-benchmark "src/Micro-Benchmarks/Query-Merger/BenchmarkQueryMerger.cpp")
#    target_link_libraries(query-merger-benchmark nes ${GBENCHMARK_LIBRARIES})
#    add_test(NAME query-merger-benchmark COMMAND query-merger-benchmark --benchmark_out=query-merger-benchmark.log --benchmark_out_format=json)

    message("---Done building benchmarks")

else (NES_BUILD_BENCHMARKS)
    message("---Not building benchmarks!")
endif ()