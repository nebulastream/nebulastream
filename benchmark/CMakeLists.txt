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
    #-------------------------------# Benchmark Suite#-----------------------------

    ### Statemanager Benchmark ###
    add_executable(statemanager-benchmark "BenchmarkSuite/StatemanagerBenchmark.cpp")
    target_link_libraries(statemanager-benchmark nes ${GBENCHMARK_LIBRARIES})
    add_test(NAME statemanager-benchmark COMMAND statemanager-benchmark --benchmark_out=statemanager-benchmark.log --benchmark_out_format=json)

    ### Tuple Buffer Benchmark ###
    add_executable(tuplebuffer-benchmark "BenchmarkSuite/TupleBufferBenchmark.cpp")
    target_link_libraries(tuplebuffer-benchmark nes ${GBENCHMARK_LIBRARIES})
    add_test(NAME tuplebuffer-benchmark COMMAND tuplebuffer-benchmark --benchmark_out=tuplebuffer-benchmark.log --benchmark_out_format=json)

    ### Filter Queries Benchmark ###
    add_executable(filter-query-benchmark "BenchmarkSuite/FilterQueryBenchmarks.cpp")
    target_link_libraries(filter-query-benchmark nes)
    target_sources(filter-query-benchmark PUBLIC "src/util/BenchmarkUtils.cpp")
    add_test(NAME filter-query-benchmark COMMAND filter-query-benchmark --benchmark_out=filter-query-benchmark.log --benchmark_out_format=json)

    ### Filter Queries Benchmark ###
    add_executable(ysb-query "BenchmarkSuite/YSBSelect.cpp")
    target_link_libraries(ysb-query nes)
    target_sources(ysb-query PUBLIC "src/util/BenchmarkUtils.cpp")
    add_test(NAME ysb-query COMMAND ysb-query --benchmark_out=filter-query-benchmark.log --benchmark_out_format=json)

    ### Map Queries Benchmark ###
    add_executable(map-query-benchmark "BenchmarkSuite/MapQueryBenchmarks.cpp")
    target_link_libraries(map-query-benchmark nes)
    target_sources(map-query-benchmark PUBLIC "src/util/BenchmarkUtils.cpp")
    add_test(NAME map-query-benchmark COMMAND map-query-benchmark --benchmark_out=map-query-benchmark.log --benchmark_out_format=json)



    add_custom_target(run_benchmark COMMAND ${CMAKE_CTEST_COMMAND})

    message("---Done building benchmarks")


else(NES_BUILD_BENCHMARKS)
    message("---Not building benchmarks!")
endif (NES_BUILD_BENCHMARKS)