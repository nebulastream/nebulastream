# Google Benchmark Framework ----------------------------------------------------
include(ExternalProject)
ExternalProject_Add(
        gbenchmark
        URL https://github.com/google/benchmark/archive/v1.5.1.zip
        PREFIX ${CMAKE_CURRENT_BINARY_DIR}/gbenchmark
        # Disable install step
        INSTALL_COMMAND ""
        URL_HASH SHA256=3e266b49f73ee08625837ea5b1fabc4056b7f5e809b29c49670527326f4f4379
        CMAKE_ARGS -DBENCHMARK_DOWNLOAD_DEPENDENCIES=ON -DCMAKE_BUILD_TYPE=Release
)

# Get GBenchmark source and binary directories from CMake project
ExternalProject_Get_Property(gbenchmark source_dir binary_dir)

#Create a libgbenchmark target as a dependency by benchmark programs
add_library(libgbenchmark IMPORTED STATIC GLOBAL)
add_dependencies(libgbenchmark gbenchmark gbenchmark-main)
set_target_properties(libgbenchmark PROPERTIES
        "IMPORTED_LOCATION" "${binary_dir}/src/libbenchmark.a"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        )

#Create a libgbenchmark_main target as a dependency by benchmark programs
add_library(libgbenchmark_main IMPORTED STATIC GLOBAL)
add_dependencies(libgbenchmark_main gbenchmark gbenchmark-main)
set_target_properties(libgbenchmark_main PROPERTIES
        "IMPORTED_LOCATION" "${binary_dir}/src/libbenchmark_main.a"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        )



set(GBENCHMARK_LIBRARIES ${LIBRARIES} ${GRPC_LIBRARIES} libgbenchmark libgbenchmark_main)


#-------------------------------##################------------------------------
#-------------------------------# Benchmark Suite#-----------------------------

### SandBox Benchmark ###
add_executable(sandbox-benchmark "BenchmarkSuite/SandBoxBenchmark.cpp")
target_link_libraries(sandbox-benchmark  nes ${GBENCHMARK_LIBRARIES})
add_test(NAME sandbox-benchmark COMMAND sandbox-benchmark)