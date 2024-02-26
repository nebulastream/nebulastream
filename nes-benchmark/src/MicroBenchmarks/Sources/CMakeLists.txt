find_package(benchmark REQUIRED)
add_executable(CsvSourceBenchmark CsvSourceBenchmark.cpp)
target_link_libraries(CsvSourceBenchmark PUBLIC benchmark::benchmark nes-benchmark nes-coordinator-test-util)