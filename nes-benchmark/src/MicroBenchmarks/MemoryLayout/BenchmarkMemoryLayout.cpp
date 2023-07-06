#include "API/Schema.hpp"
#include "Common/DataTypes/DataType.hpp"
#include "Runtime/BufferManager.hpp"
#include "Runtime/MemoryLayout/ColumnLayout.hpp"
#include "Runtime/MemoryLayout/RowLayout.hpp"
#include <Exceptions/ErrorListener.hpp>
#include <Runtime/MemoryLayout/CompressedDynamicTupleBuffer.hpp>
#include <benchmark/benchmark.h>
#include <fstream>
#include <nlohmann/json.hpp>

using namespace NES::Runtime::MemoryLayouts;

class ErrorHandler : public NES::Exceptions::ErrorListener {
  public:
    void onFatalError(int signalNumber, std::string callstack) override {
        std::cout << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack " << callstack;
    }

    void onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) override {
        std::cout << "onFatalException: exception=[" << exception->what() << "] callstack=\n" << callstack;
    }
};

class BenchmarkMemoryLayout {
  public:
    BenchmarkMemoryLayout(size_t numBuffers, size_t bufferSize);
    NES::Runtime::BufferManagerPtr bufferManager;
};

MemoryLayoutPtr createMemoryLayout(bool isRowLayout, size_t numColumns, size_t bufferSize) {
    NES::SchemaPtr schemaPtr = std::shared_ptr<NES::Schema>();
    if (numColumns == 1)
        schemaPtr = NES::Schema::create()->addField("t_0", NES::BasicType::UINT8);
    else if (numColumns == 2)
        schemaPtr = NES::Schema::create()->addField("t_0", NES::BasicType::UINT8)->addField("t_1", NES::BasicType::UINT8);
    else if (numColumns == 4)
        schemaPtr = NES::Schema::create()
                        ->addField("t_0", NES::BasicType::UINT8)
                        ->addField("t_1", NES::BasicType::UINT8)
                        ->addField("t_2", NES::BasicType::UINT8)
                        ->addField("t_3", NES::BasicType::UINT8);
    else if (numColumns == 8)
        schemaPtr = NES::Schema::create()
                        ->addField("t_0", NES::BasicType::UINT8)
                        ->addField("t_1", NES::BasicType::UINT8)
                        ->addField("t_2", NES::BasicType::UINT8)
                        ->addField("t_3", NES::BasicType::UINT8)
                        ->addField("t_4", NES::BasicType::UINT8)
                        ->addField("t_5", NES::BasicType::UINT8)
                        ->addField("t_6", NES::BasicType::UINT8)
                        ->addField("t_7", NES::BasicType::UINT8);
    else if (numColumns == 16)
        schemaPtr = NES::Schema::create()
                        ->addField("t_0", NES::BasicType::UINT8)
                        ->addField("t_1", NES::BasicType::UINT8)
                        ->addField("t_2", NES::BasicType::UINT8)
                        ->addField("t_3", NES::BasicType::UINT8)
                        ->addField("t_4", NES::BasicType::UINT8)
                        ->addField("t_5", NES::BasicType::UINT8)
                        ->addField("t_6", NES::BasicType::UINT8)
                        ->addField("t_7", NES::BasicType::UINT8)
                        ->addField("t_8", NES::BasicType::UINT8)
                        ->addField("t_9", NES::BasicType::UINT8)
                        ->addField("t_10", NES::BasicType::UINT8)
                        ->addField("t_11", NES::BasicType::UINT8)
                        ->addField("t_12", NES::BasicType::UINT8)
                        ->addField("t_13", NES::BasicType::UINT8)
                        ->addField("t_14", NES::BasicType::UINT8)
                        ->addField("t_15", NES::BasicType::UINT8);
    else {
        //NES_THROW_RUNTIME_ERROR() // todo
        std::cout << "Invalid number of columns." << std::endl;
        exit(-1);
    }

    if (isRowLayout) {
        return RowLayout::create(schemaPtr, bufferSize);
    } else {
        return ColumnLayout::create(schemaPtr, bufferSize);
    }
}

BenchmarkMemoryLayout::BenchmarkMemoryLayout(size_t numBuffers, size_t bufferSize) {
    NES::Logger::setupLogging("BenchmarkMemoryLayout.log", NES::LogLevel::LOG_DEBUG);
    auto listener = std::make_shared<ErrorHandler>();
    NES::Exceptions::installGlobalErrorListener(listener);
    bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, numBuffers);
    // write arguments to json
    nlohmann::json arguments = {{"NumBuffers", numBuffers}, {"BufferSize", bufferSize}};
    std::string file_name = "args.json";
    std::ofstream o(file_name);
    o << std::setw(2) << arguments << std::endl;
}

std::tuple<uint8_t> RECORD_1 = {65};
template<class... Args>
void BM_MemoryLayout_Write_1(benchmark::State& state, Args&&... args) {
    // parse arguments
    std::tuple args_tuple = std::make_tuple(std::move(args)...);
    size_t numBuffers = std::get<0>(args_tuple);
    size_t bufferSize = std::get<1>(args_tuple);
    size_t isRowLayout = std::get<2>(args_tuple);

    MemoryLayoutPtr memoryLayoutPtr = createMemoryLayout(isRowLayout, 1, bufferSize);
    BenchmarkMemoryLayout bench = BenchmarkMemoryLayout(numBuffers, bufferSize);
    double totalTime = 0;
    for (auto _ : state) {
        auto buffer = CompressedDynamicTupleBuffer(memoryLayoutPtr, bench.bufferManager->getBufferBlocking());
        auto start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < buffer.getCapacity() - 1; i++)
            buffer.pushRecordToBuffer(RECORD_1);
        auto end = std::chrono::high_resolution_clock::now();
        totalTime += std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
    }
    state.counters["TotalTime"] = (double) totalTime / (double) state.iterations();
}

std::tuple<uint8_t, uint8_t> RECORD_2 = {65, 66};
template<class... Args>
void BM_MemoryLayout_Write_2(benchmark::State& state, Args&&... args) {
    // parse arguments
    std::tuple args_tuple = std::make_tuple(std::move(args)...);
    size_t numBuffers = std::get<0>(args_tuple);
    size_t bufferSize = std::get<1>(args_tuple);
    size_t isRowLayout = std::get<2>(args_tuple);

    MemoryLayoutPtr memoryLayoutPtr = createMemoryLayout(isRowLayout, 2, bufferSize);
    BenchmarkMemoryLayout bench = BenchmarkMemoryLayout(numBuffers, bufferSize);
    double totalTime = 0;
    for (auto _ : state) {
        auto buffer = CompressedDynamicTupleBuffer(memoryLayoutPtr, bench.bufferManager->getBufferBlocking());
        auto start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < buffer.getCapacity() - 1; i++)
            buffer.pushRecordToBuffer(RECORD_2);
        auto end = std::chrono::high_resolution_clock::now();
        totalTime += std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
    }
    state.counters["TotalTime"] = (double) totalTime / (double) state.iterations();
}

std::tuple<uint8_t, uint8_t, uint8_t, uint8_t> RECORD_4 = {65, 66, 67, 68};
template<class... Args>
void BM_MemoryLayout_Write_4(benchmark::State& state, Args&&... args) {
    // parse arguments
    std::tuple args_tuple = std::make_tuple(std::move(args)...);
    size_t numBuffers = std::get<0>(args_tuple);
    size_t bufferSize = std::get<1>(args_tuple);
    size_t isRowLayout = std::get<2>(args_tuple);

    MemoryLayoutPtr memoryLayoutPtr = createMemoryLayout(isRowLayout, 4, bufferSize);
    BenchmarkMemoryLayout bench = BenchmarkMemoryLayout(numBuffers, bufferSize);
    double totalTime = 0;
    for (auto _ : state) {
        auto buffer = CompressedDynamicTupleBuffer(memoryLayoutPtr, bench.bufferManager->getBufferBlocking());
        auto start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < buffer.getCapacity() - 1; i++)
            buffer.pushRecordToBuffer(RECORD_4);
        auto end = std::chrono::high_resolution_clock::now();
        totalTime += std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
    }
    state.counters["TotalTime"] = (double) totalTime / (double) state.iterations();
}

std::tuple<uint8_t, uint8_t, uint8_t, uint8_t, uint8_t, uint8_t, uint8_t, uint8_t> RECORD_8 = {65, 66, 67, 68, 69, 70, 71, 72};
template<class... Args>
void BM_MemoryLayout_Write_8(benchmark::State& state, Args&&... args) {
    // parse arguments
    std::tuple args_tuple = std::make_tuple(std::move(args)...);
    size_t numBuffers = std::get<0>(args_tuple);
    size_t bufferSize = std::get<1>(args_tuple);
    size_t isRowLayout = std::get<2>(args_tuple);

    MemoryLayoutPtr memoryLayoutPtr = createMemoryLayout(isRowLayout, 8, bufferSize);
    BenchmarkMemoryLayout bench = BenchmarkMemoryLayout(numBuffers, bufferSize);
    double totalTime = 0;
    for (auto _ : state) {
        auto buffer = CompressedDynamicTupleBuffer(memoryLayoutPtr, bench.bufferManager->getBufferBlocking());
        auto start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < buffer.getCapacity() - 1; i++)
            buffer.pushRecordToBuffer(RECORD_8);
        auto end = std::chrono::high_resolution_clock::now();
        totalTime += std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
    }
    state.counters["TotalTime"] = (double) totalTime / (double) state.iterations();
}

std::tuple<uint8_t,
           uint8_t,
           uint8_t,
           uint8_t,
           uint8_t,
           uint8_t,
           uint8_t,
           uint8_t,
           uint8_t,
           uint8_t,
           uint8_t,
           uint8_t,
           uint8_t,
           uint8_t,
           uint8_t,
           uint8_t>
    RECORD_16 = {65, 66, 67, 68, 69, 70, 71, 72, 65, 66, 67, 68, 69, 70, 71, 72};
template<class... Args>
void BM_MemoryLayout_Write_16(benchmark::State& state, Args&&... args) {
    // parse arguments
    std::tuple args_tuple = std::make_tuple(std::move(args)...);
    size_t numBuffers = std::get<0>(args_tuple);
    size_t bufferSize = std::get<1>(args_tuple);
    size_t isRowLayout = std::get<2>(args_tuple);

    MemoryLayoutPtr memoryLayoutPtr = createMemoryLayout(isRowLayout, 16, bufferSize);
    BenchmarkMemoryLayout bench = BenchmarkMemoryLayout(numBuffers, bufferSize);
    double totalTime = 0;
    for (auto _ : state) {
        auto buffer = CompressedDynamicTupleBuffer(memoryLayoutPtr, bench.bufferManager->getBufferBlocking());
        auto start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < buffer.getCapacity() - 1; i++)
            buffer.pushRecordToBuffer(RECORD_16);
        auto end = std::chrono::high_resolution_clock::now();
        totalTime += std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
    }
    state.counters["TotalTime"] = (double) totalTime / (double) state.iterations();
}

// ------------------------------------------------------------
// Benchmark Parameters
// have to be changed by hand (except compression algorithm) ...
// ------------------------------------------------------------
// TODO? use environment variables for all variables?
size_t NUM_BENCHMARK_ITERATIONS = 1000;
size_t NUM_BUFFERS = 1;
size_t BUFFER_SIZE = std::atoll(std::getenv("BUF_SIZE"));

// Note: only change above values and none from below!

// ------------------------------------------------------------
// RowLayout
// ------------------------------------------------------------
BENCHMARK_CAPTURE(BM_MemoryLayout_Write_1, RowLayout1, NUM_BUFFERS, BUFFER_SIZE, true)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

BENCHMARK_CAPTURE(BM_MemoryLayout_Write_2, RowLayout2, NUM_BUFFERS, BUFFER_SIZE, true)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

BENCHMARK_CAPTURE(BM_MemoryLayout_Write_4, RowLayout4, NUM_BUFFERS, BUFFER_SIZE, true)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

BENCHMARK_CAPTURE(BM_MemoryLayout_Write_8, RowLayout8, NUM_BUFFERS, BUFFER_SIZE, true)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

BENCHMARK_CAPTURE(BM_MemoryLayout_Write_16, RowLayout16, NUM_BUFFERS, BUFFER_SIZE, true)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

// ------------------------------------------------------------
// Column Layout
// ------------------------------------------------------------
BENCHMARK_CAPTURE(BM_MemoryLayout_Write_1, ColumnLayout1, NUM_BUFFERS, BUFFER_SIZE, false)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

BENCHMARK_CAPTURE(BM_MemoryLayout_Write_2, ColumnLayout2, NUM_BUFFERS, BUFFER_SIZE, false)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

BENCHMARK_CAPTURE(BM_MemoryLayout_Write_4, ColumnLayout4, NUM_BUFFERS, BUFFER_SIZE, false)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

BENCHMARK_CAPTURE(BM_MemoryLayout_Write_8, ColumnLayout8, NUM_BUFFERS, BUFFER_SIZE, false)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

BENCHMARK_CAPTURE(BM_MemoryLayout_Write_16, ColumnLayout16, NUM_BUFFERS, BUFFER_SIZE, false)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

BENCHMARK_MAIN();