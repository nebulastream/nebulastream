#include "API/Schema.hpp"
#include "DataGeneration/ByteDataGenerator.hpp"
#include "DataGeneration/DefaultDataGenerator.hpp"
#include <Exceptions/ErrorListener.hpp>
#include <Exceptions/SignalHandling.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/CompressedDynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <benchmark/benchmark.h>
#include <chrono>
#include <fstream>
#include <gmock/gmock-matchers.h>
#include <nlohmann/json.hpp>

using namespace NES::Benchmark::DataGeneration;
using namespace NES::Runtime::MemoryLayouts;

enum MemoryLayout_ { ROW, COLUMN };

const char* getMemoryLayoutName(enum MemoryLayout_ ml) {
    switch (ml) {
        case ROW: return "Row";
        case COLUMN: return "Column";
    }
}

class ErrorHandler : public NES::Exceptions::ErrorListener {
  public:
    void onFatalError(int signalNumber, std::string callstack) override {
        std::cout << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack " << callstack;
    }

    void onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) override {
        std::cout << "onFatalException: exception=[" << exception->what() << "] callstack=\n" << callstack;
    }
};

class BenchmarkCompression {
  public:
    BenchmarkCompression(size_t numBuffers,
                         size_t bufferSize,
                         DataGenerator& dataGenerator,
                         const std::shared_ptr<MemoryLayout>& memoryLayout,
                         CompressionMode cm);
    size_t getNumberOfBuffers();
    std::tuple<size_t, double> compress(CompressionAlgorithm ca);
    void decompress();

  private:
    size_t numBuffers{};
    size_t bufferSize{};
    std::vector<CompressedDynamicTupleBuffer> buffers;
};
BenchmarkCompression::BenchmarkCompression(const size_t numBuffers,
                                           const size_t bufferSize,
                                           DataGenerator& dataGenerator,
                                           const std::shared_ptr<MemoryLayout>& memoryLayout,
                                           CompressionMode cm) {
    NES::Logger::setupLogging("BenchmarkCompression.log", NES::LogLevel::LOG_DEBUG);
    auto listener = std::make_shared<ErrorHandler>();
    NES::Exceptions::installGlobalErrorListener(listener);

    this->numBuffers = numBuffers;
    this->bufferSize = bufferSize;
    NES::Runtime::BufferManagerPtr bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, numBuffers);

    // create data
    dataGenerator.setBufferManager(bufferManager);
    auto tmpBuffers = dataGenerator.createData(numBuffers, bufferSize);

    buffers.reserve(numBuffers);
    for (const auto& tmpBuffer : tmpBuffers) {
        buffers.emplace_back(memoryLayout, tmpBuffer, cm);
    }
}
size_t BenchmarkCompression::getNumberOfBuffers() { return numBuffers; }

std::tuple<size_t, double> BenchmarkCompression::compress(CompressionAlgorithm ca) {
    double totalCompressionRatio = 0;
    size_t totalBufferSize = 0;
    for (auto& buffer : buffers) {
        buffer.compress(ca);
        //std::cout << buffer.getCompressionRatio() << std::endl;
        totalCompressionRatio += buffer.getCompressionRatio();
        totalBufferSize += buffer.getTotalCompressedSize();
    }
    return std::make_tuple(totalBufferSize, totalCompressionRatio);
}

void BenchmarkCompression::decompress() {
    for (auto& buffer : buffers) {
        buffer.decompress();
    }
}

template<class... Args>
void BM_Binomial(benchmark::State& state, Args&&... args) {
    // parse arguments
    std::tuple args_tuple = std::make_tuple(std::move(args)...);
    MemoryLayout_ memoryLayout_ = std::get<0>(args_tuple);
    NES::Schema::MemoryLayoutType memoryLayoutType;
    switch (memoryLayout_) {
        case MemoryLayout_::ROW: {
            memoryLayoutType = NES::Schema::MemoryLayoutType::ROW_LAYOUT;
            break;
        }
        case MemoryLayout_::COLUMN: {
            memoryLayoutType = NES::Schema::MemoryLayoutType::COLUMNAR_LAYOUT;
            break;
        }
    }
    CompressionMode compressionMode = std::get<1>(args_tuple);
    CompressionAlgorithm compressionAlgorithm = std::get<2>(args_tuple);
    size_t seed = std::get<3>(args_tuple);
    bool sort = std::get<4>(args_tuple);
    size_t numBuffers = std::get<5>(args_tuple);
    size_t bufferSize = std::get<6>(args_tuple);
    size_t numCols = std::get<7>(args_tuple);
    size_t minValue = std::get<7>(args_tuple);
    size_t maxValue = std::get<9>(args_tuple);
    double probability = std::get<10>(args_tuple);
    // write arguments to json
    nlohmann::json arguments = {{"Distribution", "Binomial"},
                                {"MemoryLayout", getMemoryLayoutName(memoryLayout_)},
                                {"CompressionMode", getCompressionModeName(compressionMode)},
                                {"CompressionAlgorithm", getCompressionAlgorithmName(compressionAlgorithm)},
                                {"Seed", seed},
                                {"Sort", sort},
                                {"NumBuffers", numBuffers},
                                {"BufferSize", bufferSize},
                                {"NumCols", numCols},
                                {"MinValue", minValue},
                                {"MaxValue", maxValue},
                                {"Probability", probability}};
    std::string file_name = "args_binomial_" + std::string(getMemoryLayoutName(memoryLayout_)) + "_"
        + std::string(getCompressionModeName(compressionMode)) + "_"
        + std::string(getCompressionAlgorithmName(compressionAlgorithm)) + ".json";
    std::ofstream o(file_name);
    o << std::setw(2) << arguments << std::endl;

    Binomial distribution = Binomial(probability);
    distribution.seed = seed;
    distribution.sort = sort;
    ByteDataGenerator dataGenerator = ByteDataGenerator(memoryLayoutType, numCols, minValue, maxValue, &distribution);
    std::shared_ptr<MemoryLayout> memoryLayout = RowLayout::create(dataGenerator.getSchema(), bufferSize);
    if (memoryLayout_ == MemoryLayout_::COLUMN)
        memoryLayout = ColumnLayout::create(dataGenerator.getSchema(), bufferSize);

    size_t compressedSize = 0;
    double compressionRatio = 0;
    double compressionTime = 0;
    double decompressionTime = 0;
    for (auto _ : state) {// TODO refactor?
        auto bench = BenchmarkCompression(numBuffers, bufferSize, dataGenerator, memoryLayout, compressionMode);
        try {
            auto start = std::chrono::high_resolution_clock::now();
            auto res = bench.compress(compressionAlgorithm);
            auto end = std::chrono::high_resolution_clock::now();
            compressionTime += std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
            compressedSize += std::get<0>(res) / bench.getNumberOfBuffers();
            compressionRatio += std::get<1>(res) / (double) bench.getNumberOfBuffers();

            start = std::chrono::high_resolution_clock::now();
            bench.decompress();
            end = std::chrono::high_resolution_clock::now();
            decompressionTime += std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
        } catch (NES::Exceptions::RuntimeException const& err) {
            // compressed size might be larger than original
            EXPECT_THAT(err.what(), ::testing::HasSubstr("compression failed: compressed size"));
        }
    }
    state.counters["CompressedSize"] = (double) compressedSize / (double) state.iterations();
    state.counters["CompressionRatio"] = compressionRatio / (double) state.iterations();
    state.counters["CompressionTime"] = compressionTime / (double) state.iterations();
    state.counters["DecompressionTime"] = decompressionTime / (double) state.iterations();
}

// ------------------------------------------------------------
// General parameters
// ------------------------------------------------------------
size_t NUM_ITERATIONS = 10;

CompressionAlgorithm COMPRESSION_ALGORITHM = CompressionAlgorithm::LZ4;// TODO
size_t SEED_VALUE = 42;
bool SORT = true;
size_t NUM_BUFFERS = 10;
size_t BUFFER_SIZE = 4096;
size_t NUM_COLS = 3;
size_t MIN_VALUE = 48;
size_t MAX_VALUE = 57;

// ------------------------------------------------------------
// Repeating Values distribution
// ------------------------------------------------------------
// TODO

// ------------------------------------------------------------
// Uniform distribution
// ------------------------------------------------------------
// TODO

// ------------------------------------------------------------
// Binomial distribution
// ------------------------------------------------------------
double BINOMIAL_PROBABILITY = 0.5;

BENCHMARK_CAPTURE(BM_Binomial,
                  BinomialRowHori,
                  MemoryLayout_::ROW,
                  CompressionMode::HORIZONTAL,
                  COMPRESSION_ALGORITHM,
                  SEED_VALUE,
                  SORT,
                  NUM_BUFFERS,
                  BUFFER_SIZE,
                  NUM_COLS,
                  MIN_VALUE,
                  MAX_VALUE,
                  BINOMIAL_PROBABILITY)
    ->UseManualTime()
    ->Iterations(NUM_ITERATIONS);

BENCHMARK_CAPTURE(BM_Binomial,
                  BinomialColHori,
                  MemoryLayout_::COLUMN,
                  CompressionMode::HORIZONTAL,
                  COMPRESSION_ALGORITHM,
                  SEED_VALUE,
                  SORT,
                  NUM_BUFFERS,
                  BUFFER_SIZE,
                  NUM_COLS,
                  MIN_VALUE,
                  MAX_VALUE,
                  BINOMIAL_PROBABILITY)
    ->UseManualTime()
    ->Iterations(NUM_ITERATIONS);

BENCHMARK_CAPTURE(BM_Binomial,
                  BinomialColVert,
                  MemoryLayout_::COLUMN,
                  CompressionMode::VERTICAL,
                  COMPRESSION_ALGORITHM,
                  SEED_VALUE,
                  SORT,
                  NUM_BUFFERS,
                  BUFFER_SIZE,
                  NUM_COLS,
                  MIN_VALUE,
                  MAX_VALUE,
                  BINOMIAL_PROBABILITY)
    ->UseManualTime()
    ->Iterations(NUM_ITERATIONS);

// ------------------------------------------------------------
// Zipf distribution
// ------------------------------------------------------------
// TODO

// ------------------------------------------------------------
BENCHMARK_MAIN();