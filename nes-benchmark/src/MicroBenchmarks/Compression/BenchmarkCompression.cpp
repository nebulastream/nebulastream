#include "API/Schema.hpp"
#include "DataGeneration/DefaultDataGenerator.hpp"
#include "DataGeneration/GenericDataGenerator.hpp"
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
#include <memory>
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

template<typename... Args>
class BenchmarkCompression2 {
  public:
    BenchmarkCompression2(const size_t numBuffers,
                          const size_t bufferSize,
                          GenericDataGenerator<Args...>& dataGenerator,
                          const std::shared_ptr<MemoryLayout>& memoryLayout,
                          CompressionMode cm,
                          const Args&... tupleArgs) {
        NES::Logger::setupLogging("BenchmarkCompression.log", NES::LogLevel::LOG_DEBUG);
        auto listener = std::make_shared<ErrorHandler>();
        NES::Exceptions::installGlobalErrorListener(listener);

        this->numBuffers = numBuffers;
        NES::Runtime::BufferManagerPtr bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, numBuffers);

        // create data
        dataGenerator.setBufferManager(bufferManager);
        auto tmpBuffers = dataGenerator.createData(numBuffers, bufferSize, tupleArgs...);// TODO

        buffers.reserve(numBuffers);
        for (const auto& tmpBuffer : tmpBuffers) {
            buffers.emplace_back(memoryLayout, tmpBuffer, cm);
        }
    }

    size_t getNumberOfBuffers() { return numBuffers; }
    std::tuple<size_t, double> compress(CompressionAlgorithm ca) {
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
    void decompress() {
        for (auto& buffer : buffers) {
            buffer.decompress();
        }
    }

  private:
    size_t numBuffers{};
    std::vector<CompressedDynamicTupleBuffer> buffers;
};

class BenchmarkCompression {
  public:
    BenchmarkCompression(size_t numBuffers,
                         size_t bufferSize,
                         DataGenerator& dataGenerator,
                         const std::shared_ptr<MemoryLayout>& memoryLayout,
                         CompressionMode cm);
    template<typename... Args>
    BenchmarkCompression(const size_t numBuffers,
                         const size_t bufferSize,
                         GenericDataGenerator<Args...>& dataGenerator,
                         const std::shared_ptr<MemoryLayout>& memoryLayout,
                         CompressionMode cm,
                         const Args&... tupleArgs);
    size_t getNumberOfBuffers() const;
    std::tuple<size_t, double> compress(CompressionAlgorithm ca);
    void decompress();

  private:
    size_t numBuffers{};
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
    NES::Runtime::BufferManagerPtr bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, numBuffers);

    // create data
    dataGenerator.setBufferManager(bufferManager);
    auto tmpBuffers = dataGenerator.createData(numBuffers, bufferSize);

    buffers.reserve(numBuffers);
    for (const auto& tmpBuffer : tmpBuffers) {
        buffers.emplace_back(memoryLayout, tmpBuffer, cm);
    }
}
template<typename... Args>
BenchmarkCompression::BenchmarkCompression(const size_t numBuffers,
                                           const size_t bufferSize,
                                           GenericDataGenerator<Args...>& dataGenerator,
                                           const std::shared_ptr<MemoryLayout>& memoryLayout,
                                           CompressionMode cm,
                                           const Args&... tupleArgs) {
    NES::Logger::setupLogging("BenchmarkCompression.log", NES::LogLevel::LOG_DEBUG);
    auto listener = std::make_shared<ErrorHandler>();
    NES::Exceptions::installGlobalErrorListener(listener);

    this->numBuffers = numBuffers;
    NES::Runtime::BufferManagerPtr bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, numBuffers);

    // create data
    dataGenerator.setBufferManager(bufferManager);
    auto tmpBuffers = dataGenerator.createData(numBuffers, bufferSize, std::forward<Args>(tupleArgs)...);

    buffers.reserve(numBuffers);
    for (const auto& tmpBuffer : tmpBuffers) {
        buffers.emplace_back(memoryLayout, tmpBuffer, cm);
    }
}
size_t BenchmarkCompression::getNumberOfBuffers() const { return numBuffers; }

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
void BM_RepeatingValues(benchmark::State& state, Args&&... args) {
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
    size_t numBuffers = std::get<4>(args_tuple);
    size_t bufferSize = std::get<5>(args_tuple);
    size_t numCols = std::get<6>(args_tuple);
    size_t minValue = std::get<7>(args_tuple);
    size_t maxValue = std::get<8>(args_tuple);
    int numRepeats = std::get<9>(args_tuple);
    uint8_t sigma = std::get<10>(args_tuple);
    double changeProbability = std::get<11>(args_tuple);
    // write arguments to json
    nlohmann::json arguments = {{"Distribution", "RepeatingValues"},
                                {"MemoryLayout", getMemoryLayoutName(memoryLayout_)},
                                {"CompressionMode", getCompressionModeName(compressionMode)},
                                {"CompressionAlgorithm", getCompressionAlgorithmName(compressionAlgorithm)},
                                {"Seed", seed},
                                {"NumBuffers", numBuffers},
                                {"BufferSize", bufferSize},
                                {"NumCols", numCols},
                                {"MinValue", minValue},
                                {"MaxValue", maxValue},
                                {"NumRepeats", numRepeats},
                                {"Sigma", sigma},
                                {"ChangeProbability", changeProbability}};
    std::string file_name = "args_repeating_values_" + std::string(getMemoryLayoutName(memoryLayout_)) + "_"
        + std::string(getCompressionModeName(compressionMode)) + "_"
        + std::string(getCompressionAlgorithmName(compressionAlgorithm)) + ".json";
    std::ofstream o(file_name);
    o << std::setw(2) << arguments << std::endl;
    auto distribution = std::make_shared<RepeatingValues>(numRepeats, sigma, changeProbability);
    distribution->seed = seed;
    DoubleParameter p1 = DoubleParameter(5.5234, 99.00004, distribution.get());
    DoubleParameter p2 = DoubleParameter(5.5234, 99.00004, distribution.get());
    DoubleParameter p3 = DoubleParameter(5.5234, 99.00004, distribution.get());
    /*
auto p1 = std::make_shared<DoubleParameter>(5.5234, 99.00004, distribution);
auto p2 = std::make_shared<DoubleParameter>(5.5234, 99.00004, distribution);
auto p3 = std::make_shared<DoubleParameter>(5.5234, 99.00004, distribution);
     */
    GenericDataGenerator dataGenerator =
        GenericDataGenerator<DoubleParameter, DoubleParameter, DoubleParameter>(memoryLayoutType, p1, p2, p3);
    std::shared_ptr<MemoryLayout> memoryLayout = RowLayout::create(dataGenerator.getSchema(), bufferSize);
    if (memoryLayout_ == MemoryLayout_::COLUMN)
        memoryLayout = ColumnLayout::create(dataGenerator.getSchema(), bufferSize);

    bool failed = false;
    size_t compressedSize = 0;
    double compressionRatio = 0;
    double compressionTime = 0;
    double decompressionTime = 0;
    for (auto _ : state) {// TODO refactor?
        auto bench = BenchmarkCompression2<DoubleParameter, DoubleParameter, DoubleParameter>(numBuffers,
                                                                                              bufferSize,
                                                                                              dataGenerator,
                                                                                              memoryLayout,
                                                                                              compressionMode,
                                                                                              p1,
                                                                                              p2,
                                                                                              p3);
        try {
            auto start = std::chrono::high_resolution_clock::now();
            auto res = bench.compress(compressionAlgorithm);
            auto end = std::chrono::high_resolution_clock::now();
            compressionTime += std::chrono::duration<double, std::milli>(end - start).count();
            compressedSize += std::get<0>(res) / bench.getNumberOfBuffers();
            compressionRatio += std::get<1>(res) / (double) bench.getNumberOfBuffers();

            start = std::chrono::high_resolution_clock::now();
            bench.decompress();
            end = std::chrono::high_resolution_clock::now();
            decompressionTime += std::chrono::duration<double, std::milli>(end - start).count();
        } catch (NES::Exceptions::RuntimeException const& err) {
            // compressed size might be larger than original
            EXPECT_THAT(err.what(), ::testing::HasSubstr("compression failed: compressed size"));
            failed = true;
        }
    }
    if (failed) {
        state.counters["CompressedSize"] = 0;
        state.counters["CompressionRatio"] = 0;
        state.counters["CompressionTime"] = 0;
        state.counters["DecompressionTime"] = 0;
    } else {
        state.counters["CompressedSize"] = (double) compressedSize / (double) state.iterations();
        state.counters["CompressionRatio"] = compressionRatio / (double) state.iterations();
        state.counters["CompressionTime"] = compressionTime / (double) state.iterations();
        state.counters["DecompressionTime"] = decompressionTime / (double) state.iterations();
    }
}

template<class... Args>
void BM_RepeatingValuesText(benchmark::State& state, Args&&... args) {
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
    size_t numBuffers = std::get<4>(args_tuple);
    size_t bufferSize = std::get<5>(args_tuple);
    size_t numCols = std::get<6>(args_tuple);
    size_t minValue = std::get<7>(args_tuple);
    size_t maxValue = std::get<8>(args_tuple);
    int numRepeats = std::get<9>(args_tuple);
    uint8_t sigma = std::get<10>(args_tuple);
    double changeProbability = std::get<11>(args_tuple);
    // write arguments to json
    nlohmann::json arguments = {{"Distribution", "RepeatingValues"},
                                {"MemoryLayout", getMemoryLayoutName(memoryLayout_)},
                                {"CompressionMode", getCompressionModeName(compressionMode)},
                                {"CompressionAlgorithm", getCompressionAlgorithmName(compressionAlgorithm)},
                                {"Seed", seed},
                                {"NumBuffers", numBuffers},
                                {"BufferSize", bufferSize},
                                {"NumCols", numCols},
                                {"MinValue", minValue},
                                {"MaxValue", maxValue},
                                {"NumRepeats", numRepeats},
                                {"Sigma", sigma},
                                {"ChangeProbability", changeProbability}};
    std::string file_name = "args_repeating_values_" + std::string(getMemoryLayoutName(memoryLayout_)) + "_"
        + std::string(getCompressionModeName(compressionMode)) + "_"
        + std::string(getCompressionAlgorithmName(compressionAlgorithm)) + ".json";
    std::ofstream o(file_name);
    o << std::setw(2) << arguments << std::endl;

    std::vector<std::shared_ptr<RepeatingValues>> distributions;
    for (size_t i = 0; i < numCols; i++) {
        auto distribution = std::make_shared<RepeatingValues>(numRepeats, sigma, changeProbability);
        distribution->seed = seed + i;
        distributions.push_back(distribution);
    }
    // TODO dynamic fixedCharLength
    TextParameter p1 = TextParameter(minValue, maxValue, 13, distributions[0].get());
    TextParameter p2 = TextParameter(minValue, maxValue, 13, distributions[1].get());
    TextParameter p3 = TextParameter(minValue, maxValue, 13, distributions[2].get());
    GenericDataGenerator dataGenerator =
        GenericDataGenerator<TextParameter, TextParameter, TextParameter>(memoryLayoutType, p1, p2, p3);
    std::shared_ptr<MemoryLayout> memoryLayout = RowLayout::create(dataGenerator.getSchema(), bufferSize);
    if (memoryLayout_ == MemoryLayout_::COLUMN)
        memoryLayout = ColumnLayout::create(dataGenerator.getSchema(), bufferSize);

    bool failed = false;
    size_t compressedSize = 0;
    double compressionRatio = 0;
    double compressionTime = 0;
    double decompressionTime = 0;
    for (auto _ : state) {// TODO refactor?
        auto bench = BenchmarkCompression2<TextParameter, TextParameter, TextParameter>(numBuffers,
                                                                                        bufferSize,
                                                                                        dataGenerator,
                                                                                        memoryLayout,
                                                                                        compressionMode,
                                                                                        p1,
                                                                                        p2,
                                                                                        p3);
        try {
            auto start = std::chrono::high_resolution_clock::now();
            auto res = bench.compress(compressionAlgorithm);
            auto end = std::chrono::high_resolution_clock::now();
            compressionTime += std::chrono::duration<double, std::milli>(end - start).count();
            compressedSize += std::get<0>(res) / bench.getNumberOfBuffers();
            compressionRatio += std::get<1>(res) / (double) bench.getNumberOfBuffers();

            start = std::chrono::high_resolution_clock::now();
            bench.decompress();
            end = std::chrono::high_resolution_clock::now();
            decompressionTime += std::chrono::duration<double, std::milli>(end - start).count();
        } catch (NES::Exceptions::RuntimeException const& err) {
            // compressed size might be larger than original
            EXPECT_THAT(err.what(), ::testing::HasSubstr("compression failed: compressed size"));
            failed = true;
        }
    }
    if (failed) {
        state.counters["CompressedSize"] = 0;
        state.counters["CompressionRatio"] = 0;
        state.counters["CompressionTime"] = 0;
        state.counters["DecompressionTime"] = 0;
    } else {
        state.counters["CompressedSize"] = (double) compressedSize / (double) state.iterations();
        state.counters["CompressionRatio"] = compressionRatio / (double) state.iterations();
        state.counters["CompressionTime"] = compressionTime / (double) state.iterations();
        state.counters["DecompressionTime"] = decompressionTime / (double) state.iterations();
    }
}

/*
template<class... Args>
void BM_Uniform(benchmark::State& state, Args&&... args) {
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
    size_t minValue = std::get<8>(args_tuple);
    size_t maxValue = std::get<9>(args_tuple);
    // write arguments to json
    nlohmann::json arguments = {{"Distribution", "Uniform"},
                                {"MemoryLayout", getMemoryLayoutName(memoryLayout_)},
                                {"CompressionMode", getCompressionModeName(compressionMode)},
                                {"CompressionAlgorithm", getCompressionAlgorithmName(compressionAlgorithm)},
                                {"Seed", seed},
                                {"Sort", sort},
                                {"NumBuffers", numBuffers},
                                {"BufferSize", bufferSize},
                                {"NumCols", numCols},
                                {"MinValue", minValue},
                                {"MaxValue", maxValue}};
    std::string file_name = "args_uniform_" + std::string(getMemoryLayoutName(memoryLayout_)) + "_"
        + std::string(getCompressionModeName(compressionMode)) + "_"
        + std::string(getCompressionAlgorithmName(compressionAlgorithm)) + ".json";
    std::ofstream o(file_name);
    o << std::setw(2) << arguments << std::endl;

    Uniform distribution = Uniform();
    distribution.seed = seed;
    distribution.sort = sort;
    GenericDataGenerator dataGenerator = GenericDataGenerator(memoryLayoutType, numCols, minValue, maxValue, &distribution);
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
            compressionTime += std::chrono::duration<double, std::milli>(end - start).count();
            compressedSize += std::get<0>(res) / bench.getNumberOfBuffers();
            compressionRatio += std::get<1>(res) / (double) bench.getNumberOfBuffers();

            start = std::chrono::high_resolution_clock::now();
            bench.decompress();
            end = std::chrono::high_resolution_clock::now();
            decompressionTime += std::chrono::duration<double, std::milli>(end - start).count();
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
    size_t minValue = std::get<8>(args_tuple);
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
    GenericDataGenerator dataGenerator = GenericDataGenerator(memoryLayoutType, numCols, minValue, maxValue, &distribution);
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
            compressionTime += std::chrono::duration<double, std::milli>(end - start).count();
            compressedSize += std::get<0>(res) / bench.getNumberOfBuffers();
            compressionRatio += std::get<1>(res) / (double) bench.getNumberOfBuffers();

            start = std::chrono::high_resolution_clock::now();
            bench.decompress();
            end = std::chrono::high_resolution_clock::now();
            decompressionTime += std::chrono::duration<double, std::milli>(end - start).count();
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

template<class... Args>
void BM_Zipf(benchmark::State& state, Args&&... args) {
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
    size_t minValue = std::get<8>(args_tuple);
    size_t maxValue = std::get<9>(args_tuple);
    double alpha = std::get<10>(args_tuple);
    // write arguments to json
    nlohmann::json arguments = {{"Distribution", "Zipf"},
                                {"MemoryLayout", getMemoryLayoutName(memoryLayout_)},
                                {"CompressionMode", getCompressionModeName(compressionMode)},
                                {"CompressionAlgorithm", getCompressionAlgorithmName(compressionAlgorithm)},
                                {"Seed", seed},
                                {"NumBuffers", numBuffers},
                                {"BufferSize", bufferSize},
                                {"NumCols", numCols},
                                {"MinValue", minValue},
                                {"MaxValue", maxValue},
                                {"Alpha", alpha}};
    std::string file_name = "args_zipf_" + std::string(getMemoryLayoutName(memoryLayout_)) + "_"
        + std::string(getCompressionModeName(compressionMode)) + "_"
        + std::string(getCompressionAlgorithmName(compressionAlgorithm)) + ".json";
    std::ofstream o(file_name);
    o << std::setw(2) << arguments << std::endl;

    Zipf distribution = Zipf(alpha);
    distribution.seed = seed;
    distribution.sort = sort;
    GenericDataGenerator dataGenerator = GenericDataGenerator(memoryLayoutType, numCols, minValue, maxValue, &distribution);
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
            compressionTime += std::chrono::duration<double, std::milli>(end - start).count();
            compressedSize += std::get<0>(res) / bench.getNumberOfBuffers();
            compressionRatio += std::get<1>(res) / (double) bench.getNumberOfBuffers();

            start = std::chrono::high_resolution_clock::now();
            bench.decompress();
            end = std::chrono::high_resolution_clock::now();
            decompressionTime += std::chrono::duration<double, std::milli>(end - start).count();
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
*/
// ------------------------------------------------------------
// Benchmark Parameters
// have to be changed by hand (except compression algorithm) ...
// ------------------------------------------------------------
// TODO? use environment variables for all variables?
size_t NUM_BENCHMARK_ITERATIONS = 1;
auto COMPRESSION_ALGORITHM = CompressionAlgorithm::LZ4;// TODO
//auto COMPRESSION_ALGORITHM = getCompressionAlgorithmByName(std::getenv("CA"));
size_t SEED_VALUE = 42;
bool SORT = true;
size_t NUM_BUFFERS = 1;
size_t BUFFER_SIZE = 300;
size_t NUM_COLS = 3;
size_t MIN_VALUE = 0;
size_t MAX_VALUE = 150;

int REPEATING_VALUES_NUM_REPEATS = 3;
uint8_t REPEATING_VALUES_SIGMA = 3;
double REPEATING_VALUES_CHANGE_PROBABILITY = 0.9;

double BINOMIAL_PROBABILITY = 0.5;

double ZIPF_ALPHA = 0.15;

// Note: only change above values and none from below!

// ------------------------------------------------------------
// Repeating Values distribution
// ------------------------------------------------------------
BENCHMARK_CAPTURE(BM_RepeatingValuesText,
                  BM_RepeatingValuesRowHori,
                  MemoryLayout_::ROW,
                  CompressionMode::HORIZONTAL,
                  COMPRESSION_ALGORITHM,
                  SEED_VALUE,
                  NUM_BUFFERS,
                  BUFFER_SIZE,
                  NUM_COLS,
                  MIN_VALUE,
                  MAX_VALUE,
                  REPEATING_VALUES_NUM_REPEATS,
                  REPEATING_VALUES_SIGMA,
                  REPEATING_VALUES_CHANGE_PROBABILITY)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

/*
BENCHMARK_CAPTURE(BM_RepeatingValues,
                  BM_RepeatingValuesColHori,
                  MemoryLayout_::COLUMN,
                  CompressionMode::HORIZONTAL,
                  COMPRESSION_ALGORITHM,
                  SEED_VALUE,
                  NUM_BUFFERS,
                  BUFFER_SIZE,
                  NUM_COLS,
                  MIN_VALUE,
                  MAX_VALUE,
                  REPEATING_VALUES_NUM_REPEATS,
                  REPEATING_VALUES_SIGMA,
                  REPEATING_VALUES_CHANGE_PROBABILITY)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

BENCHMARK_CAPTURE(BM_RepeatingValues,
                  BM_RepeatingValuesColVert,
                  MemoryLayout_::COLUMN,
                  CompressionMode::VERTICAL,
                  COMPRESSION_ALGORITHM,
                  SEED_VALUE,
                  NUM_BUFFERS,
                  BUFFER_SIZE,
                  NUM_COLS,
                  MIN_VALUE,
                  MAX_VALUE,
                  REPEATING_VALUES_NUM_REPEATS,
                  REPEATING_VALUES_SIGMA,
                  REPEATING_VALUES_CHANGE_PROBABILITY)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);
*/

/*
// ------------------------------------------------------------
// Uniform distribution
// ------------------------------------------------------------
BENCHMARK_CAPTURE(BM_Uniform,
                  UniformRowHori,
                  MemoryLayout_::ROW,
                  CompressionMode::HORIZONTAL,
                  COMPRESSION_ALGORITHM,
                  SEED_VALUE,
                  SORT,
                  NUM_BUFFERS,
                  BUFFER_SIZE,
                  NUM_COLS,
                  MIN_VALUE,
                  MAX_VALUE)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

BENCHMARK_CAPTURE(BM_Uniform,
                  UniformColHori,
                  MemoryLayout_::COLUMN,
                  CompressionMode::HORIZONTAL,
                  COMPRESSION_ALGORITHM,
                  SEED_VALUE,
                  SORT,
                  NUM_BUFFERS,
                  BUFFER_SIZE,
                  NUM_COLS,
                  MIN_VALUE,
                  MAX_VALUE)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

BENCHMARK_CAPTURE(BM_Uniform,
                  UniformColVert,
                  MemoryLayout_::COLUMN,
                  CompressionMode::VERTICAL,
                  COMPRESSION_ALGORITHM,
                  SEED_VALUE,
                  SORT,
                  NUM_BUFFERS,
                  BUFFER_SIZE,
                  NUM_COLS,
                  MIN_VALUE,
                  MAX_VALUE)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

// ------------------------------------------------------------
// Binomial distribution
// ------------------------------------------------------------
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
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

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
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

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
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

// ------------------------------------------------------------
// Zipf distribution
// ------------------------------------------------------------
BENCHMARK_CAPTURE(BM_Zipf,
                  ZipfRowHori,
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
                  ZIPF_ALPHA)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

BENCHMARK_CAPTURE(BM_Zipf,
                  ZipfColHori,
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
                  ZIPF_ALPHA)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

BENCHMARK_CAPTURE(BM_Zipf,
                  ZipfColVert,
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
                  ZIPF_ALPHA)
    ->UseManualTime()
    ->Iterations(NUM_BENCHMARK_ITERATIONS);

// ------------------------------------------------------------
 */
BENCHMARK_MAIN();