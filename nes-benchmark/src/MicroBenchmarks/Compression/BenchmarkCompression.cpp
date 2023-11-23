#include "API/AttributeField.hpp"
#include "API/Schema.hpp"
#include "DataGeneration/DefaultDataGenerator.hpp"
#include "DataGeneration/GenericDataGen.hpp"
#include "DataGeneration/GenericDataGenerator.hpp"
#include <Exceptions/ErrorListener.hpp>
#include <Exceptions/SignalHandling.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/CompressedDynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Util/Common.hpp>
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
using namespace nlohmann;

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
    BenchmarkCompression();
    BenchmarkCompression(size_t numBuffers,
                         size_t bufferSize,
                         const std::shared_ptr<DataGenerator>& dataGenerator,
                         const std::shared_ptr<MemoryLayout>& memoryLayout,
                         CompressionMode cm);
    BenchmarkCompression(size_t numBuffers,
                         size_t bufferSize,
                         DataGenerator& dataGenerator,
                         const std::shared_ptr<MemoryLayout>& memoryLayout,
                         CompressionMode cm);
    BenchmarkCompression(const size_t numBuffers,
                         const size_t bufferSize,
                         GenericDataGenerator<>& dataGenerator,
                         const std::shared_ptr<MemoryLayout>& memoryLayout,
                         CompressionMode cm);
    size_t getNumberOfBuffers() const;
    size_t getNumberOfTuples() const;
    void writeBuffersToCSV(std::string directory, size_t id);
    std::tuple<size_t, double> compress(std::vector<CompressionAlgorithm>& cas);
    void decompress();

  private:
    size_t numBuffers{};
    std::vector<CompressedDynamicTupleBuffer> buffers;
    NES::SchemaPtr schema;
};
BenchmarkCompression::BenchmarkCompression() {
    // dummy
}

BenchmarkCompression::BenchmarkCompression(const size_t numBuffers,
                                           const size_t bufferSize,
                                           const std::shared_ptr<DataGenerator>& dataGenerator,
                                           const std::shared_ptr<MemoryLayout>& memoryLayout,
                                           CompressionMode cm) {
    NES::Logger::setupLogging("BenchmarkCompression.log", NES::LogLevel::LOG_DEBUG);
    auto listener = std::make_shared<ErrorHandler>();
    NES::Exceptions::installGlobalErrorListener(listener);

    this->schema = dataGenerator->getSchema();

    this->numBuffers = numBuffers;
    NES::Runtime::BufferManagerPtr bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, numBuffers);

    // create data
    dataGenerator->setBufferManager(bufferManager);
    auto tmpBuffers = dataGenerator->createData(numBuffers, bufferSize);

    buffers.reserve(numBuffers);
    for (const auto& tmpBuffer : tmpBuffers) {
        buffers.emplace_back(memoryLayout, tmpBuffer, cm);
    }
}
BenchmarkCompression::BenchmarkCompression(const size_t numBuffers,
                                           const size_t bufferSize,
                                           DataGenerator& dataGenerator,
                                           const std::shared_ptr<MemoryLayout>& memoryLayout,
                                           CompressionMode cm) {
    NES::Logger::setupLogging("BenchmarkCompression.log", NES::LogLevel::LOG_DEBUG);
    auto listener = std::make_shared<ErrorHandler>();
    NES::Exceptions::installGlobalErrorListener(listener);

    this->schema = dataGenerator.getSchema();

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
BenchmarkCompression::BenchmarkCompression(const size_t numBuffers,
                                           const size_t bufferSize,
                                           GenericDataGenerator<>& dataGenerator,
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
size_t BenchmarkCompression::getNumberOfBuffers() const { return numBuffers; }

size_t BenchmarkCompression::getNumberOfTuples() const {
    size_t numTuples = 0;
    for (const auto& buf : buffers)
        numTuples += buf.getNumberOfTuples();
    return numTuples;
}

void BenchmarkCompression::writeBuffersToCSV(std::string directory, size_t id) {
    if (directory.back() != '/')
        directory += "/";

    for (size_t bufferId = 0; bufferId < buffers.size(); bufferId++) {
        std::string csvPath = directory + "buffer_" + std::to_string(id) + "_" + std::to_string(bufferId) + ".csv";
        auto buffer = buffers[bufferId];

        std::string header;
        for (size_t s = 0; s < schema->getSize(); s++) {
            header += schema->get(s)->getDataType()->toString() + ";";// TODO get BasicType?
        }
        NES::Util::writeHeaderToCsvFile(csvPath, header);

        for (size_t row = 0; row < buffer.getCapacity(); row++) {
            std::string csvRow;
            for (size_t col = 0; col < buffer.getOffsets().size(); col++) {
                auto value = buffer[row][col].toString();
                csvRow += value + ";";
            }
            NES::Util::writeRowToCsvFile(csvPath, csvRow);
        }
    }
}

std::tuple<size_t, double> BenchmarkCompression::compress(std::vector<CompressionAlgorithm>& cas) {
    double totalCompressionRatio = 0;
    size_t totalBufferSize = 0;
    for (auto& buffer : buffers) {
        buffer.compress(cas);
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

/*
class Config {
  public:
    size_t numBenchmarkIterations{};
    std::string benchmarkName;
    NES::Schema::MemoryLayoutType memoryLayoutType = NES::Schema::MemoryLayoutType::ROW_LAYOUT;
    CompressionMode compressionMode;
    size_t numBuffers{};
    size_t bufferSize{};
    std::vector<GenericDataDistribution*> dataDistributions;
    std::vector<std::shared_ptr<GenericDataGeneratorParameter>> dataTypes;
    std::vector<CompressionAlgorithm> compressionAlgorithms;
};

static Config jsonToConfig(const std::string& filePath) {
    std::ifstream ifs(filePath);
    json j = json::parse(ifs);
    Config c;

    j.at("num_benchmark_iterations").get_to(c.numBenchmarkIterations);
    j.at("benchmark_name").get_to(c.benchmarkName);

    auto memoryLayoutString = j.at("memory_layout").get<std::string>();
    if (memoryLayoutString == "RowLayout")
        c.memoryLayoutType = NES::Schema::MemoryLayoutType::ROW_LAYOUT;
    else if (memoryLayoutString == "ColumnLayout") {
        c.memoryLayoutType = NES::Schema::MemoryLayoutType::COLUMNAR_LAYOUT;
    } else
        NES_THROW_RUNTIME_ERROR("Invalid MemoryLayoutType");

    j.at("compression_mode").get_to<CompressionMode>(c.compressionMode);
    j.at("num_buffers").get_to(c.numBuffers);
    j.at("buffer_size").get_to(c.bufferSize);

    for (auto& distribution : j["distributions"]) {
        auto distributionName = distribution.at("distribution_name").get<DistributionName>();
        switch (distributionName) {
            case DistributionName::REPEATING_VALUES: {
                auto numRepeats = distribution.at("num_repeats").get<int>();
                auto sigma = distribution.at("sigma").get<uint8_t>();
                auto changeProbability = distribution.at("change_probability").get<double>();
                GenericDataDistribution dist = RepeatingValues(numRepeats, sigma, changeProbability);
                try {
                    dist.seed = distribution.at("seed").get<size_t>();
                } catch (json::out_of_range& e) {
                }
                c.dataDistributions.push_back(&dist);
                break;
            }
            case DistributionName::UNIFORM: {
                GenericDataDistribution dist = Uniform();
                try {
                    dist.seed = distribution.at("seed").get<size_t>();
                } catch (json::out_of_range& e) {
                }
                try {
                    dist.sort = distribution.at("sort").get<bool>();
                } catch (json::out_of_range& e) {
                }
                c.dataDistributions.push_back(&dist);
                break;
            }
            case DistributionName::BINOMIAL: {
                auto probability = distribution.at("probability").get<double>();
                GenericDataDistribution dist = Binomial(probability);
                try {
                    dist.seed = distribution.at("seed").get<size_t>();
                } catch (json::out_of_range& e) {
                }
                try {
                    dist.sort = distribution.at("sort").get<bool>();
                } catch (json::out_of_range& e) {
                }
                c.dataDistributions.push_back(&dist);
                break;
            }
            case DistributionName::ZIPF: {
                auto alpha = distribution.at("alpha").get<double>();
                GenericDataDistribution dist = Zipf(alpha);
                try {
                    dist.seed = distribution.at("seed").get<size_t>();
                } catch (json::out_of_range& e) {
                }
                try {
                    dist.sort = distribution.at("sort").get<bool>();
                } catch (json::out_of_range& e) {
                }
                c.dataDistributions.push_back(&dist);
                break;
            }
        }
    }

    size_t distIdx = 0;
    for (auto& dataType : j["data_types"]) {
        auto d = dataType.at("data_type").get<NES::BasicType>();
        switch (d) {
            case NES::BasicType::INT8: {
                int8_t minValue = dataType.at("min_value").get<int8_t>();
                int8_t maxValue = dataType.at("max_value").get<int8_t>();
                auto p = std::make_shared<Int8Parameter>(minValue, maxValue, c.dataDistributions[distIdx]);// TODO pointer?
                c.dataTypes.push_back(p);
                break;
            }
            case NES::BasicType::UINT8: break;
            case NES::BasicType::INT16: break;
            case NES::BasicType::UINT16: break;
            case NES::BasicType::INT32: break;
            case NES::BasicType::UINT32: break;
            case NES::BasicType::INT64: break;
            case NES::BasicType::FLOAT32: break;
            case NES::BasicType::UINT64: break;
            case NES::BasicType::FLOAT64: {
                double minValue = dataType.at("min_value").get<double>();
                double maxValue = dataType.at("max_value").get<double>();
                auto p = std::make_shared<DoubleParameter>(minValue, maxValue, c.dataDistributions[distIdx]);
                c.dataTypes.push_back(p);
                break;
            }
            case NES::BasicType::BOOLEAN: break;
            case NES::BasicType::CHAR: break;
            case NES::BasicType::TEXT: {
                size_t minValue = dataType.at("min_value").get<size_t>();
                size_t maxValue = dataType.at("max_value").get<size_t>();
                DataSet dataSetType = dataType.at("data_set").get<DataSet>();
                switch (dataSetType) {
                    case DataSet::IPSUM_WORDS: {
                        maxValue = (maxValue > IpsumWords::maxIndexValue) ? IpsumWords::maxIndexValue : maxValue;
                        auto p = std::make_shared<TextParameter>(minValue, maxValue, IpsumWords::maxValueLength, c.dataDistributions[distIdx]);
                        c.dataTypes.push_back(p);
                        break;
                    }
                    case DataSet::IPSUM_SENTENCES: {
                        maxValue = (maxValue > IpsumSentences::maxIndexValue) ? IpsumSentences::maxIndexValue : maxValue;
                        auto p = std::make_shared<TextParameter>(minValue, maxValue, IpsumSentences::maxValueLength, c.dataDistributions[distIdx]);
                        c.dataTypes.push_back(p);
                        break;
                    }
                    case INVALID: break;
                }
                break;
            }
        }
        distIdx++;
    }

    c.compressionAlgorithms = j.at("compression_algorithms").get<std::vector<CompressionAlgorithm>>();

    if (c.dataTypes.size() != c.dataDistributions.size())
        NES_THROW_RUNTIME_ERROR("Number of data types must be equal to number of distributions.");

    if (c.memoryLayoutType == NES::Schema::MemoryLayoutType::ROW_LAYOUT && c.compressionMode == CompressionMode::HORIZONTAL
        && c.compressionAlgorithms.size() != 1)
        NES_THROW_RUNTIME_ERROR("Horizontal compression of row layout must have exactly 1 compression Algorithm");
    if (c.memoryLayoutType == NES::Schema::MemoryLayoutType::COLUMNAR_LAYOUT && c.compressionMode == HORIZONTAL
        && c.compressionAlgorithms.size() == 1)
        NES_THROW_RUNTIME_ERROR("Horizontal compression of columnar layout must have exactly 1 compression Algorithm");
    if (c.memoryLayoutType == NES::Schema::MemoryLayoutType::COLUMNAR_LAYOUT && c.compressionMode == VERTICAL
        && c.compressionAlgorithms.size() == c.dataTypes.size())
        NES_THROW_RUNTIME_ERROR("Vertical compression of columnar layout must have as many compression algorithms as columns");

    return c;
}
*/

class Config {
  public:
    size_t numBenchmarkIterations{};
    std::string benchmarkName;
    NES::Schema::MemoryLayoutType memoryLayoutType = NES::Schema::MemoryLayoutType::ROW_LAYOUT;
    CompressionMode compressionMode;
    size_t numBuffers{};
    size_t bufferSize{};
    std::vector<DistributionPtr> dataDistributions;
    std::vector<CompressionAlgorithm> compressionAlgorithms;
};

static Config jsonToConfig(const std::string& filePath) {
    std::ifstream ifs(filePath);
    json j = json::parse(ifs);
    Config c;

    j.at("benchmark_name").get_to(c.benchmarkName);

    auto memoryLayoutString = j.at("memory_layout").get<std::string>();
    if (memoryLayoutString == "RowLayout")
        c.memoryLayoutType = NES::Schema::MemoryLayoutType::ROW_LAYOUT;
    else if (memoryLayoutString == "ColumnLayout") {
        c.memoryLayoutType = NES::Schema::MemoryLayoutType::COLUMNAR_LAYOUT;
    } else
        NES_THROW_RUNTIME_ERROR("Invalid MemoryLayoutType");

    j.at("compression_mode").get_to<CompressionMode>(c.compressionMode);
    j.at("num_buffers").get_to(c.numBuffers);
    j.at("buffer_size").get_to(c.bufferSize);

    for (auto& distribution : j["distributions"]) {
        auto distributionName = distribution.at("distribution_name").get<DistributionName2>();
        int seed = distribution.at("seed").get<int>();
        auto dataType = distribution.at("data_type").get<NES::BasicType>();
        switch (distributionName) {
            case DistributionName2::DELTA_VALUES: {
                switch (dataType) {
                    case NES::BasicType::UINT8: {
                        uint8_t initValue = distribution.at("init_value").get<uint8_t>();
                        int numRepeats = distribution.at("num_repeats").get<int>();
                        int sigma = distribution.at("sigma").get<int>();
                        uint8_t step = distribution.at("step").get<uint8_t>();
                        uint8_t stepDeviation = distribution.at("step_deviation").get<uint8_t>();
                        auto dist = std::make_shared<DeltaValueDistribution<uint8_t>>(initValue,
                                                                                           numRepeats,
                                                                                           sigma,
                                                                                           step,
                                                                                           stepDeviation,
                                                                                           seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    default: exit(-1);// TODO NES_NOT_IMPLEMENTED()
                }
                break;
            }
            case DistributionName2::REPEATING_VALUES: {
                auto numRepeats = distribution.at("num_repeats").get<int>();
                auto sigma = distribution.at("sigma").get<uint8_t>();
                auto changeProbability = distribution.at("change_probability").get<double>();
                switch (dataType) {
                    case NES::BasicType::UINT8: {
                        uint8_t minValue = distribution.at("min_value").get<uint8_t>();
                        uint8_t maxValue = distribution.at("max_value").get<uint8_t>();
                        auto dist = std::make_shared<RepeatingValuesDistribution<uint8_t>>(minValue,
                                                                                           maxValue,
                                                                                           numRepeats,
                                                                                           sigma,
                                                                                           changeProbability,
                                                                                           seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::INT32: {
                        int32_t minValue = distribution.at("min_value").get<int32_t>();
                        int32_t maxValue = distribution.at("max_value").get<int32_t>();
                        auto dist = std::make_shared<RepeatingValuesDistribution<int32_t>>(minValue,
                                                                                           maxValue,
                                                                                           numRepeats,
                                                                                           sigma,
                                                                                           changeProbability,
                                                                                           seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::FLOAT64: {
                        double minValue = distribution.at("min_value").get<double>();
                        double maxValue = distribution.at("max_value").get<double>();
                        auto dist = std::make_shared<RepeatingValuesDistribution<double>>(minValue,
                                                                                          maxValue,
                                                                                          numRepeats,
                                                                                          sigma,
                                                                                          changeProbability,
                                                                                          seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::UINT64: {
                        uint64_t minValue = distribution.at("min_value").get<uint64_t>();
                        uint64_t maxValue = distribution.at("max_value").get<uint64_t>();
                        auto dist = std::make_shared<RepeatingValuesDistribution<uint64_t>>(minValue,
                                                                                            maxValue,
                                                                                            numRepeats,
                                                                                            sigma,
                                                                                            changeProbability,
                                                                                            seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::TEXT: {
                        int minValue = distribution.at("min_value").get<int>();
                        int maxValue = distribution.at("max_value").get<int>();
                        DataSet dataSet = distribution.at("data_set").get<DataSet>();
                        auto dist = std::make_shared<RepeatingValuesDistribution<std::string>>(minValue,
                                                                                               maxValue,
                                                                                               numRepeats,
                                                                                               sigma,
                                                                                               changeProbability,
                                                                                               seed,
                                                                                               dataSet);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    default: exit(-1);// TODO NES_NOT_IMPLEMENTED()
                }
                break;
            }
            case DistributionName2::UNIFORM: {
                switch (dataType) {
                    case NES::BasicType::UINT8: {
                        uint8_t minValue = distribution.at("min_value").get<uint8_t>();
                        uint8_t maxValue = distribution.at("max_value").get<uint8_t>();
                        auto dist = std::make_shared<UniformDistribution<uint8_t>>(minValue, maxValue, seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::INT32: {
                        int32_t minValue = distribution.at("min_value").get<int32_t>();
                        int32_t maxValue = distribution.at("max_value").get<int32_t>();
                        auto dist = std::make_shared<UniformDistribution<int32_t>>(minValue, maxValue, seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::FLOAT64: {
                        double minValue = distribution.at("min_value").get<double>();
                        double maxValue = distribution.at("max_value").get<double>();
                        auto dist = std::make_shared<UniformDistribution<double>>(minValue, maxValue, seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::UINT64: {
                        uint64_t minValue = distribution.at("min_value").get<uint64_t>();
                        uint64_t maxValue = distribution.at("max_value").get<uint64_t>();
                        auto dist = std::make_shared<UniformDistribution<uint64_t>>(minValue, maxValue, seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::TEXT: {
                        int minValue = distribution.at("min_value").get<int>();
                        int maxValue = distribution.at("max_value").get<int>();
                        DataSet dataSet = distribution.at("data_set").get<DataSet>();
                        auto dist = std::make_shared<UniformDistribution<std::string>>(minValue, maxValue, seed, dataSet);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    default: exit(-1);// TODO NES_NOT_IMPLEMENTED()
                }
                break;
            }
            case DistributionName2::BINOMIAL: {
                double probability = distribution.at("probability").get<double>();
                switch (dataType) {
                    case NES::BasicType::UINT8: {
                        uint8_t minValue = distribution.at("min_value").get<uint8_t>();
                        uint8_t maxValue = distribution.at("max_value").get<uint8_t>();
                        auto dist = std::make_shared<BinomialDistribution<uint8_t>>(minValue, maxValue, probability, seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::INT32: {
                        int32_t minValue = distribution.at("min_value").get<int32_t>();
                        int32_t maxValue = distribution.at("max_value").get<int32_t>();
                        auto dist = std::make_shared<BinomialDistribution<int32_t>>(minValue, maxValue, probability, seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::FLOAT64: {
                        double minValue = distribution.at("min_value").get<double>();
                        double maxValue = distribution.at("max_value").get<double>();
                        auto dist = std::make_shared<BinomialDistribution<double>>(minValue, maxValue, probability, seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::UINT64: {
                        uint64_t minValue = distribution.at("min_value").get<uint64_t>();
                        uint64_t maxValue = distribution.at("max_value").get<uint64_t>();
                        auto dist = std::make_shared<BinomialDistribution<uint64_t>>(minValue, maxValue, probability, seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::TEXT: {
                        int minValue = distribution.at("min_value").get<int>();
                        int maxValue = distribution.at("max_value").get<int>();
                        DataSet dataSet = distribution.at("data_set").get<DataSet>();
                        auto dist =
                            std::make_shared<BinomialDistribution<std::string>>(minValue, maxValue, probability, seed, dataSet);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    default: exit(-1);// TODO NES_NOT_IMPLEMENTED()
                }
                break;
            }
            case DistributionName2::ZIPF: {
                double alpha = distribution.at("alpha").get<double>();
                switch (dataType) {
                    case NES::BasicType::UINT8: {
                        uint8_t minValue = distribution.at("min_value").get<uint8_t>();
                        uint8_t maxValue = distribution.at("max_value").get<uint8_t>();
                        auto dist = std::make_shared<ZipfianDistribution<uint8_t>>(minValue, maxValue, alpha, seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::INT32: {
                        int32_t minValue = distribution.at("min_value").get<int32_t>();
                        int32_t maxValue = distribution.at("max_value").get<int32_t>();
                        auto dist = std::make_shared<ZipfianDistribution<int32_t>>(minValue, maxValue, alpha, seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::FLOAT64: {
                        double minValue = distribution.at("min_value").get<double>();
                        double maxValue = distribution.at("max_value").get<double>();
                        auto dist = std::make_shared<ZipfianDistribution<double>>(minValue, maxValue, alpha, seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::UINT64: {
                        uint64_t minValue = distribution.at("min_value").get<uint64_t>();
                        uint64_t maxValue = distribution.at("max_value").get<uint64_t>();
                        auto dist = std::make_shared<ZipfianDistribution<uint64_t>>(minValue, maxValue, alpha, seed);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    case NES::BasicType::TEXT: {
                        int minValue = distribution.at("min_value").get<int>();
                        int maxValue = distribution.at("max_value").get<int>();
                        DataSet dataSet = distribution.at("data_set").get<DataSet>();
                        auto dist = std::make_shared<ZipfianDistribution<std::string>>(minValue, maxValue, alpha, seed, dataSet);
                        c.dataDistributions.push_back(dist);
                        break;
                    }
                    default: exit(-1);// TODO NES_NOT_IMPLEMENTED()
                }
                break;
            }
        }
    }

    c.compressionAlgorithms = j.at("compression_algorithms").get<std::vector<CompressionAlgorithm>>();

    if (c.memoryLayoutType == NES::Schema::MemoryLayoutType::ROW_LAYOUT && c.compressionMode == CompressionMode::HORIZONTAL
        && c.compressionAlgorithms.size() != 1)
        NES_THROW_RUNTIME_ERROR("Horizontal compression of row layout must have exactly 1 compression Algorithm");
    if (c.memoryLayoutType == NES::Schema::MemoryLayoutType::COLUMNAR_LAYOUT && c.compressionMode == HORIZONTAL
        && c.compressionAlgorithms.size() != 1)
        NES_THROW_RUNTIME_ERROR("Horizontal compression of columnar layout must have exactly 1 compression Algorithm");
    if (c.memoryLayoutType == NES::Schema::MemoryLayoutType::COLUMNAR_LAYOUT && c.compressionMode == VERTICAL
        && c.compressionAlgorithms.size() != c.dataDistributions.size())
        NES_THROW_RUNTIME_ERROR("Vertical compression of columnar layout must have as many compression algorithms as columns");

    return c;
}

void BM_TestConfig([[maybe_unused]] benchmark::State& state) {
    NES::Logger::setupLogging("BenchmarkCompression.log", NES::LogLevel::LOG_DEBUG);
    auto listener = std::make_shared<ErrorHandler>();
    NES::Exceptions::installGlobalErrorListener(listener);

    //auto filePath = "/home/maseiler/Coding/nebulastream/cmake-build-debug/nes-benchmark/config.json";
    auto filePath = "/home/maseiler/Coding/nebulastream/cmake-build-release/nes-benchmark/config.json";
    Config config = jsonToConfig(filePath);

    auto dataGen = std::make_shared<GenericDataGen>(config.memoryLayoutType, config.dataDistributions);
    std::shared_ptr<MemoryLayout> memoryLayout;
    if (config.memoryLayoutType == NES::Schema::MemoryLayoutType::ROW_LAYOUT)
        memoryLayout = RowLayout::create(dataGen->getSchema(), config.bufferSize);
    else
        memoryLayout = ColumnLayout::create(dataGen->getSchema(), config.bufferSize);

    bool wroteBufferToCsv = false;
    bool failed = false;
    size_t compressedSize = 0;
    double compressionRatio = 0;
    double compressionTime = 0;
    double decompressionTime = 0;
    size_t numTuples = 0;
    size_t id = 0;
    for (auto _ : state) {// TODO refactor?
        auto bench = std::make_shared<BenchmarkCompression>(config.numBuffers,
                                                            config.bufferSize,
                                                            dataGen,
                                                            memoryLayout,
                                                            config.compressionMode);
        numTuples += bench->getNumberOfTuples();
        if (!wroteBufferToCsv) {
            // std::string csvDir = "/home/maseiler/Coding/nebulastream/cmake-build-debug/nes-benchmark/";
            std::string csvDir = "/home/maseiler/Coding/nebulastream/cmake-build-release/nes-benchmark/";
            bench->writeBuffersToCSV(csvDir, id);
            wroteBufferToCsv = true;
        }
        try {
            auto start = std::chrono::high_resolution_clock::now();
            auto res = bench->compress(config.compressionAlgorithms);
            auto end = std::chrono::high_resolution_clock::now();
            compressionTime += std::chrono::duration<double, std::milli>(end - start).count();
            compressedSize += std::get<0>(res) / bench->getNumberOfBuffers();
            compressionRatio += std::get<1>(res) / (double) bench->getNumberOfBuffers();

            start = std::chrono::high_resolution_clock::now();
            bench->decompress();
            end = std::chrono::high_resolution_clock::now();
            decompressionTime += std::chrono::duration<double, std::milli>(end - start).count();
        } catch (NES::Exceptions::RuntimeException const& err) {
            // compressed size might be larger than original
            EXPECT_THAT(err.what(), ::testing::HasSubstr("compression failed: compressed size"));
            failed = true;
        }
        id++;
    }
    if (failed) {
        state.counters["CompressedSize"] = -1;
        state.counters["CompressionRatio"] = -1;
        state.counters["CompressionTime"] = -1;
        state.counters["DecompressionTime"] = -1;
        state.counters["NumberOfTuples"] = -1;
    } else {
        state.counters["CompressedSize"] = (double) compressedSize / (double) state.iterations();
        state.counters["CompressionRatio"] = compressionRatio / (double) state.iterations();
        state.counters["CompressionTime"] = compressionTime / (double) state.iterations();
        state.counters["DecompressionTime"] = decompressionTime / (double) state.iterations();
        state.counters["NumberOfTuples"] = numTuples / (double) state.iterations();
    }
}

constexpr size_t NUM_BENCHMARK_ITERATIONS = 10;
constexpr size_t NUM_BENCHMARK_REPETITIONS = 10;

BENCHMARK(BM_TestConfig)->UseManualTime()->Iterations(NUM_BENCHMARK_ITERATIONS)->Repetitions(NUM_BENCHMARK_REPETITIONS);
BENCHMARK_MAIN();


/*
class MyFixture : public benchmark::Fixture {
  public:
    void SetUp([[maybe_unused]] const ::benchmark::State& state) {
        std::cout << state.iterations() << std::endl;
        std::cout << "SetUp complete" << std::endl;
        for (int i = 0; i < 1000; i++)
            numbers.push_back(i);
        chars = (char*) malloc(sizeof(char) * 100);
        strcpy(chars, "abcd");
    }

    void TearDown([[maybe_unused]] const ::benchmark::State& state) {
        numbers.clear();
        free(chars);
        std::cout << "TearDown complete" << std::endl;
    }

    std::vector<int> numbers;
    char* chars{};
};

void printSum(std::vector<int>& numbers) {
    int sum = 0;
    for (auto i : numbers)
        sum += i;
    std::cout << sum << std::endl;
}

void printChars(char* chars) { std::cout << chars << std::endl; }

BENCHMARK_DEFINE_F(MyFixture, BarTest)([[maybe_unused]] benchmark::State& st) {
    for (auto _ : st) {
        printSum(numbers);
        printChars(chars);
        std::cout << "Benchmark complete" << std::endl;
    }
}
//BENCHMARK_REGISTER_F(MyFixture, BarTest)->Iterations(10);
//BENCHMARK_MAIN();

class MyFixture2 : public benchmark::Fixture {
  public:
    void SetUp([[maybe_unused]] const ::benchmark::State& state) {
        NES::Logger::setupLogging("BenchmarkCompression.log", NES::LogLevel::LOG_DEBUG);
        auto listener = std::make_shared<ErrorHandler>();
        NES::Exceptions::installGlobalErrorListener(listener);

        auto filePath =
            "/home/maseiler/Coding/nebulastream/nes-benchmark/src/MicroBenchmarks/Compression/configs/testRowHori.json";
        Config config = jsonToConfig(filePath);

        compressionAlgorithms = config.compressionAlgorithms;
        auto dataGen = GenericDataGen(config.memoryLayoutType, config.dataDistributions);
        std::shared_ptr<MemoryLayout> memoryLayout = RowLayout::create(dataGen.getSchema(), config.bufferSize);
        bench = BenchmarkCompression(config.numBuffers, config.bufferSize, dataGen, memoryLayout, config.compressionMode);
        std::string csvDir = "/home/maseiler/Coding/nebulastream/cmake-build-debug/nes-benchmark/";
        bench.writeBuffersToCSV(csvDir, 0);
    }

    void TearDown([[maybe_unused]] const ::benchmark::State& state) { std::cout << "TearDown complete" << std::endl; }

    BenchmarkCompression bench;
    std::vector<CompressionAlgorithm> compressionAlgorithms;

};


void compress(BenchmarkCompression bench, std::vector<CompressionAlgorithm>& compressionAlgorithms) {
    bench.compress(compressionAlgorithms);
}

void decompress(BenchmarkCompression bench) { bench.decompress(); }

BENCHMARK_DEFINE_F(MyFixture2, BarTest)([[maybe_unused]] benchmark::State& st) {
    for (auto _ : st) {
        compress(bench, compressionAlgorithms);
        decompress(bench);
        std::cout << "Benchmark complete" << std::endl;
    }
}

BENCHMARK_REGISTER_F(MyFixture2, BarTest)->Iterations(10);
BENCHMARK_MAIN();
*/