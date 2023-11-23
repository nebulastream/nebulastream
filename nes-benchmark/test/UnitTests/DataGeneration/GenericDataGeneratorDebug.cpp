#include <Exceptions/ErrorListener.hpp>
#include <Exceptions/SignalHandling.hpp>
#include <iostream>
#include <typeindex>

#include "DataGeneration/GenericDataDistribution.hpp"
#include "DataGeneration/GenericDataGen.hpp"
#include "DataGeneration/GenericDataGenerator.hpp"
#include <API/Schema.hpp>
#include <DataGeneration/data/TextDataSet.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/CompressedDynamicTupleBuffer.hpp>

using namespace NES::Benchmark::DataGeneration;
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

void debugRepeatingValues() {
    size_t seed = 42;
    uint8_t minValue = 65;
    uint8_t maxValue = 72;
    size_t numRepeats = 5;
    int sigma = 2;
    double changeProbability = 0.5;
    std::vector<DistributionPtr> dists;
    auto dist1 =
        std::make_shared<RepeatingValuesDistribution<uint8_t>>(minValue, maxValue, numRepeats, sigma, changeProbability, seed);
    dists.push_back(dist1);
    auto dist2 =
        std::make_shared<RepeatingValuesDistribution<int32_t>>(minValue, maxValue, numRepeats, sigma, changeProbability, seed);
    dists.push_back(dist2);
    auto dist3 =
        std::make_shared<RepeatingValuesDistribution<double>>(minValue, maxValue, numRepeats, sigma, changeProbability, seed);
    dists.push_back(dist3);
    auto dist4 =
        std::make_shared<RepeatingValuesDistribution<uint64_t>>(minValue, maxValue, numRepeats, sigma, changeProbability, seed);
    dists.push_back(dist4);
    auto dist5 = std::make_shared<RepeatingValuesDistribution<std::string>>(minValue,
                                                                            maxValue,
                                                                            numRepeats,
                                                                            sigma,
                                                                            changeProbability,
                                                                            seed);
    //dist5->setDataSet(DataSet::IPSUM_WORDS);
    dist5->setDataSet(DataSet::IPSUM_SENTENCES);
    dists.push_back(dist5);

    size_t numBuffers = 1;
    size_t bufferSize = 10000;
    NES::Runtime::BufferManagerPtr bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, numBuffers);
    NES::Schema::MemoryLayoutType memoryLayoutType = NES::Schema::MemoryLayoutType::COLUMNAR_LAYOUT;
    GenericDataGen dataGen = GenericDataGen(memoryLayoutType, dists);
    dataGen.setBufferManager(bufferManager);
    auto buffers = dataGen.createData(numBuffers, bufferSize);

    MemoryLayoutPtr memoryLayout = ColumnLayout::create(dataGen.getSchema(), bufferSize);
    auto dynBuffer = CompressedDynamicTupleBuffer(memoryLayout, buffers[0]);
    for (size_t row = 0; row < dynBuffer.getCapacity(); row++) {
        auto c1 = dynBuffer[row][0].read<uint8_t>();
        auto c2 = dynBuffer[row][1].read<int32_t>();
        auto c3 = dynBuffer[row][2].read<double>();
        auto c4 = dynBuffer[row][3].read<uint64_t>();
        //auto c5 = dynBuffer[row][4].read<NES::ExecutableTypes::Array<char, IpsumWords::maxValueLength>>();
        auto c5 = dynBuffer[row][4].read<NES::ExecutableTypes::Array<char, IpsumSentences::maxValueLength>>();
        std::cout << (int) c1 << "\t" << c2 << "\t" << c3 << "\t" << c4 << "\t" << c5 << std::endl;
    }
}

void debugUniform() {
    size_t seed = 42;
    uint8_t minValue = 65;
    uint8_t maxValue = 72;
    size_t numRepeats = 5;
    int sigma = 2;
    double changeProbability = 0.5;
    std::vector<DistributionPtr> dists;
    auto dist1 = std::make_shared<UniformDistribution<uint8_t>>(minValue, maxValue, seed);
    dists.push_back(dist1);
    auto dist2 = std::make_shared<UniformDistribution<int32_t>>(minValue, maxValue, seed);
    dists.push_back(dist2);
    auto dist3 = std::make_shared<UniformDistribution<double>>(minValue, maxValue, seed);
    dists.push_back(dist3);
    auto dist4 = std::make_shared<UniformDistribution<uint64_t>>(minValue, maxValue, seed);
    dists.push_back(dist4);
    auto dist5 = std::make_shared<UniformDistribution<std::string>>(minValue, maxValue, seed);
    //dist5->setDataSet(DataSet::IPSUM_WORDS);
    dist5->setDataSet(DataSet::IPSUM_SENTENCES);
    dists.push_back(dist5);

    size_t numBuffers = 1;
    size_t bufferSize = 1000;
    NES::Runtime::BufferManagerPtr bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, numBuffers);
    NES::Schema::MemoryLayoutType memoryLayoutType = NES::Schema::MemoryLayoutType::COLUMNAR_LAYOUT;
    GenericDataGen dataGen = GenericDataGen(memoryLayoutType, dists);
    dataGen.setBufferManager(bufferManager);
    auto buffers = dataGen.createData(numBuffers, bufferSize);

    MemoryLayoutPtr memoryLayout = ColumnLayout::create(dataGen.getSchema(), bufferSize);
    auto dynBuffer = CompressedDynamicTupleBuffer(memoryLayout, buffers[0]);
    for (size_t row = 0; row < dynBuffer.getCapacity(); row++) {
        auto c1 = dynBuffer[row][0].read<uint8_t>();
        auto c2 = dynBuffer[row][1].read<int32_t>();
        auto c3 = dynBuffer[row][2].read<double>();
        auto c4 = dynBuffer[row][3].read<uint64_t>();
        //auto c5 = dynBuffer[row][4].read<NES::ExecutableTypes::Array<char, IpsumWords::maxValueLength>>();
        auto c5 = dynBuffer[row][4].read<NES::ExecutableTypes::Array<char, IpsumSentences::maxValueLength>>();
        std::cout << (int) c1 << "\t" << c2 << "\t" << c3 << "\t" << c4 << "\t" << c5 << std::endl;
    }
}

void debugBinomial() {
    size_t seed = 42;
    uint8_t minValue = 65;
    uint8_t maxValue = 72;
    double probability = 0.5;
    std::vector<DistributionPtr> dists;
    auto dist1 = std::make_shared<BinomialDistribution<uint8_t>>(minValue, maxValue, probability, seed);
    dists.push_back(dist1);
    auto dist2 = std::make_shared<BinomialDistribution<int32_t>>(minValue, maxValue, probability, seed);
    dists.push_back(dist2);
    auto dist3 = std::make_shared<BinomialDistribution<double>>(minValue, maxValue, probability, seed);
    dists.push_back(dist3);
    auto dist4 = std::make_shared<BinomialDistribution<uint64_t>>(minValue, maxValue, probability, seed);
    dists.push_back(dist4);
    auto dist5 = std::make_shared<BinomialDistribution<std::string>>(minValue, maxValue, probability, seed);
    dist5->setDataSet(DataSet::IPSUM_WORDS);
    //dist5->setDataSet(DataSet::IPSUM_SENTENCES);
    dists.push_back(dist5);

    size_t numBuffers = 1;
    size_t bufferSize = 1000;
    NES::Runtime::BufferManagerPtr bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, numBuffers);
    NES::Schema::MemoryLayoutType memoryLayoutType = NES::Schema::MemoryLayoutType::COLUMNAR_LAYOUT;
    GenericDataGen dataGen = GenericDataGen(memoryLayoutType, dists);
    dataGen.setBufferManager(bufferManager);
    auto buffers = dataGen.createData(numBuffers, bufferSize);

    MemoryLayoutPtr memoryLayout = ColumnLayout::create(dataGen.getSchema(), bufferSize);
    auto dynBuffer = CompressedDynamicTupleBuffer(memoryLayout, buffers[0]);
    for (size_t row = 0; row < dynBuffer.getCapacity(); row++) {
        auto c1 = dynBuffer[row][0].read<uint8_t>();
        auto c2 = dynBuffer[row][1].read<int32_t>();
        auto c3 = dynBuffer[row][2].read<double>();
        auto c4 = dynBuffer[row][3].read<uint64_t>();
        auto c5 = dynBuffer[row][4].read<NES::ExecutableTypes::Array<char, IpsumWords::maxValueLength>>();
        //auto c5 = dynBuffer[row][4].read<NES::ExecutableTypes::Array<char, IpsumSentences::maxValueLength>>();
        std::cout << (int) c1 << "\t" << c2 << "\t" << c3 << "\t" << c4 << "\t" << c5 << std::endl;
    }
}

void debugZipfian() {
    size_t seed = 42;
    uint8_t minValue = 65;
    uint8_t maxValue = 72;
    double probability = 0.5;
    std::vector<DistributionPtr> dists;
    auto dist1 = std::make_shared<ZipfianDistribution<uint8_t>>(minValue, maxValue, probability, seed);
    dists.push_back(dist1);
    auto dist2 = std::make_shared<ZipfianDistribution<int32_t>>(minValue, maxValue, probability, seed);
    dists.push_back(dist2);
    auto dist3 = std::make_shared<ZipfianDistribution<double>>(minValue, maxValue, probability, seed);
    dists.push_back(dist3);
    auto dist4 = std::make_shared<ZipfianDistribution<uint64_t>>(minValue, maxValue, probability, seed);
    dists.push_back(dist4);
    auto dist5 = std::make_shared<ZipfianDistribution<std::string>>(minValue, maxValue, probability, seed);
    //dist5->setDataSet(DataSet::IPSUM_WORDS);
    dist5->setDataSet(DataSet::IPSUM_SENTENCES);
    dists.push_back(dist5);

    size_t numBuffers = 1;
    size_t bufferSize = 1000;
    NES::Runtime::BufferManagerPtr bufferManager = std::make_shared<NES::Runtime::BufferManager>(bufferSize, numBuffers);
    NES::Schema::MemoryLayoutType memoryLayoutType = NES::Schema::MemoryLayoutType::COLUMNAR_LAYOUT;
    GenericDataGen dataGen = GenericDataGen(memoryLayoutType, dists);
    dataGen.setBufferManager(bufferManager);
    auto buffers = dataGen.createData(numBuffers, bufferSize);

    MemoryLayoutPtr memoryLayout = ColumnLayout::create(dataGen.getSchema(), bufferSize);
    auto dynBuffer = CompressedDynamicTupleBuffer(memoryLayout, buffers[0]);
    for (size_t row = 0; row < dynBuffer.getCapacity(); row++) {
        auto c1 = dynBuffer[row][0].read<uint8_t>();
        auto c2 = dynBuffer[row][1].read<int32_t>();
        auto c3 = dynBuffer[row][2].read<double>();
        auto c4 = dynBuffer[row][3].read<uint64_t>();
        // auto c5 = dynBuffer[row][4].read<NES::ExecutableTypes::Array<char, IpsumWords::maxValueLength>>();
        auto c5 = dynBuffer[row][4].read<NES::ExecutableTypes::Array<char, IpsumSentences::maxValueLength>>();
        std::cout << (int) c1 << "\t" << c2 << "\t" << c3 << "\t" << c4 << "\t" << c5 << std::endl;
    }
}

int main() {
    NES::Logger::setupLogging("GenericDataGeneratorDebug.log", NES::LogLevel::LOG_DEBUG);
    auto listener = std::make_shared<ErrorHandler>();
    NES::Exceptions::installGlobalErrorListener(listener);

    //debugRepeatingValues();
    //debugUniform();
    //debugBinomial();
    debugZipfian();
}
