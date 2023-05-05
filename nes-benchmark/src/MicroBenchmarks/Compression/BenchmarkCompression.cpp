#include "API/Schema.hpp"
#include "DataGeneration/DefaultDataGenerator.hpp"
#include "DataGeneration/ZipfianDataGenerator.hpp"
#include <DataGeneration/YSBDataGenerator.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Exceptions/SignalHandling.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/CompressedDynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <chrono>

using namespace NES::Benchmark;

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
    BenchmarkCompression(size_t bufferSize,
                         NES::Benchmark::DataGeneration::DataGenerator& dataGenerator,
                         const std::shared_ptr<NES::Runtime::MemoryLayouts::MemoryLayout>& memoryLayout,
                         NES::Runtime::MemoryLayouts::CompressionMode cm);
    void run();

    const size_t numberOfBuffersToProduce = 10;

  private:
    size_t bufferSize;
    std::vector<NES::Runtime::MemoryLayouts::CompressedDynamicTupleBuffer> buffers;
    uint8_t* origBuffers[10]{};// TODO dynamic (numberOfBuffersToProduce)
    double compress(NES::Runtime::MemoryLayouts::CompressionAlgorithm ca);
    void decompressAndVerify();
};

BenchmarkCompression::BenchmarkCompression(const size_t bufferSize,
                                           NES::Benchmark::DataGeneration::DataGenerator& dataGenerator,
                                           const std::shared_ptr<NES::Runtime::MemoryLayouts::MemoryLayout>& memoryLayout,
                                           NES::Runtime::MemoryLayouts::CompressionMode cm) {
    NES::Logger::setupLogging("BenchmarkCompression.log", NES::LogLevel::LOG_DEBUG);
    auto listener = std::make_shared<ErrorHandler>();
    NES::Exceptions::installGlobalErrorListener(listener);

    this->bufferSize = bufferSize;
    NES::Runtime::BufferManagerPtr bufferManager =
        std::make_shared<NES::Runtime::BufferManager>(bufferSize, numberOfBuffersToProduce);

    // create data
    dataGenerator.setBufferManager(bufferManager);
    auto tmpBuffers = dataGenerator.createData(numberOfBuffersToProduce, bufferSize);

    buffers.reserve(tmpBuffers.size());
    for (const auto& tmpBuffer : tmpBuffers) {
        buffers.emplace_back(memoryLayout, tmpBuffer, cm);
    }
    for (size_t i = 0; i < tmpBuffers.size(); i++) {
        auto* bufferOrig = (uint8_t*) malloc(bufferSize);
        memcpy(bufferOrig, tmpBuffers[i].getBuffer(), bufferSize);
        origBuffers[i] = bufferOrig;
    }
}

void BenchmarkCompression::run() {
    std::cout << "------------------------------\n"
              << "Algo\tRatio\tSpeed\n";
    // compress and verify
    compress(NES::Runtime::MemoryLayouts::CompressionAlgorithm::LZ4);
    decompressAndVerify();

    compress(NES::Runtime::MemoryLayouts::CompressionAlgorithm::SNAPPY);
    decompressAndVerify();

    compress(NES::Runtime::MemoryLayouts::CompressionAlgorithm::FSST);
    decompressAndVerify();

    compress(NES::Runtime::MemoryLayouts::CompressionAlgorithm::RLE);
    decompressAndVerify();
    std::cout << "------------------------------\n";
}

double BenchmarkCompression::compress(NES::Runtime::MemoryLayouts::CompressionAlgorithm ca) {
    double totalCompressionRatio = 0;
    auto start = std::chrono::high_resolution_clock::now();
    for (auto& buffer : buffers) {
        buffer.compress(ca);
        //std::cout << buffer.getCompressionRatio() << std::endl;
        totalCompressionRatio += buffer.getCompressionRatio();
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> ms_double = end - start;
    std::cout << getCompressionAlgorithmName(ca) << "\t" << totalCompressionRatio << "\t" << ms_double.count() << " ms"
              << std::endl;
    return totalCompressionRatio;
}

void BenchmarkCompression::decompressAndVerify() {
    for (size_t i = 0; i < buffers.size(); i++) {
        buffers[i].decompress();
        auto contentOrig = reinterpret_cast<const char*>(origBuffers[i]);
        auto contentCompressed = reinterpret_cast<const char*>(buffers[i].getBuffer().getBuffer());
        bool contentIsEqual = memcmp(contentOrig, contentCompressed, bufferSize) == 0;

        auto startX = contentOrig;
        auto startY = contentCompressed;
        for (size_t j = 0; j < bufferSize; j++) {
            if (memcmp(startX, startY, 1) != 0) {
                std::cout << i << "," << j << ": \"" << startX << "\" \"" << startY << "\"\n";
            }
            startX++;
            startY++;
        }
        NES_ASSERT(contentIsEqual, "Decompressed content does not equal original content.");
    }
}

void benchmarkYsb(const size_t numberOfTuples = 10) {
    NES::Benchmark::DataGeneration::YSBDataGenerator dataGenerator;
    const size_t bufferSizeInBytes = dataGenerator.getSchema().get()->getSchemaSizeInBytes() * numberOfTuples;

    /*
    // RowLayout, Horizontal
    auto rowLayout = NES::Runtime::MemoryLayouts::RowLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
    auto cm = NES::Runtime::MemoryLayouts::CompressionMode::HORIZONTAL;
    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, rowLayout, cm);
    std::cout << "MemoryLayout: RowLayout\n"
              << "CompressionMode: Horizontal\n"
              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
              << "Buffer Size: " << bufferSizeInBytes << "\n"
              << "Num Tuples: " << numberOfTuples << "\n"
              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
    b.run();

    // ColumnLayout, Horizontal
    auto columnLayout = NES::Runtime::MemoryLayouts::ColumnLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
    auto cm = NES::Runtime::MemoryLayouts::CompressionMode::HORIZONTAL;
    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, columnLayout, cm);
    std::cout << "MemoryLayout: ColumnLayout\n"
              << "CompressionMode: Horizontal\n"
              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
              << "Buffer Size: " << bufferSizeInBytes << "\n"
              << "Num Tuples: " << numberOfTuples << "\n"
              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
    b.run();
    */
    // ColumnLayout, Vertical
    auto columnLayout = NES::Runtime::MemoryLayouts::ColumnLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
    auto cm = NES::Runtime::MemoryLayouts::CompressionMode::VERTICAL;
    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, columnLayout, cm);
    std::cout << "MemoryLayout: ColumnLayout\n"
              << "CompressionMode: Vertical\n"
              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
              << "Buffer Size: " << bufferSizeInBytes << "\n"
              << "Num Tuples: " << numberOfTuples << "\n"
              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
    b.run();
}

void benchmarkUniform(const size_t numberOfTuples = 10, const size_t min = 0, const size_t max = 100) {
    NES::Benchmark::DataGeneration::DefaultDataGenerator dataGenerator(min, max);
    const size_t bufferSizeInBytes = dataGenerator.getSchema().get()->getSchemaSizeInBytes() * numberOfTuples;

    /*
    // RowLayout, Horizontal
    auto rowLayout = NES::Runtime::MemoryLayouts::RowLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
    auto cm = NES::Runtime::MemoryLayouts::CompressionMode::HORIZONTAL;
    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, rowLayout, cm);
    std::cout << "MemoryLayout: RowLayout\n"
              << "CompressionMode: Horizontal\n"
              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
              << "Buffer Size: " << bufferSizeInBytes << "\n"
              << "Num Tuples: " << numberOfTuples << "\n"
              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
    b.run();

    // ColumnLayout, Horizontal
    auto columnLayout = NES::Runtime::MemoryLayouts::ColumnLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
    auto cm = NES::Runtime::MemoryLayouts::CompressionMode::HORIZONTAL;
    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, columnLayout, cm);
    std::cout << "MemoryLayout: ColumnLayout\n"
              << "CompressionMode: Horizontal\n"
              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
              << "Buffer Size: " << bufferSizeInBytes << "\n"
              << "Num Tuples: " << numberOfTuples << "\n"
              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
    b.run();
    */
    // ColumnLayout, Vertical
    auto columnLayout = NES::Runtime::MemoryLayouts::ColumnLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
    auto cm = NES::Runtime::MemoryLayouts::CompressionMode::VERTICAL;
    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, columnLayout, cm);
    std::cout << "MemoryLayout: ColumnLayout\n"
              << "CompressionMode: Vertical\n"
              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
              << "Buffer Size: " << bufferSizeInBytes << "\n"
              << "Num Tuples: " << numberOfTuples << "\n"
              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
    b.run();
}

void benchmarkZipf(const size_t numberOfTuples = 10, const size_t min = 0, const size_t max = 100) {
    NES::Benchmark::DataGeneration::ZipfianDataGenerator dataGenerator(0.9, min, max);
    const size_t bufferSizeInBytes = dataGenerator.getSchema().get()->getSchemaSizeInBytes() * numberOfTuples;

    /*
    // RowLayout, Horizontal
    auto rowLayout = NES::Runtime::MemoryLayouts::RowLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
    auto cm = NES::Runtime::MemoryLayouts::CompressionMode::HORIZONTAL;
    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, rowLayout, cm);
    std::cout << "MemoryLayout: RowLayout\n"
              << "CompressionMode: Horizontal\n"
              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
              << "Buffer Size: " << bufferSizeInBytes << "\n"
              << "Num Tuples: " << numberOfTuples << "\n"
              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
    b.run();

    // ColumnLayout, Horizontal
    auto columnLayout = NES::Runtime::MemoryLayouts::ColumnLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
    auto cm = NES::Runtime::MemoryLayouts::CompressionMode::HORIZONTAL;
    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, columnLayout, cm);
    std::cout << "MemoryLayout: ColumnLayout\n"
              << "CompressionMode: Horizontal\n"
              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
              << "Buffer Size: " << bufferSizeInBytes << "\n"
              << "Num Tuples: " << numberOfTuples << "\n"
              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
    b.run();
    */
    // ColumnLayout, Vertical
    auto columnLayout = NES::Runtime::MemoryLayouts::ColumnLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
    auto cm = NES::Runtime::MemoryLayouts::CompressionMode::VERTICAL;
    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, columnLayout, cm);
    std::cout << "MemoryLayout: ColumnLayout\n"
              << "CompressionMode: Vertical\n"
              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
              << "Buffer Size: " << bufferSizeInBytes << "\n"
              << "Num Tuples: " << numberOfTuples << "\n"
              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
    b.run();
}

int main() {
    benchmarkYsb();
    benchmarkUniform();
    benchmarkZipf();
    return 0;
}