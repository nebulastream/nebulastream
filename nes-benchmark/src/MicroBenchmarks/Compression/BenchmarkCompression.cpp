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
#include <gmock/gmock-matchers.h>

using namespace NES::Benchmark::DataGeneration;
using namespace NES::Runtime::MemoryLayouts;

enum class MemoryLayout_ { ROW, COLUMN };

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
                         DataGenerator& dataGenerator,
                         const std::shared_ptr<MemoryLayout>& memoryLayout,
                         CompressionMode cm);
    const size_t numberOfBuffersToProduce = 10;
    void run();

  private:
    size_t bufferSize;
    std::vector<CompressedDynamicTupleBuffer> buffers;
    uint8_t* origBuffers[10]{};// TODO dynamic (numberOfBuffersToProduce)
    double compress(CompressionAlgorithm ca);
    void decompressAndVerify();
};

BenchmarkCompression::BenchmarkCompression(const size_t bufferSize,
                                           DataGenerator& dataGenerator,
                                           const std::shared_ptr<MemoryLayout>& memoryLayout,
                                           CompressionMode cm) {
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
    compress(CompressionAlgorithm::LZ4);
    decompressAndVerify();

    compress(CompressionAlgorithm::SNAPPY);
    decompressAndVerify();

    compress(CompressionAlgorithm::FSST);
    decompressAndVerify();

    //compress(CompressionAlgorithm::BINARY_RLE);
    // TODO fix bug ↓
    //decompressAndVerify();

    try {
        compress(CompressionAlgorithm::SPRINTZ);
        decompressAndVerify();
    } catch (NES::Exceptions::RuntimeException const& err) {
        // compressed size  might be larger than original for depending on data value distribution
        EXPECT_THAT(err.what(), ::testing::HasSubstr("Sprintz compression failed: compressed size"));
    }

    std::cout << "------------------------------\n";
}

double BenchmarkCompression::compress(CompressionAlgorithm ca) {
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

        /*
        // find unequal position(s)
        auto startX = contentOrig;
        auto startY = contentCompressed;
        for (size_t j = 0; j < bufferSize; j++) {
            if (memcmp(startX, startY, 1) != 0) {
                std::cout << i << "," << j << ": \"" << startX << "\" \"" << startY << "\"\n";
            }
            startX++;
            startY++;
        }
        */
        //NES_ASSERT(contentIsEqual, "Decompressed content does not equal original content.");
        if (!contentIsEqual)
            NES_WARNING("Decompressed content does not equal original content.")
    }
}

void benchmarkYsb(const MemoryLayout_ ml, const CompressionMode cm, const size_t numberOfTuples = 10) {
    YSBDataGenerator dataGenerator;
    const size_t bufferSizeInBytes = dataGenerator.getSchema().get()->getSchemaSizeInBytes() * numberOfTuples;

    switch (ml) {
        case MemoryLayout_::ROW: {
            switch (cm) {
                case CompressionMode::HORIZONTAL: {
                    auto rowLayout = RowLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
                    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, rowLayout, cm);
                    std::cout << "MemoryLayout: RowLayout\n"
                              << "CompressionMode: Horizontal\n"
                              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
                              << "Buffer Size: " << bufferSizeInBytes << "\n"
                              << "Num Tuples: " << numberOfTuples << "\n"
                              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
                    b.run();
                    break;
                }
                case CompressionMode::VERTICAL: {
                    // TODO
                    break;
                }
            }
        }
        case MemoryLayout_::COLUMN:
            switch (cm) {
                case CompressionMode::HORIZONTAL: {
                    auto columnLayout = ColumnLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
                    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, columnLayout, cm);
                    std::cout << "MemoryLayout: ColumnLayout\n"
                              << "CompressionMode: Horizontal\n"
                              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
                              << "Buffer Size: " << bufferSizeInBytes << "\n"
                              << "Num Tuples: " << numberOfTuples << "\n"
                              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
                    b.run();
                    break;
                }
                case CompressionMode::VERTICAL: {
                    auto columnLayout = ColumnLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
                    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, columnLayout, cm);
                    std::cout << "MemoryLayout: ColumnLayout\n"
                              << "CompressionMode: Vertical\n"
                              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
                              << "Buffer Size: " << bufferSizeInBytes << "\n"
                              << "Num Tuples: " << numberOfTuples << "\n"
                              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
                    b.run();
                    break;
                }
            }
            break;
    }
}

void benchmarkUniform(const MemoryLayout_ ml,
                      const CompressionMode cm,
                      const size_t numberOfTuples = 10,
                      const size_t min = 0,
                      const size_t max = 100) {
    DefaultDataGenerator dataGenerator(min, max);
    const size_t bufferSizeInBytes = dataGenerator.getSchema().get()->getSchemaSizeInBytes() * numberOfTuples;

    switch (ml) {
        case MemoryLayout_::ROW: {
            switch (cm) {
                case CompressionMode::HORIZONTAL: {
                    auto rowLayout = RowLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
                    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, rowLayout, cm);
                    std::cout << "MemoryLayout: RowLayout\n"
                              << "CompressionMode: Horizontal\n"
                              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
                              << "Buffer Size: " << bufferSizeInBytes << "\n"
                              << "Num Tuples: " << numberOfTuples << "\n"
                              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
                    b.run();
                    break;
                }
                case CompressionMode::VERTICAL: {
                    // TODO throw
                    break;
                }
            }
            break;
        }
        case MemoryLayout_::COLUMN: {
            switch (cm) {
                case CompressionMode::HORIZONTAL: {
                    auto columnLayout = ColumnLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
                    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, columnLayout, cm);
                    std::cout << "MemoryLayout: ColumnLayout\n"
                              << "CompressionMode: Horizontal\n"
                              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
                              << "Buffer Size: " << bufferSizeInBytes << "\n"
                              << "Num Tuples: " << numberOfTuples << "\n"
                              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
                    b.run();
                    break;
                }
                case CompressionMode::VERTICAL: {
                    auto columnLayout = ColumnLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
                    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, columnLayout, cm);
                    std::cout << "MemoryLayout: ColumnLayout\n"
                              << "CompressionMode: Vertical\n"
                              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
                              << "Buffer Size: " << bufferSizeInBytes << "\n"
                              << "Num Tuples: " << numberOfTuples << "\n"
                              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
                    b.run();
                    break;
                }
            }
            break;
        }
    }
}

void benchmarkZipf(const MemoryLayout_ ml,
                   const CompressionMode cm,
                   const size_t numberOfTuples = 10,
                   const double alpha = 0.9,
                   const size_t min = 0,
                   const size_t max = 100) {
    ZipfianDataGenerator dataGenerator(alpha, min, max);
    const size_t bufferSizeInBytes = dataGenerator.getSchema().get()->getSchemaSizeInBytes() * numberOfTuples;

    switch (ml) {
        case MemoryLayout_::ROW: {
            switch (cm) {
                case CompressionMode::HORIZONTAL: {
                    auto rowLayout = RowLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
                    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, rowLayout, cm);
                    std::cout << "MemoryLayout: RowLayout\n"
                              << "CompressionMode: Horizontal\n"
                              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
                              << "Buffer Size: " << bufferSizeInBytes << "\n"
                              << "Num Tuples: " << numberOfTuples << "\n"
                              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
                    b.run();
                    break;
                }
                case CompressionMode::VERTICAL: {
                    // TODO throw
                    break;
                }
            }
            break;
        }
        case MemoryLayout_::COLUMN: {
            switch (cm) {
                case CompressionMode::HORIZONTAL: {
                    auto columnLayout = ColumnLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
                    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, columnLayout, cm);
                    std::cout << "MemoryLayout: ColumnLayout\n"
                              << "CompressionMode: Horizontal\n"
                              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
                              << "Buffer Size: " << bufferSizeInBytes << "\n"
                              << "Num Tuples: " << numberOfTuples << "\n"
                              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
                    b.run();
                    break;
                }
                case CompressionMode::VERTICAL: {
                    auto columnLayout = ColumnLayout::create(dataGenerator.getSchema(), bufferSizeInBytes);
                    auto b = BenchmarkCompression(bufferSizeInBytes, dataGenerator, columnLayout, cm);
                    std::cout << "MemoryLayout: ColumnLayout\n"
                              << "CompressionMode: Vertical\n"
                              << "Num Buffers: " << b.numberOfBuffersToProduce << "\n"
                              << "Buffer Size: " << bufferSizeInBytes << "\n"
                              << "Num Tuples: " << numberOfTuples << "\n"
                              << "Schema Size: " << dataGenerator.getSchema().get()->getSchemaSizeInBytes() << "\n";
                    b.run();
                    break;
                }
            }
            break;
        }
    }
}

int main() {
    MemoryLayout_ ml = MemoryLayout_::COLUMN;
    CompressionMode cm = CompressionMode::VERTICAL;

    benchmarkYsb(ml, cm, 100);
    benchmarkUniform(ml, cm, 100, 0, 10);
    benchmarkZipf(ml, cm, 100, 0.1);
    return 0;
}