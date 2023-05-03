#include "DataGeneration/DefaultDataGenerator.hpp"
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

const size_t ysbTotalTypeSize = 78;
const size_t numberOfTuples = 10;
const size_t bufferSizeInBytes = ysbTotalTypeSize * numberOfTuples;
const size_t numberOfBuffersToProduce = 10;

class ErrorHandler : public NES::Exceptions::ErrorListener {
  public:
    void onFatalError(int signalNumber, std::string callstack) override {
        std::cout << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack " << callstack;
    }

    void onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) override {
        std::cout << "onFatalException: exception=[" << exception->what() << "] callstack=\n" << callstack;
    }
};

std::string defaultTupleToString(std::tuple<uint64_t, uint64_t, uint64_t, uint64_t> tuple) {
    std::string out;
    out += std::to_string(get<0>(tuple)) += "\t";
    out += std::to_string(get<1>(tuple)) += "\t";
    out += std::to_string(get<2>(tuple)) += "\t";
    out += std::to_string(get<3>(tuple)) += "\t";
    return out;
}

std::string ysbTupleToString(
    std::tuple<uint64_t, uint64_t, uint64_t, uint64_t, uint64_t, uint64_t, uint64_t, uint64_t, uint64_t, uint32_t, uint16_t>
        tuple) {
    std::string out;
    out += std::to_string(get<0>(tuple)) += "\t";
    out += std::to_string(get<1>(tuple)) += "\t";
    out += std::to_string(get<2>(tuple)) += "\t";//
    out += std::to_string(get<3>(tuple)) += "\t";
    out += std::to_string(get<4>(tuple)) += "\t";//
    out += std::to_string(get<5>(tuple)) += "\t";//
    out += std::to_string(get<6>(tuple)) += "\t";
    out += std::to_string(get<7>(tuple)) += "\t";
    out += std::to_string(get<8>(tuple)) += "\t";
    out += std::to_string(get<9>(tuple)) += "\t";
    out += std::to_string(get<10>(tuple));
    return out;
}

double compress(std::vector<NES::Runtime::MemoryLayouts::CompressedDynamicTupleBuffer>& buffers,
                NES::Runtime::MemoryLayouts::CompressionAlgorithm ca) {
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

static void decompressAndVerify(uint8_t* origBuffers[],
                                std::vector<NES::Runtime::MemoryLayouts::CompressedDynamicTupleBuffer>& buffers,
                                const size_t bufferSize) {
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

void rowLayoutHorizontalYsb() {
    NES::Logger::setupLogging("BenchmarkCompression.log", NES::LogLevel::LOG_DEBUG);
    auto listener = std::make_shared<ErrorHandler>();
    NES::Exceptions::installGlobalErrorListener(listener);

    NES::Runtime::BufferManagerPtr bufferManager =
        std::make_shared<NES::Runtime::BufferManager>(bufferSizeInBytes, numberOfBuffersToProduce);

    // create data
    NES::Benchmark::DataGeneration::YSBDataGenerator dataGenerator;
    dataGenerator.setBufferManager(bufferManager);
    auto rowLayout = NES::Runtime::MemoryLayouts::RowLayout::create(dataGenerator.getSchema(), bufferManager->getBufferSize());
    auto tmpBuffers = dataGenerator.createData(numberOfBuffersToProduce, bufferSizeInBytes);
    std::vector<NES::Runtime::MemoryLayouts::CompressedDynamicTupleBuffer> buffers;
    buffers.reserve(tmpBuffers.size());
    for (const auto& tmpBuffer : tmpBuffers) {
        buffers.emplace_back(rowLayout, tmpBuffer, NES::Runtime::MemoryLayouts::CompressionMode::HORIZONTAL);
    }
    uint8_t* origBuffers[10];// TODO dynamic (numberOfBuffersToProduce)
    for (size_t i = 0; i < tmpBuffers.size(); i++) {
        auto* bufferOrig = (uint8_t*) malloc(bufferSizeInBytes);
        memcpy(bufferOrig, tmpBuffers[i].getBuffer(), bufferSizeInBytes);
        origBuffers[i] = bufferOrig;
    }

    std::cout << "Algo\tRatio\tSpeed" << std::endl;
    // compress and verify
    compress(buffers, NES::Runtime::MemoryLayouts::CompressionAlgorithm::LZ4);
    decompressAndVerify(origBuffers, buffers, bufferSizeInBytes);

    compress(buffers, NES::Runtime::MemoryLayouts::CompressionAlgorithm::SNAPPY);
    decompressAndVerify(origBuffers, buffers, bufferSizeInBytes);

    compress(buffers, NES::Runtime::MemoryLayouts::CompressionAlgorithm::FSST);
    decompressAndVerify(origBuffers, buffers, bufferSizeInBytes);

    compress(buffers, NES::Runtime::MemoryLayouts::CompressionAlgorithm::RLE);
    decompressAndVerify(origBuffers, buffers, bufferSizeInBytes);
}

void columnLayoutHorizontalYsb() {
    NES::Logger::setupLogging("BenchmarkCompression.log", NES::LogLevel::LOG_DEBUG);
    auto listener = std::make_shared<ErrorHandler>();
    NES::Exceptions::installGlobalErrorListener(listener);

    NES::Runtime::BufferManagerPtr bufferManager =
        std::make_shared<NES::Runtime::BufferManager>(bufferSizeInBytes, numberOfBuffersToProduce);

    // create data
    NES::Benchmark::DataGeneration::YSBDataGenerator dataGenerator;
    dataGenerator.setBufferManager(bufferManager);
    auto columnLayout =
        NES::Runtime::MemoryLayouts::ColumnLayout::create(dataGenerator.getSchema(), bufferManager->getBufferSize());
    auto tmpBuffers = dataGenerator.createData(numberOfBuffersToProduce, bufferSizeInBytes);
    std::vector<NES::Runtime::MemoryLayouts::CompressedDynamicTupleBuffer> buffers;
    buffers.reserve(tmpBuffers.size());
    for (const auto& tmpBuffer : tmpBuffers) {
        buffers.emplace_back(columnLayout, tmpBuffer, NES::Runtime::MemoryLayouts::CompressionMode::HORIZONTAL);
    }
    uint8_t* origBuffers[10];// TODO dynamic (numberOfBuffersToProduce)
    for (size_t i = 0; i < tmpBuffers.size(); i++) {
        auto* bufferOrig = (uint8_t*) malloc(bufferSizeInBytes);
        memcpy(bufferOrig, tmpBuffers[i].getBuffer(), bufferSizeInBytes);
        origBuffers[i] = bufferOrig;
    }

    std::cout << "Algo\tRatio\tSpeed" << std::endl;
    // compress and verify
    compress(buffers, NES::Runtime::MemoryLayouts::CompressionAlgorithm::LZ4);
    decompressAndVerify(origBuffers, buffers, bufferSizeInBytes);

    compress(buffers, NES::Runtime::MemoryLayouts::CompressionAlgorithm::SNAPPY);
    decompressAndVerify(origBuffers, buffers, bufferSizeInBytes);

    compress(buffers, NES::Runtime::MemoryLayouts::CompressionAlgorithm::FSST);
    decompressAndVerify(origBuffers, buffers, bufferSizeInBytes);

    compress(buffers, NES::Runtime::MemoryLayouts::CompressionAlgorithm::RLE);
    decompressAndVerify(origBuffers, buffers, bufferSizeInBytes);
}

void columnLayoutVerticalYsb() {
    NES::Logger::setupLogging("BenchmarkCompression.log", NES::LogLevel::LOG_DEBUG);
    auto listener = std::make_shared<ErrorHandler>();
    NES::Exceptions::installGlobalErrorListener(listener);

    NES::Runtime::BufferManagerPtr bufferManager =
        std::make_shared<NES::Runtime::BufferManager>(bufferSizeInBytes, numberOfBuffersToProduce);

    // create data
    NES::Benchmark::DataGeneration::YSBDataGenerator dataGenerator;
    dataGenerator.setBufferManager(bufferManager);
    auto columnLayout =
        NES::Runtime::MemoryLayouts::ColumnLayout::create(dataGenerator.getSchema(), bufferManager->getBufferSize());
    auto tmpBuffers = dataGenerator.createData(numberOfBuffersToProduce, bufferSizeInBytes);
    std::vector<NES::Runtime::MemoryLayouts::CompressedDynamicTupleBuffer> buffers;
    buffers.reserve(tmpBuffers.size());
    for (const auto& tmpBuffer : tmpBuffers) {
        buffers.emplace_back(columnLayout, tmpBuffer, NES::Runtime::MemoryLayouts::CompressionMode::VERTICAL);
    }
    uint8_t* origBuffers[10];// TODO dynamic (numberOfBuffersToProduce)
    for (size_t i = 0; i < tmpBuffers.size(); i++) {
        auto* bufferOrig = (uint8_t*) malloc(bufferSizeInBytes);
        memcpy(bufferOrig, tmpBuffers[i].getBuffer(), bufferSizeInBytes);
        origBuffers[i] = bufferOrig;
    }

    std::cout << "Algo\tRatio\tSpeed" << std::endl;
    // compress and verify
    compress(buffers, NES::Runtime::MemoryLayouts::CompressionAlgorithm::LZ4);
    decompressAndVerify(origBuffers, buffers, bufferSizeInBytes);

    compress(buffers, NES::Runtime::MemoryLayouts::CompressionAlgorithm::SNAPPY);
    decompressAndVerify(origBuffers, buffers, bufferSizeInBytes);

    compress(buffers, NES::Runtime::MemoryLayouts::CompressionAlgorithm::FSST);
    decompressAndVerify(origBuffers, buffers, bufferSizeInBytes);

    compress(buffers, NES::Runtime::MemoryLayouts::CompressionAlgorithm::RLE);
    decompressAndVerify(origBuffers, buffers, bufferSizeInBytes);
}

int main() {
    rowLayoutHorizontalYsb();
    //columnLayoutHorizontalYsb();
    //columnLayoutVerticalYsb();
    return 0;
}