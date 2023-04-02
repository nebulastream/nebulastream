#include "Runtime/MemoryLayout/CompressedDynamicTupleBuffer.hpp"
#include "API/Schema.hpp"
#include "Runtime/MemoryLayout/ColumnLayout.hpp"
#include "Runtime/MemoryLayout/RowLayout.hpp"
#include "Util/Logger/Logger.hpp"
#include <lz4.h>
#include <utility>

namespace NES::Runtime::MemoryLayouts {

CompressedDynamicTupleBuffer::CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout, TupleBuffer buffer)
    : DynamicTupleBuffer(memoryLayout, std::move(buffer)) {
    maxBufferSize = this->getBuffer().getBufferSize();
    compressionAlgorithm = CompressionAlgorithm::NONE;
    compressionMode = CompressionMode::FULL_BUFFER;
    lz4CompressedSizes = std::vector<size_t>{0};
    offsets = getOffsets(memoryLayout);
}

CompressedDynamicTupleBuffer::CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout,
                                                           TupleBuffer buffer,
                                                           CompressionMode cm)
    : DynamicTupleBuffer(memoryLayout, std::move(buffer)) {
    maxBufferSize = this->getBuffer().getBufferSize();
    this->compressionAlgorithm = CompressionAlgorithm::NONE;
    this->compressionMode = cm;
    lz4CompressedSizes = std::vector<size_t>{0};
    offsets = getOffsets(memoryLayout);
}

CompressedDynamicTupleBuffer::CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout,
                                                           TupleBuffer buffer,
                                                           CompressionAlgorithm ca,
                                                           CompressionMode cm)
    : DynamicTupleBuffer(memoryLayout, std::move(buffer)) {
    NES_NOT_IMPLEMENTED();
    maxBufferSize = this->getBuffer().getBufferSize();
    this->compressionAlgorithm = ca;
    // TODO compress
    this->compressionMode = cm;
    lz4CompressedSizes = std::vector<size_t>{0};
    offsets = getOffsets(memoryLayout);
}

CompressionAlgorithm CompressedDynamicTupleBuffer::getCompressionAlgorithm() { return compressionAlgorithm; }
CompressionMode CompressedDynamicTupleBuffer::getCompressionMode() { return compressionMode; }

std::vector<uint64_t> CompressedDynamicTupleBuffer::getOffsets(const MemoryLayoutPtr& memoryLayout) {
    if (auto rowLayout = dynamic_cast<RowLayout*>(memoryLayout.get())) {
        //offsets = rowLayout->getFieldOffSets();
        return offsets = {0};
    } else if (auto columnLayout = dynamic_cast<ColumnLayout*>(memoryLayout.get())) {
        return offsets = columnLayout->getColumnOffsets();
    }
    NES_NOT_IMPLEMENTED();
}

void CompressedDynamicTupleBuffer::clearBuffer() {
    auto dynTupleBuffer = *this;
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    for (size_t i = 0; i < this->getBuffer().getBufferSize(); i++) {
        baseSrcPointer[i] = '\0';
    }
}

void CompressedDynamicTupleBuffer::compress(CompressionAlgorithm targetCa) { compress(targetCa, this->compressionMode); }
void CompressedDynamicTupleBuffer::compress(CompressionAlgorithm targetCa, CompressionMode targetCm) {
    if (compressionAlgorithm == CompressionAlgorithm::NONE) {
        switch (targetCa) {
            case CompressionAlgorithm::NONE: decompress(); break;
            case CompressionAlgorithm::LZ4:
                switch (targetCm) {
                    case CompressionMode::FULL_BUFFER: compressLz4FullBuffer(); break;
                    case CompressionMode::COLUMN_WISE: {
                        auto rowLayout = dynamic_cast<RowLayout*>(this->getMemoryLayout().get());
                        if (rowLayout != nullptr) {
                            NES_THROW_RUNTIME_ERROR("Column-wise compression cannot be performed on row layout.");
                        }
                        compressLz4ColumnWise();
                        break;
                    }
                }
                break;
            case CompressionAlgorithm::SNAPPY: NES_NOT_IMPLEMENTED();
            case CompressionAlgorithm::RLE: NES_NOT_IMPLEMENTED();
        }
    } else {
        NES_THROW_RUNTIME_ERROR(printf("Cannot compress from %s to %s.",
                                       getCompressionAlgorithmName(compressionAlgorithm),
                                       getCompressionAlgorithmName(targetCa)));
    }
}

void CompressedDynamicTupleBuffer::decompress() {
    switch (compressionAlgorithm) {
        case CompressionAlgorithm::NONE: break;
        case CompressionAlgorithm::LZ4:
            switch (this->compressionMode) {
                case CompressionMode::FULL_BUFFER: decompressLz4FullBuffer(); break;
                case CompressionMode::COLUMN_WISE: {
                    auto rowLayout = dynamic_cast<RowLayout*>(this->getMemoryLayout().get());
                    if (rowLayout != nullptr) {
                        NES_THROW_RUNTIME_ERROR("Column-wise decompression cannot be performed on row layout.");
                    }
                    decompressLz4ColumnWise();
                    break;
                }
            }
            break;
        case CompressionAlgorithm::SNAPPY: NES_NOT_IMPLEMENTED();
        case CompressionAlgorithm::RLE: NES_NOT_IMPLEMENTED();
    }
}

void CompressedDynamicTupleBuffer::compressLz4FullBuffer() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) malloc(this->getBuffer().getBufferSize());// TODO? new TupleBuffer instead?
    size_t dstLength = 0;
    std::vector<size_t> compressedSizes;
    for (auto offset : this->offsets) {
        const char* src = reinterpret_cast<const char*>(baseSrcPointer + offset);
        const int srcSize = strlen(src);
        const int maxDstSize = LZ4_compressBound(srcSize);// TODO handle if > buffer size
        char* compressed = (char*) malloc((size_t) maxDstSize);
        if (compressed == nullptr)
            NES_THROW_RUNTIME_ERROR("Invalid destination pointer.");
        const int compressedSize = LZ4_compress_default(src, compressed, srcSize, maxDstSize);
        if (compressedSize <= 0)
            NES_THROW_RUNTIME_ERROR("LZ4 compression failed.");
        compressedSizes.push_back(compressedSize);
        // free up memory
        compressed = (char*) realloc(compressed, (size_t) compressedSize);
        dstLength += compressedSize;
        memcpy(baseDstPointer + offset, compressed, compressedSize);
    }
    clearBuffer();
    memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    // TODO reduce buffer to `dstLength`
    if (dstLength > maxBufferSize)
        maxBufferSize = dstLength;
    this->lz4CompressedSizes = compressedSizes;
    this->compressionAlgorithm = CompressionAlgorithm::LZ4;
}

void CompressedDynamicTupleBuffer::decompressLz4FullBuffer() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) malloc(maxBufferSize);
    int i = 0;
    size_t dstLength = 0;
    for (auto offset : this->offsets) {
        const char* compressed = reinterpret_cast<const char*>(baseSrcPointer + offset);
        const int compressedSize = this->lz4CompressedSizes[i];
        const int dstCapacity = 3 * compressedSize;// TODO magic number
        char* const decompressed = (char*) malloc(dstCapacity);
        if (decompressed == nullptr)
            NES_THROW_RUNTIME_ERROR("Invalid destination pointer.");
        const int decompressedSize = LZ4_decompress_safe(compressed, decompressed, compressedSize, dstCapacity);
        if (decompressedSize < 0)
            NES_THROW_RUNTIME_ERROR("LZ4 decompression failed.");
        memcpy(baseDstPointer + offset, decompressed, decompressedSize);
        dstLength += decompressedSize;
        i++;
    }
    if (dstLength > maxBufferSize)
        maxBufferSize = dstLength;
    clearBuffer();
    // TODO could overwrite allocated boundaries
    memcpy(baseSrcPointer, baseDstPointer, maxBufferSize);
    // TODO adjust buffer to `dstLength`
    this->lz4CompressedSizes = {0};
    this->compressionAlgorithm = CompressionAlgorithm::NONE;
}

void CompressedDynamicTupleBuffer::compressLz4ColumnWise() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    size_t bufferSize = this->getBuffer().getBufferSize();
    char* src = new char[bufferSize];
    std::vector<uint64_t> newOffsets{0};
    // create one string to be compressed (increases chances of better compression)
    strcpy(src, reinterpret_cast<const char*>(baseSrcPointer));
    size_t newOffset = 0;
    for (size_t i = 1; i < this->offsets.size(); i++) {
        const char* tmp = reinterpret_cast<const char*>(baseSrcPointer + this->offsets[i]);
        strcat(src, tmp);
        newOffset += strlen(tmp);
        newOffsets.push_back(newOffset);
    }
    int srcSize = strlen(src);
    const int maxDstSize = LZ4_compressBound(srcSize);// TODO handle if > buffer size
    char* compressed = (char*) malloc((size_t) maxDstSize);
    if (compressed == nullptr)
        NES_THROW_RUNTIME_ERROR("Invalid destination pointer.");
    const size_t compressedSize = LZ4_compress_default(src, compressed, srcSize, maxDstSize);
    if (compressedSize <= 0)
        NES_THROW_RUNTIME_ERROR("LZ4 decompression failed.");
    // free up memory
    if (compressedSize > maxBufferSize)
        maxBufferSize = compressedSize;
    compressed = (char*) realloc(compressed, (size_t) compressedSize);
    // TODO could overwrite allocated boundaries
    clearBuffer();
    memcpy(baseSrcPointer, compressed, compressedSize);
    this->lz4CompressedSizes = {compressedSize};
    this->compressionAlgorithm = CompressionAlgorithm::LZ4;
}

void CompressedDynamicTupleBuffer::decompressLz4ColumnWise() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    const char* compressed = reinterpret_cast<const char*>(baseSrcPointer);
    const size_t compressedSize = this->lz4CompressedSizes[0];
    const size_t dstCapacity = 3 * compressedSize;// TODO magic number
    char* const decompressed = (char*) malloc(dstCapacity);
    if (decompressed == nullptr)
        NES_THROW_RUNTIME_ERROR("Invalid destination pointer.");// TODO
    const int decompressedSize = LZ4_decompress_safe(compressed, decompressed, compressedSize, dstCapacity);
    if (decompressedSize < 0)
        NES_THROW_RUNTIME_ERROR("LZ4 decompression failed.");

    // insert values
    clearBuffer();
    size_t numCols = this->getMemoryLayout().get()->getSchema()->getSize();
    size_t numTuples = this->getNumberOfTuples();
    auto dynTupleBuffer = *this;
    int decoIdx = 0;
    for (size_t col = 0; col < numCols; col++) {
        for (size_t row = 0; row < numTuples; row++) {
            dynTupleBuffer[row][col].write<uint8_t>(decompressed[decoIdx]);
            decoIdx++;
        }
    }
    dynTupleBuffer.setNumberOfTuples(numTuples);
    std::vector<uint64_t> newOffsets{0};
    for (uint64_t i = 1; i < numCols; i++) {
        newOffsets.push_back(numTuples * i);
    }
    this->lz4CompressedSizes = {0};
    this->offsets = newOffsets;
    this->compressionAlgorithm = CompressionAlgorithm::NONE;
}

}// namespace NES::Runtime::MemoryLayouts
