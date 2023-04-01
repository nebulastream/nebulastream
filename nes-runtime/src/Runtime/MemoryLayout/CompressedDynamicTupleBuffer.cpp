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
    lz4CompressedSizes = std::vector<int>{0};
    offsets = getOffsets(memoryLayout);
}

CompressedDynamicTupleBuffer::CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout,
                                                           TupleBuffer buffer,
                                                           CompressionMode cm)
    : DynamicTupleBuffer(memoryLayout, std::move(buffer)) {
    maxBufferSize = this->getBuffer().getBufferSize();
    this->compressionAlgorithm = CompressionAlgorithm::NONE;
    this->compressionMode = cm;
    lz4CompressedSizes = std::vector<int>{0};
    offsets = getOffsets(memoryLayout);
}

CompressedDynamicTupleBuffer::CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout,
                                                           TupleBuffer buffer,
                                                           CompressionAlgorithm ca,
                                                           CompressionMode cm)
    : DynamicTupleBuffer(memoryLayout, std::move(buffer)) {
    maxBufferSize = this->getBuffer().getBufferSize();
    this->compressionAlgorithm = ca;
    // TODO compress
    this->compressionMode = cm;
    lz4CompressedSizes = std::vector<int>{0};
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
void CompressedDynamicTupleBuffer::compress(CompressionAlgorithm targetCa) { compress(targetCa, CompressionMode::FULL_BUFFER); }
void CompressedDynamicTupleBuffer::compress(CompressionAlgorithm targetCa, CompressionMode targetCm) {
    if (compressionAlgorithm == CompressionAlgorithm::NONE) {
        switch (targetCa) {
            case CompressionAlgorithm::NONE: decompress(); break;
            case CompressionAlgorithm::LZ4:
                switch (targetCm) {
                    case CompressionMode::FULL_BUFFER: compressLz4FullBuffer(); break;
                    case CompressionMode::COLUMN_WISE: NES_NOT_IMPLEMENTED();
                }
                break;
            case CompressionAlgorithm::SNAPPY: NES_NOT_IMPLEMENTED();
            case CompressionAlgorithm::RLE: NES_NOT_IMPLEMENTED();
        }
    } else {
        NES_THROW_RUNTIME_ERROR(printf("Cannot compress %s to %s.",
                                       getCompressionAlgorithmName(compressionAlgorithm),
                                       getCompressionAlgorithmName(targetCa)));
    }
}

void CompressedDynamicTupleBuffer::decompress() {
    switch (compressionAlgorithm) {
        case CompressionAlgorithm::NONE: break;
        case CompressionAlgorithm::LZ4: decompressLz4FullBuffer(); break;
        case CompressionAlgorithm::SNAPPY: NES_NOT_IMPLEMENTED();
        case CompressionAlgorithm::RLE: NES_NOT_IMPLEMENTED();
    }
}

void CompressedDynamicTupleBuffer::compressLz4FullBuffer() {//TODO WIP
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) malloc(this->getBuffer().getBufferSize());// TODO? new TupleBuffer instead?
    size_t dstLength = 0;
    std::vector<int> compressedSizes;
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
        compressed = (char*) realloc(compressed, (size_t) compressedSize);// TODO? always same as buffer size
        dstLength += compressedSize;
        memcpy(baseDstPointer + offset, compressed, compressedSize);
    }
    memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    // TODO reduce buffer to `dstLength`
    if (dstLength > maxBufferSize)
        maxBufferSize = dstLength;
    this->lz4CompressedSizes = compressedSizes;
    this->compressionAlgorithm = CompressionAlgorithm::LZ4;
}

void CompressedDynamicTupleBuffer::decompressLz4FullBuffer() {//TODO WIP
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
    // TODO could overwrite allocated boundaries
    memcpy(baseSrcPointer, baseDstPointer, maxBufferSize);
    // TODO adjust buffer to `dstLength`
    this->lz4CompressedSizes = {0};
    this->compressionAlgorithm = CompressionAlgorithm::NONE;
}

}// namespace NES::Runtime::MemoryLayouts
