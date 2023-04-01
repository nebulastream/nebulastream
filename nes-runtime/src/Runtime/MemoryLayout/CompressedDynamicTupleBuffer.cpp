#include "Runtime/MemoryLayout/CompressedDynamicTupleBuffer.hpp"
#include "Runtime/MemoryLayout/RowLayout.hpp"
#include "Runtime/MemoryLayout/ColumnLayout.hpp"
#include "API/Schema.hpp"
//#include "../nes-data-types/include/API/Schema.hpp"
#include "Util/Logger/Logger.hpp"
#include <lz4.h>
#include <utility>

namespace NES::Runtime::MemoryLayouts {

CompressedDynamicTupleBuffer::CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout, TupleBuffer buffer)
    : DynamicTupleBuffer(memoryLayout, std::move(buffer)) {
    compressionAlgorithm = CompressionAlgorithm::NONE;
    compressionMode = CompressionMode::FULL_BUFFER;
    compressed = false;
    lz4CompressedSizes = std::vector<int>{0};
    offsets = getOffsets(memoryLayout);
}

CompressedDynamicTupleBuffer::CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout,
                                                           TupleBuffer buffer,
                                                           CompressionAlgorithm compressionAlgorithm,
                                                           CompressionMode compressionMode)
    : DynamicTupleBuffer(memoryLayout, std::move(buffer)) {
    this->compressionAlgorithm = compressionAlgorithm;
    this->compressionMode = compressionMode;
    compressed = false;
    lz4CompressedSizes = std::vector<int>{0};
    offsets = getOffsets(memoryLayout);
}

std::vector<uint64_t> CompressedDynamicTupleBuffer::getOffsets(const MemoryLayoutPtr& memoryLayout) {
    if (auto rowLayout = dynamic_cast<RowLayout*>(memoryLayout.get())) {
        //offsets = rowLayout->getFieldOffSets();
        return offsets = {0};
    } else if (auto columnLayout = dynamic_cast<ColumnLayout*>(memoryLayout.get())) {
        return offsets = columnLayout->getColumnOffsets();
    }
    NES_NOT_IMPLEMENTED();
}

// ====================================================================================================
// Compressor
// ====================================================================================================
void Compressor::compress(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf) {
    switch (outBuf.compressionMode) {
        case CompressionMode::FULL_BUFFER:
            switch (outBuf.compressionAlgorithm) {
                case CompressionAlgorithm::NONE: NES_NOT_IMPLEMENTED();
                case CompressionAlgorithm::LZ4: Compressor::compressLz4(inBuf, outBuf); break;
                case CompressionAlgorithm::RLE: Compressor::compressRLE(inBuf, outBuf); break;
                case CompressionAlgorithm::SNAPPY: NES_NOT_IMPLEMENTED();
            }
            break;
        case CompressionMode::COLUMN_WISE:
            if (dynamic_cast<ColumnLayout*>(outBuf.getMemoryLayout().get()) == nullptr) {
                NES_THROW_RUNTIME_ERROR("Only ColumnLayout supported for row-wise compression.");
            }
            switch (outBuf.compressionAlgorithm) {
                case CompressionAlgorithm::NONE: NES_NOT_IMPLEMENTED();
                case CompressionAlgorithm::LZ4: Compressor::compressLz4Columnar(inBuf, outBuf); break;
                case CompressionAlgorithm::RLE: NES_NOT_IMPLEMENTED();
                case CompressionAlgorithm::SNAPPY: NES_NOT_IMPLEMENTED();
            }
            break;
        default: NES_NOT_IMPLEMENTED();
    }
}

void Compressor::compressLz4(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf) {
    uint8_t* baseSrcPointer = inBuf.getBuffer().getBuffer();
    uint8_t* baseDstPointer = outBuf.getBuffer().getBuffer();
    std::vector<int> compressedSizes;
    for (auto offset : inBuf.offsets) {
        const char* src = reinterpret_cast<const char*>(baseSrcPointer + offset);
        const int srcSize = strlen(src);
        const int maxDstSize = LZ4_compressBound(srcSize);// TODO handle if > buffer size
        char* compressed = (char*) malloc((size_t) maxDstSize);
        if (compressed == nullptr)
            NES_NOT_IMPLEMENTED();// TODO
        const int compressedSize = LZ4_compress_default(src, compressed, srcSize, outBuf.getCapacity());
        if (compressedSize <= 0)
            NES_NOT_IMPLEMENTED();// TODO
        compressedSizes.push_back(compressedSize);
        // free up memory
        compressed = (char*) realloc(compressed, (size_t) compressedSize);// TODO? always same as buffer size
        memcpy(baseDstPointer + offset, compressed, compressedSize);
    }
    outBuf.lz4CompressedSizes = compressedSizes;
    outBuf.compressed = true;
}

void Compressor::compressLz4Columnar(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf) {
    uint8_t* baseSrcPointer = inBuf.getBuffer().getBuffer();
    uint8_t* baseDstPointer = outBuf.getBuffer().getBuffer();
    int bufferSize = inBuf.getBuffer().getBufferSize();
    char* src = new char[bufferSize];
    std::vector<uint64_t> newOffsets{0};
    // create one string to be compressed
    strcpy(src, reinterpret_cast<const char*>(baseSrcPointer));
    size_t newOffset = 0;
    for (size_t i = 1; i < inBuf.offsets.size(); i++) {
        const char* tmp = reinterpret_cast<const char*>(baseSrcPointer + inBuf.offsets[i]);
        strcat(src, tmp);
        newOffset += strlen(tmp);
        newOffsets.push_back(newOffset);
    }
    int srcSize = strlen(src);
    const int maxDstSize = LZ4_compressBound(srcSize);// TODO handle if > buffer size
    char* compressed = (char*) malloc((size_t) maxDstSize);
    if (compressed == nullptr)
        NES_NOT_IMPLEMENTED();// TODO
    const int compressedSize = LZ4_compress_default(src, compressed, srcSize, maxDstSize);
    if (compressedSize <= 0)
        NES_NOT_IMPLEMENTED();// TODO
    // free up memory
    compressed = (char*) realloc(compressed, (size_t) compressedSize);// TODO? always same as buffer size
    memcpy(baseDstPointer, compressed, compressedSize);
    outBuf.lz4CompressedSizes = {compressedSize};
    outBuf.compressed = true;
}

void Compressor::compressRLE(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf) {
    uint8_t* baseSrcPointer = inBuf.getBuffer().getBuffer();
    uint8_t* baseDstPointer = outBuf.getBuffer().getBuffer();
    for (auto offset : inBuf.offsets) {
        const char* in = reinterpret_cast<const char*>(baseSrcPointer + offset);
        size_t size = strlen(in);
        std::string temp;
        for (size_t i = 0; i < size; i++) {
            int count = 1;
            while (in[i] == in[i + 1]) {
                count++;
                i++;
            }
            if (count <= 1) {
                temp += in[i];
            } else {
                temp += std::to_string(count);
                temp += in[i];
            }
        }
        memcpy(baseDstPointer + offset, temp.c_str(), temp.size());// TODO handle case if buffer is too small
    }
    outBuf.compressed = true;
}

// ====================================================================================================
// Decompressor
// ====================================================================================================
void Decompressor::decompress(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf) {
    switch (outBuf.compressionMode) {
        case CompressionMode::FULL_BUFFER:
            switch (inBuf.compressionAlgorithm) {
                case CompressionAlgorithm::NONE: NES_NOT_IMPLEMENTED();
                case CompressionAlgorithm::LZ4: decompressLz4(inBuf, outBuf); break;
                case CompressionAlgorithm::RLE: decompressRLE(inBuf, outBuf); break;
                case CompressionAlgorithm::SNAPPY: NES_NOT_IMPLEMENTED();
            }
            break;
        case CompressionMode::COLUMN_WISE:
            if (dynamic_cast<ColumnLayout*>(outBuf.getMemoryLayout().get()) == nullptr) {
                NES_THROW_RUNTIME_ERROR("Only ColumnLayout supported for row-wise compression.");
            }
            switch (inBuf.compressionAlgorithm) {
                case CompressionAlgorithm::NONE: NES_NOT_IMPLEMENTED();
                case CompressionAlgorithm::LZ4: decompressLz4Columnar(inBuf, outBuf); break;
                case CompressionAlgorithm::RLE: NES_NOT_IMPLEMENTED();
                case CompressionAlgorithm::SNAPPY: NES_NOT_IMPLEMENTED();
            }
            break;
        default: NES_NOT_IMPLEMENTED();
    }
}
void Decompressor::decompressLz4(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf) {
    uint8_t* baseSrcPointer = inBuf.getBuffer().getBuffer();
    uint8_t* baseDstPointer = outBuf.getBuffer().getBuffer();
    int i = 0;
    for (auto offset : inBuf.offsets) {
        const char* compressed = reinterpret_cast<const char*>(baseSrcPointer + offset);
        const int compressedSize = inBuf.lz4CompressedSizes[i];
        const int dstCapacity = 3 * compressedSize;// TODO
        char* const decompressed = (char*) malloc(dstCapacity);
        if (decompressed == nullptr)
            NES_THROW_RUNTIME_ERROR("Invalid destination pointer.");// TODO
        const int decompressedSize = LZ4_decompress_safe(compressed, decompressed, compressedSize, dstCapacity);
        if (decompressedSize < 0)
            NES_THROW_RUNTIME_ERROR("LZ4 decompression failed.");
        memcpy(baseDstPointer + offset, decompressed, decompressedSize);
        i++;
    }
    outBuf.compressed = false;
}

void Decompressor::decompressLz4Columnar(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf) {
    uint8_t* baseSrcPointer = inBuf.getBuffer().getBuffer();
    uint8_t* baseDstPointer = outBuf.getBuffer().getBuffer();
    const char* compressed = reinterpret_cast<const char*>(baseSrcPointer);
    const int compressedSize = inBuf.lz4CompressedSizes[0];
    const int dstCapacity = 3 * compressedSize;// TODO
    char* const decompressed = (char*) malloc(dstCapacity);
    if (decompressed == nullptr)
        NES_THROW_RUNTIME_ERROR("Invalid destination pointer.");// TODO
    const int decompressedSize = LZ4_decompress_safe(compressed, decompressed, compressedSize, dstCapacity);
    if (decompressedSize < 0)
        NES_THROW_RUNTIME_ERROR("LZ4 decompression failed.");
    memcpy(baseDstPointer, decompressed, decompressedSize);
    auto columnLayout = dynamic_cast<ColumnLayout*>(inBuf.getMemoryLayout().get());
    if (columnLayout == nullptr) {
        NES_THROW_RUNTIME_ERROR("Invalid MemoryLayout.");
    }
    size_t numCols = columnLayout->getSchema()->getSize();
    size_t numTuples = inBuf.getNumberOfTuples();
    // insert values
    int decoIdx = 0;
    for (size_t col = 0; col < numCols; col++) {
        for (size_t row = 0; row < numTuples; row++) {
            outBuf[row][col].write<uint8_t>(decompressed[decoIdx]);
            decoIdx++;
        }
    }
    outBuf.setNumberOfTuples(numTuples);
    std::vector<uint64_t> newOffsets{0};
    for (uint64_t i = 1; i < numCols; i++) {
        newOffsets.push_back(numTuples * i);
    }
    outBuf.offsets = newOffsets;
    outBuf.compressed = false;
}

static bool alphaOrSpace(const char c) { return isalpha(c) || c == ' '; }

void Decompressor::decompressRLE(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf) {
    uint8_t* baseSrcPointer = inBuf.getBuffer().getBuffer();
    uint8_t* baseDstPointer = outBuf.getBuffer().getBuffer();
    for (auto offset : inBuf.offsets) {
        const char* in = reinterpret_cast<const char*>(baseSrcPointer + offset);
        size_t size = strlen(in);
        std::string temp;
        size_t i = 0;
        size_t repeat;
        while (i < size) {
            // normal alpha characters
            while (alphaOrSpace(in[i]))
                temp.push_back(in[i++]);

            // repeat number
            repeat = 0;
            while (isdigit(in[i]))
                repeat = 10 * repeat + (in[i++] - '0');

            // unroll related characters
            auto char_to_unroll = in[i++];
            while (repeat--)
                temp.push_back(char_to_unroll);
        }
        memcpy(baseDstPointer + offset, temp.c_str(), temp.size());// TODO handle case if buffer is too small
    }
    outBuf.compressed = false;
}

}// namespace NES::Runtime::MemoryLayouts
