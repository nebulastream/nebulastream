#include "Runtime/MemoryLayout/CompressedDynamicTupleBuffer.hpp"
#include <lz4.h>
#include <utility>

namespace NES::Runtime::MemoryLayouts {

CompressedDynamicTupleBuffer::CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout, TupleBuffer buffer)
    : DynamicTupleBuffer(memoryLayout, std::move(buffer)) {
    compressionAlgorithm = CompressionAlgorithm::NONE;
    compressed = false;
    lz4CompressedSizes = std::vector<int>{0};
    offsets = getOffsets(memoryLayout);
}

CompressedDynamicTupleBuffer::CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout,
                                                           TupleBuffer buffer,
                                                           CompressionAlgorithm compressionAlgorithm)
    : DynamicTupleBuffer(memoryLayout, std::move(buffer)) {
    this->compressionAlgorithm = compressionAlgorithm;
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
    switch (outBuf.compressionAlgorithm) {
        case CompressionAlgorithm::NONE: NES_NOT_IMPLEMENTED();
        case CompressionAlgorithm::LZ4: Compressor::compressLz4(inBuf, outBuf); break;
        case CompressionAlgorithm::RLE: Compressor::compressRLE(inBuf, outBuf); break;
        case CompressionAlgorithm::SNAPPY: NES_NOT_IMPLEMENTED();
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
    switch (inBuf.compressionAlgorithm) {
        case CompressionAlgorithm::NONE: NES_NOT_IMPLEMENTED();
        case CompressionAlgorithm::LZ4: decompressLz4(inBuf, outBuf); break;
        case CompressionAlgorithm::RLE: decompressRLE(inBuf, outBuf); break;
        case CompressionAlgorithm::SNAPPY: NES_NOT_IMPLEMENTED();
    }
}
void Decompressor::decompressLz4(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf) {
    uint8_t* baseSrcPointer = inBuf.getBuffer().getBuffer();
    uint8_t* baseDstPointer = outBuf.getBuffer().getBuffer();
    int i = 0;
    for (auto offset : inBuf.offsets) {
        const char* compressed = reinterpret_cast<const char*>(baseSrcPointer + offset);
        const int compressedSize = inBuf.lz4CompressedSizes[i];
        char* const decompressed = (char*) malloc(compressedSize * 2);
        if (decompressed == nullptr)
            NES_NOT_IMPLEMENTED();// TODO
        const int decompressedSize = LZ4_decompress_safe(compressed, decompressed, compressedSize, outBuf.getCapacity());
        if (decompressedSize < 0)
            NES_NOT_IMPLEMENTED();// TODO
        memcpy(baseDstPointer + offset, decompressed, decompressedSize);
        i++;
    }
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
