#include "Runtime/MemoryLayout/CompressedDynamicTupleBuffer.hpp"
#include "API/Schema.hpp"
#include "Runtime/MemoryLayout/ColumnLayout.hpp"
#include "Runtime/MemoryLayout/Compression/Sprintz/sprintz.h"
#include "Runtime/MemoryLayout/Compression/brle.h"
#include "Runtime/MemoryLayout/Compression/fsst.h"
#include "Runtime/MemoryLayout/Compression/libfsst.hpp"
#include "Runtime/MemoryLayout/RowLayout.hpp"
#include "Util/Logger/Logger.hpp"
#include <lz4.h>
#include <snappy.h>
#include <utility>

namespace NES::Runtime::MemoryLayouts {

CompressedDynamicTupleBuffer::CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout, TupleBuffer buffer)
    : DynamicTupleBuffer(memoryLayout, std::move(buffer)) {
    compressionAlgorithm = CompressionAlgorithm::NONE;
    compressedSizes = std::vector<size_t>{};
    totalOriginalSize = this->getMemoryLayout()->getTupleSize() * this->getNumberOfTuples();
    offsets = getOffsets(memoryLayout);
    // set default compression mode
    auto rowLayout = dynamic_cast<RowLayout*>(this->getMemoryLayout().get());
    if (rowLayout != nullptr) {
        compressionMode = CompressionMode::HORIZONTAL;
    }
    auto columnLayout = dynamic_cast<ColumnLayout*>(this->getMemoryLayout().get());
    if (columnLayout != nullptr) {
        compressionMode = CompressionMode::VERTICAL;
    }
}

CompressedDynamicTupleBuffer::CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout,
                                                           TupleBuffer buffer,
                                                           CompressionMode cm)
    : DynamicTupleBuffer(memoryLayout, std::move(buffer)) {
    compressionAlgorithm = CompressionAlgorithm::NONE;
    compressionMode = cm;
    compressedSizes = std::vector<size_t>{};
    totalOriginalSize = this->getMemoryLayout()->getTupleSize() * this->getNumberOfTuples();
    offsets = getOffsets(memoryLayout);
}

CompressedDynamicTupleBuffer::CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout,
                                                           TupleBuffer buffer,
                                                           [[maybe_unused]] CompressionAlgorithm ca,
                                                           [[maybe_unused]] CompressionMode cm)
    : DynamicTupleBuffer(memoryLayout, std::move(buffer)) {
    NES_NOT_IMPLEMENTED();
    // TODO compress
}

void CompressedDynamicTupleBuffer::setNumberOfTuples(uint64_t value) {
    DynamicTupleBuffer::setNumberOfTuples(value);
    totalOriginalSize = this->getMemoryLayout()->getTupleSize() * this->getNumberOfTuples();
}

CompressionAlgorithm CompressedDynamicTupleBuffer::getCompressionAlgorithm() { return compressionAlgorithm; }
CompressionMode CompressedDynamicTupleBuffer::getCompressionMode() { return compressionMode; }
std::vector<uint64_t> CompressedDynamicTupleBuffer::getOffsets() { return offsets; }
std::vector<size_t> CompressedDynamicTupleBuffer::getCompressedSize() { return compressedSizes; }
double CompressedDynamicTupleBuffer::getCompressionRatio() {
    size_t compressedSize = 0;
    for (const auto& s : compressedSizes)
        compressedSize += s;
    return (double) totalOriginalSize / (double) compressedSize;
}

std::vector<uint64_t> CompressedDynamicTupleBuffer::getOffsets(const MemoryLayoutPtr& memoryLayout) {
    if (auto rowLayout = dynamic_cast<RowLayout*>(memoryLayout.get())) {
        //offsets = rowLayout->getFieldOffSets();
        return {0};
    } else if (auto columnLayout = dynamic_cast<ColumnLayout*>(memoryLayout.get())) {
        return columnLayout->getColumnOffsets();
    }
    NES_NOT_IMPLEMENTED();
}

void CompressedDynamicTupleBuffer::clearBuffer() { clearBuffer(0); }

void CompressedDynamicTupleBuffer::clearBuffer(size_t start) {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    for (size_t i = start; i < this->getBuffer().getBufferSize(); i++) {
        baseSrcPointer[i] = '\0';
    }
}

void CompressedDynamicTupleBuffer::compress(CompressionAlgorithm targetCa) { compress(targetCa, compressionMode); }
void CompressedDynamicTupleBuffer::compress(CompressionAlgorithm targetCa, CompressionMode targetCm) {
    compressionMode = targetCm;
    auto rowLayout = dynamic_cast<RowLayout*>(this->getMemoryLayout().get());
    if (rowLayout != nullptr) {
        switch (targetCm) {
            case CompressionMode::HORIZONTAL: {
                compressHorizontal(targetCa);
                break;
            }
            case CompressionMode::VERTICAL: {
                NES_THROW_RUNTIME_ERROR("Vertical compression cannot be performed on row layout.");
            }
        }
        return;
    }
    auto columnLayout = dynamic_cast<ColumnLayout*>(this->getMemoryLayout().get());
    if (columnLayout != nullptr) {
        switch (targetCm) {
            case CompressionMode::HORIZONTAL: {
                concatColumns();
                compressHorizontal(targetCa);
                break;
            }
            case CompressionMode::VERTICAL: {
                compressVertical(targetCa);
            }
        }
        return;
    }
    NES_THROW_RUNTIME_ERROR("Invalid memory layout.");
}

void CompressedDynamicTupleBuffer::compressHorizontal(CompressionAlgorithm targetCa) {
    if (compressionAlgorithm == CompressionAlgorithm::NONE) {
        switch (targetCa) {
            case CompressionAlgorithm::NONE: break;// TODO
            case CompressionAlgorithm::LZ4: compressLz4Horizontal(); break;
            case CompressionAlgorithm::SNAPPY: compressSnappyHorizontal(); break;
            case CompressionAlgorithm::FSST: compressFsstHorizontal(); break;
            case CompressionAlgorithm::RLE: compressRleHorizontal(); break;
            case CompressionAlgorithm::SPRINTZ: compressSprintzHorizontal(); break;
        }
    } else {
        NES_THROW_RUNTIME_ERROR(printf("Cannot compress from %s to %s.",
                                       getCompressionAlgorithmName(compressionAlgorithm),
                                       getCompressionAlgorithmName(targetCa)));
    }
}

void CompressedDynamicTupleBuffer::compressVertical(CompressionAlgorithm targetCa) {
    if (compressionAlgorithm == CompressionAlgorithm::NONE) {
        switch (targetCa) {
            case CompressionAlgorithm::NONE: break;// TODO
            case CompressionAlgorithm::LZ4: compressLz4Vertical(); break;
            case CompressionAlgorithm::SNAPPY: compressSnappyVertical(); break;
            case CompressionAlgorithm::FSST: compressFsstVertical(); break;
            case CompressionAlgorithm::RLE: compressRleVertical(); break;
            case CompressionAlgorithm::SPRINTZ: compressSprintzVertical(); break;
        }
    } else {
        NES_THROW_RUNTIME_ERROR(printf("Cannot compress from %s to %s.",
                                       getCompressionAlgorithmName(compressionAlgorithm),
                                       getCompressionAlgorithmName(targetCa)));
    }
}

void CompressedDynamicTupleBuffer::decompress() {
    auto rowLayout = dynamic_cast<RowLayout*>(this->getMemoryLayout().get());
    if (rowLayout != nullptr) {
        switch (compressionMode) {
            case CompressionMode::HORIZONTAL: {
                decompressHorizontal();
                break;
            }
            case CompressionMode::VERTICAL: {
                NES_THROW_RUNTIME_ERROR("Vertical decompression cannot be performed on row layout.");
            }
        }
        return;
    }
    auto columnLayout = dynamic_cast<ColumnLayout*>(this->getMemoryLayout().get());
    if (columnLayout != nullptr) {
        switch (compressionMode) {
            case CompressionMode::HORIZONTAL: {
                decompressHorizontal();
                break;
            }
            case CompressionMode::VERTICAL: {
                decompressVertical();
            }
        }
        return;
    }
    NES_THROW_RUNTIME_ERROR("Invalid memory layout.");
}

void CompressedDynamicTupleBuffer::decompressHorizontal() {
    switch (compressionAlgorithm) {
        case CompressionAlgorithm::NONE: break;
        case CompressionAlgorithm::LZ4: decompressLz4Horizontal(); break;
        case CompressionAlgorithm::SNAPPY: decompressSnappyHorizontal(); break;
        case CompressionAlgorithm::FSST: decompressFsstHorizontal(); break;
        case CompressionAlgorithm::RLE: decompressRleHorizontal(); break;
        case CompressionAlgorithm::SPRINTZ: decompressSprintzHorizontal(); break;
    }
}

void CompressedDynamicTupleBuffer::decompressVertical() {
    switch (compressionAlgorithm) {
        case CompressionAlgorithm::NONE: break;
        case CompressionAlgorithm::LZ4: decompressLz4Vertical(); break;
        case CompressionAlgorithm::SNAPPY: decompressSnappyVertical(); break;
        case CompressionAlgorithm::FSST: decompressFsstVertical(); break;
        case CompressionAlgorithm::RLE: decompressRleVertical(); break;
        case CompressionAlgorithm::SPRINTZ: decompressSprintzVertical(); break;
    }
}

void CompressedDynamicTupleBuffer::concatColumns() {
    if (offsets.size() == 1)
        return;
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    std::vector<uint64_t> newOffsets{};
    // create one string to be compressed (could increase chances of better compression)
    size_t newOffset = 0;
    auto types = this->getMemoryLayout()->getPhysicalTypes();
    for (size_t i = 0; i < offsets.size(); i++) {
        size_t typeSize = types[i].get()->size();
        size_t dstSize = typeSize * this->getNumberOfTuples();
        memcpy(baseSrcPointer + newOffset, baseSrcPointer + offsets[i], dstSize);
        newOffsets.push_back(newOffset);
        newOffset += dstSize;
    }
    clearBuffer(newOffset);
    offsets = newOffsets;
}

// ===================================
// LZ4
// ===================================
void CompressedDynamicTupleBuffer::compressLz4Horizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    auto src = reinterpret_cast<const char*>(baseSrcPointer);
    const int dstCapacity = LZ4_compressBound((int) totalOriginalSize);
    char* compressed = (char*) calloc(1, dstCapacity);
    const size_t compressedSize = LZ4_compress_default(src, compressed, (int) totalOriginalSize, dstCapacity);
    if (compressedSize <= 0)
        NES_THROW_RUNTIME_ERROR("LZ4 compression failed.");
    if (compressedSize > totalOriginalSize) {
        // TODO do not compress and return original buffer
        NES_THROW_RUNTIME_ERROR("LZ4 compression failed: compressed size ("
                                << compressedSize << ") is larger than original size (" << totalOriginalSize << ").");
    }
    clearBuffer();
    memcpy(baseSrcPointer, compressed, compressedSize);
    compressedSizes = {compressedSize};
    compressionAlgorithm = CompressionAlgorithm::LZ4;
    free(compressed);
}

void CompressedDynamicTupleBuffer::decompressLz4Horizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    const char* compressed = reinterpret_cast<const char*>(baseSrcPointer);
    const size_t compressedSize = compressedSizes[0];
    char* decompressed = (char*) malloc(this->getBuffer().getBufferSize());
    const int decompressedSize =
        LZ4_decompress_safe(compressed, decompressed, (int) compressedSize, (int) this->getBuffer().getBufferSize());
    if (decompressedSize <= 0)
        NES_THROW_RUNTIME_ERROR("LZ4 decompression failed.");
    if ((size_t) decompressedSize != totalOriginalSize)
        NES_THROW_RUNTIME_ERROR("LZ4 decompression failed: decompressed size (" << decompressedSize << ") != original size ("
                                                                                << totalOriginalSize << ").");
    clearBuffer();
    if (offsets.size() == 1) {
        memcpy(baseSrcPointer, decompressed, decompressedSize);
    } else {
        auto newOffsets = getOffsets(this->getMemoryLayout());
        auto types = this->getMemoryLayout()->getPhysicalTypes();
        for (size_t i = 0; i < offsets.size(); i++) {
            size_t typeSize = types[i].get()->size();
            size_t dstSize = typeSize * this->getNumberOfTuples();
            memcpy(baseSrcPointer + newOffsets[i], decompressed + offsets[i], dstSize);
        }
        offsets = newOffsets;
    }
    compressedSizes = {};
    compressionAlgorithm = CompressionAlgorithm::NONE;
    free(decompressed);
}

void CompressedDynamicTupleBuffer::compressLz4Vertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) malloc(this->getBuffer().getBufferSize());
    auto types = this->getMemoryLayout()->getPhysicalTypes();

    size_t dstLength = 0;
    size_t totalCompressedSize = 0;
    for (size_t i = 0; i < offsets.size(); i++) {
        auto src = reinterpret_cast<const char*>(baseSrcPointer + offsets[i]);
        const size_t srcSize = types[i].get()->size() * this->getNumberOfTuples();
        const int maxDstSize = LZ4_compressBound((int) srcSize);
        char* compressed = (char*) malloc((size_t) maxDstSize);
        const int compressedSize = LZ4_compress_default(src, compressed, (int) srcSize, maxDstSize);
        if (compressedSize <= 0)
            NES_THROW_RUNTIME_ERROR("LZ4 compression failed.");
        totalCompressedSize += compressedSize;
        if (totalCompressedSize > totalOriginalSize) {
            // TODO do not compress and return original buffer
            NES_THROW_RUNTIME_ERROR("LZ4 compression failed: compressed size ("
                                    << totalCompressedSize << ") is larger than original size (" << totalOriginalSize << ").");
        }
        compressedSizes.push_back(compressedSize);
        dstLength += compressedSize;
        memcpy(baseDstPointer + offsets[i], compressed, compressedSize);
        free(compressed);
    }
    clearBuffer();
    memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    compressionAlgorithm = CompressionAlgorithm::LZ4;
    free(baseDstPointer);
}

void CompressedDynamicTupleBuffer::decompressLz4Vertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    auto baseDstPointer = (uint8_t*) calloc(1, this->getBuffer().getBufferSize());

    auto types = this->getMemoryLayout()->getPhysicalTypes();
    for (size_t i = 0; i < offsets.size(); i++) {
        const char* compressed = reinterpret_cast<const char*>(baseSrcPointer + offsets[i]);
        const size_t dstSize = types[i].get()->size() * this->getNumberOfTuples();
        char* decompressed = (char*) malloc(dstSize);
        const int decompressedSize = LZ4_decompress_safe(compressed, decompressed, (int) compressedSizes[i], dstSize);
        if (decompressedSize < 0)
            NES_THROW_RUNTIME_ERROR("LZ4 decompression failed.");
        if ((size_t) decompressedSize != dstSize)
            NES_THROW_RUNTIME_ERROR("LZ4 decompression failed: decompressed size (" << decompressedSize << ") != original size ("
                                                                                    << dstSize << ").");
        memcpy(baseDstPointer + offsets[i], decompressed, decompressedSize);
        free(decompressed);
    }
    clearBuffer();
    memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    compressionAlgorithm = CompressionAlgorithm::NONE;
    compressedSizes = {};
    free(baseDstPointer);
}

// ===================================
// Snappy
// ===================================
void CompressedDynamicTupleBuffer::compressSnappyHorizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    auto src = reinterpret_cast<char*>(baseSrcPointer);
    const size_t dstCapacity = snappy::MaxCompressedLength(totalOriginalSize);
    char* compressed = (char*) malloc(dstCapacity);
    size_t compressedSize;
    snappy::RawCompress(src, totalOriginalSize, compressed, &compressedSize);
    if (compressedSize > totalOriginalSize) {
        // TODO do not compress and return original buffer
        NES_THROW_RUNTIME_ERROR("Snappy compression failed: compressed size ("
                                << compressedSize << ") is larger than original size (" << totalOriginalSize << ").");
    }
    compressedSizes.push_back(compressedSize);
    clearBuffer();
    memcpy(baseSrcPointer, compressed, compressedSize);
    compressionAlgorithm = CompressionAlgorithm::SNAPPY;
    free(compressed);
}

void CompressedDynamicTupleBuffer::decompressSnappyHorizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    const char* compressed = reinterpret_cast<const char*>(baseSrcPointer);
    size_t decompressedSize;
    bool success = snappy::GetUncompressedLength(compressed, compressedSizes[0], &decompressedSize);
    if (!success)
        NES_THROW_RUNTIME_ERROR("Snappy decompression failed: could not get decompressed length.");
    char* decompressed = (char*) malloc(decompressedSize);
    success = snappy::RawUncompress(compressed, compressedSizes[0], decompressed);
    if (!success)
        NES_THROW_RUNTIME_ERROR("Snappy decompression failed.");
    if ((size_t) decompressedSize != totalOriginalSize)
        NES_THROW_RUNTIME_ERROR("Snappy decompression failed: decompressed size (" << decompressedSize << ") != original size ("
                                                                                   << totalOriginalSize << ").");
    clearBuffer();
    if (offsets.size() == 1) {
        memcpy(baseSrcPointer, decompressed, decompressedSize);
    } else {
        auto newOffsets = getOffsets(this->getMemoryLayout());
        auto types = this->getMemoryLayout()->getPhysicalTypes();
        for (size_t i = 0; i < offsets.size(); i++) {
            size_t typeSize = types[i].get()->size();
            size_t dstSize = typeSize * this->getNumberOfTuples();
            memcpy(baseSrcPointer + newOffsets[i], decompressed + offsets[i], dstSize);
        }
        offsets = newOffsets;
    }
    compressedSizes = {};
    compressionAlgorithm = CompressionAlgorithm::NONE;
    free(decompressed);
}

void CompressedDynamicTupleBuffer::compressSnappyVertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    auto* baseDstPointer = (uint8_t*) calloc(1, this->getBuffer().getBufferSize());
    auto types = this->getMemoryLayout()->getPhysicalTypes();

    size_t compressedSize;
    size_t totalCompressedSize = 0;
    for (size_t i = 0; i < offsets.size(); i++) {
        auto src = reinterpret_cast<char*>(baseSrcPointer + offsets[i]);
        const size_t srcSize = types[i].get()->size() * this->getNumberOfTuples();
        const size_t dstCapacity = snappy::MaxCompressedLength(srcSize);
        char* compressed = (char*) malloc(dstCapacity);
        snappy::RawCompress(src, srcSize, compressed, &compressedSize);
        totalCompressedSize += compressedSize;
        if (totalCompressedSize > totalOriginalSize) {
            // TODO do not compress and return original buffer
            NES_THROW_RUNTIME_ERROR("Snappy compression failed: compressed size ("
                                    << totalCompressedSize << ") is larger than original size (" << totalOriginalSize << ").");
        }
        memcpy(baseDstPointer + offsets[i], compressed, compressedSize);
        compressedSizes.push_back(compressedSize);
        free(compressed);
    }
    clearBuffer();
    memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    compressionAlgorithm = CompressionAlgorithm::SNAPPY;
    free(baseDstPointer);
}

void CompressedDynamicTupleBuffer::decompressSnappyVertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    auto* baseDstPointer = (uint8_t*) calloc(1, this->getBuffer().getBufferSize());

    auto types = this->getMemoryLayout()->getPhysicalTypes();
    for (size_t i = 0; i < offsets.size(); i++) {
        const char* compressed = reinterpret_cast<const char*>(baseSrcPointer + offsets[i]);
        size_t decompressedSize;
        bool success = snappy::GetUncompressedLength(compressed, compressedSizes[i], &decompressedSize);
        if (!success)
            NES_THROW_RUNTIME_ERROR("Snappy decompression failed: could not get decompressed length.");
        char* decompressed = (char*) malloc(decompressedSize);
        success = snappy::RawUncompress(compressed, compressedSizes[i], decompressed);
        if (!success)
            NES_THROW_RUNTIME_ERROR("Snappy decompression failed.");
        const size_t dstSize = types[i].get()->size() * this->getNumberOfTuples();
        if (decompressedSize != dstSize)
            NES_THROW_RUNTIME_ERROR("RLE decompression failed: decompressed size (" << decompressedSize << ") != original size ("
                                                                                    << dstSize << ").");
        memcpy(baseDstPointer + offsets[i], decompressed, decompressedSize);
        free(decompressed);
    }
    clearBuffer();
    memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    compressionAlgorithm = CompressionAlgorithm::NONE;
    compressedSizes = {};
    free(baseDstPointer);
}

// ===================================
// FSST
// ===================================
void CompressedDynamicTupleBuffer::compressFsstHorizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) malloc(this->getBuffer().getBufferSize());

    // prepare encoder and input data
    unsigned char* input = baseSrcPointer;
    fsstEncoder = fsst_create(1, &totalOriginalSize, &input, false);

    // prepare compression
    compressedSizes.resize(1);
    fsstOutSize = 7 + 2 * totalOriginalSize;// as specified in fsst.h
    auto outputPtr = (unsigned char*) malloc(fsstOutSize);
    std::string output;
    output.resize(fsstOutSize);

    // compress
    size_t numCompressed = fsst_compress(fsstEncoder,
                                         1,
                                         &totalOriginalSize,
                                         &input,
                                         fsstOutSize,
                                         reinterpret_cast<unsigned char*>(output.data()),
                                         compressedSizes.data(),
                                         &outputPtr);
    if (numCompressed < 1)
        NES_THROW_RUNTIME_ERROR("FSST compression failed.");
    if (compressedSizes[0] > totalOriginalSize) {
        // TODO do not compress and return original buffer
        NES_THROW_RUNTIME_ERROR("FSST compression failed: compressed size ("
                                << compressedSizes[0] << ") is larger than original size (" << totalOriginalSize << ").");
    }
    memcpy(baseDstPointer, outputPtr, compressedSizes[0]);
    clearBuffer();
    std::memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    compressionAlgorithm = CompressionAlgorithm::FSST;
    free(baseDstPointer);
}

void CompressedDynamicTupleBuffer::decompressFsstHorizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) calloc(1, this->getBuffer().getBufferSize());

    // prepare decompression
    fsst_decoder_t decoder = fsst_decoder(fsstEncoder);
    auto* output = (unsigned char*) calloc(1, fsstOutSize);

    // decompress
    const size_t decompressedSize = fsst_decompress(&decoder, compressedSizes[0], baseSrcPointer, fsstOutSize, output);
    if (decompressedSize < 1)
        NES_THROW_RUNTIME_ERROR("FSST decompression failed.");
    if ((size_t) decompressedSize != totalOriginalSize)
        NES_THROW_RUNTIME_ERROR("FSST decompression failed: decompressed size (" << decompressedSize << ") != original size ("
                                                                                 << totalOriginalSize << ").");
    if (offsets.size() == 1) {
        memcpy(baseDstPointer, output, decompressedSize);
    } else {
        auto newOffsets = getOffsets(this->getMemoryLayout());
        auto types = this->getMemoryLayout()->getPhysicalTypes();
        for (size_t i = 0; i < offsets.size(); i++) {
            size_t typeSize = types[i].get()->size();
            size_t dstSize = typeSize * this->getNumberOfTuples();
            memcpy(baseDstPointer + newOffsets[i], output + offsets[i], dstSize);
        }
        offsets = newOffsets;
    }
    clearBuffer();
    std::memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    compressionAlgorithm = CompressionAlgorithm::NONE;
    compressedSizes = {};
    fsstOutSize = 0;
    free(baseDstPointer);
    free(output);
}

void CompressedDynamicTupleBuffer::compressFsstVertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) calloc(1, this->getBuffer().getBufferSize());
    auto types = this->getMemoryLayout()->getPhysicalTypes();

    // prepare encoder and input data
    size_t numStrings = offsets.size();
    std::vector<size_t> srcLengths;
    srcLengths.reserve(numStrings);
    std::vector<unsigned char*> input;
    input.reserve(numStrings);
    for (size_t i = 0; i < offsets.size(); i++) {
        size_t typeSize = types[i].get()->size();
        size_t srcSize = typeSize * this->getNumberOfTuples();
        srcLengths.push_back(srcSize);
        input.push_back(baseSrcPointer + offsets[i]);
    }
    fsstEncoder = fsst_create(numStrings, srcLengths.data(), input.data(), false);

    // prepare compression
    compressedSizes.resize(numStrings);
    size_t tmpFsstOutSize = 0;
    for (size_t len : srcLengths) {
        tmpFsstOutSize += len;
    }
    fsstOutSize = 7 + 2 * tmpFsstOutSize;// as specified in fsst.h
    std::vector<unsigned char*> outputPtr;
    outputPtr.resize(numStrings);
    std::string output;
    output.resize(fsstOutSize);

    // compress
    const size_t numCompressed = fsst_compress(fsstEncoder,
                                               numStrings,
                                               srcLengths.data(),
                                               input.data(),
                                               fsstOutSize,
                                               reinterpret_cast<unsigned char*>(output.data()),
                                               compressedSizes.data(),
                                               outputPtr.data());
    if (numCompressed < 1)
        NES_THROW_RUNTIME_ERROR("FSST compression failed.");
    size_t totalCompressedSize = 0;
    for (auto i : compressedSizes)
        totalCompressedSize += i;
    if (totalCompressedSize > totalOriginalSize) {
        // TODO do not compress and return original buffer
        NES_THROW_RUNTIME_ERROR("FSST compression failed: compressed size ("
                                << totalCompressedSize << ") is larger than original size (" << totalOriginalSize << ").");
    }
    for (size_t i = 0; i < numStrings; i++) {
        memcpy(baseDstPointer + offsets[i], outputPtr[i], compressedSizes[i]);
    }
    clearBuffer();
    std::memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    compressionAlgorithm = CompressionAlgorithm::FSST;
    free(baseDstPointer);
}

void CompressedDynamicTupleBuffer::decompressFsstVertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) calloc(1, this->getBuffer().getBufferSize());

    // prepare decompression
    fsst_decoder_t decoder = fsst_decoder(fsstEncoder);
    const size_t outSize = fsstOutSize / offsets.size();
    auto* output = (unsigned char*) calloc(1, outSize);

    // decompress
    size_t totalDecompressedSize = 0;
    for (size_t i = 0; i < offsets.size(); i++) {
        const size_t decompressedSize =
            fsst_decompress(&decoder, compressedSizes[i], baseSrcPointer + offsets[i], outSize, output);
        if (decompressedSize < 1)
            NES_THROW_RUNTIME_ERROR("FSST decompression failed.");
        totalDecompressedSize += decompressedSize;
        memcpy(baseDstPointer + offsets[i], output, decompressedSize);
    }
    if (totalDecompressedSize != totalOriginalSize)
        NES_THROW_RUNTIME_ERROR("FSST decompression failed: decompressed size ("
                                << totalDecompressedSize << ") != original size (" << totalOriginalSize << ").");
    clearBuffer();
    std::memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    compressionAlgorithm = CompressionAlgorithm::NONE;
    compressedSizes = {};
    fsstOutSize = 0;
    free(baseDstPointer);
    free(output);
}

// ===================================
// RLE
// ===================================
void CompressedDynamicTupleBuffer::compressRleHorizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    const size_t dstSize = std::ceil(1.16 * (double) totalOriginalSize);// as specified in docs
    uint8_t* baseDstPointer = (uint8_t*) calloc(1, dstSize);

    uint8_t* end = pg::brle::encode(baseSrcPointer, baseSrcPointer + totalOriginalSize, baseDstPointer);
    clearBuffer();
    const size_t compressedSize = std::distance(baseDstPointer, end);
    if (compressedSize > totalOriginalSize) {
        // TODO do not compress and return original buffer
        NES_THROW_RUNTIME_ERROR("RLE compression failed: compressed size ("
                                << compressedSize << ") is larger than original size (" << totalOriginalSize << ").");
    }
    compressedSizes.push_back(compressedSize);
    memcpy(baseSrcPointer, baseDstPointer, compressedSize);
    compressionAlgorithm = CompressionAlgorithm::RLE;
    free(baseDstPointer);
}

void CompressedDynamicTupleBuffer::decompressRleHorizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) calloc(1, this->getBuffer().getBufferSize());

    uint8_t* end = pg::brle::decode(baseSrcPointer, baseSrcPointer + compressedSizes[0], baseDstPointer);
    const size_t decompressedSize = std::distance(baseDstPointer, end);
    if (decompressedSize != totalOriginalSize)
        NES_THROW_RUNTIME_ERROR("RLE decompression failed: decompressed size (" << decompressedSize << ") != original size ("
                                                                                << totalOriginalSize << ").");
    clearBuffer();
    if (offsets.size() == 1) {
        memcpy(baseSrcPointer, baseDstPointer, decompressedSize);
    } else {
        auto newOffsets = getOffsets(this->getMemoryLayout());
        auto types = this->getMemoryLayout()->getPhysicalTypes();
        for (size_t i = 0; i < offsets.size(); i++) {
            const size_t dstSize = types[i].get()->size() * this->getNumberOfTuples();
            memcpy(baseSrcPointer + newOffsets[i], baseDstPointer + offsets[i], dstSize);
        }
        offsets = newOffsets;
    }
    compressedSizes = {};
    compressionAlgorithm = CompressionAlgorithm::NONE;
    free(baseDstPointer);
}

void CompressedDynamicTupleBuffer::compressRleVertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) calloc(1, this->getBuffer().getBufferSize());
    size_t totalCompressedSize = 0;
    auto types = this->getMemoryLayout()->getPhysicalTypes();
    for (size_t i = 0; i < offsets.size(); i++) {
        const size_t srcSize = types[i].get()->size() * this->getNumberOfTuples();
        uint8_t* src = baseSrcPointer + offsets[i];
        uint8_t* dst = baseDstPointer + offsets[i];
        uint8_t* end = pg::brle::encode(src, src + srcSize, dst);
        const size_t compressedSize = std::distance(dst, end);
        totalCompressedSize += compressedSize;
        if (totalCompressedSize > totalOriginalSize) {
            // TODO do not compress and return original buffer
            NES_THROW_RUNTIME_ERROR("LZ4 compression failed: compressed size ("
                                    << totalCompressedSize << ") is larger than original size (" << totalOriginalSize << ").");
        }
        compressedSizes.push_back(compressedSize);
    }
    clearBuffer();
    memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    compressionAlgorithm = CompressionAlgorithm::RLE;
    free(baseDstPointer);
}

void CompressedDynamicTupleBuffer::decompressRleVertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) calloc(1, this->getBuffer().getBufferSize());

    auto types = this->getMemoryLayout()->getPhysicalTypes();
    for (size_t i = 0; i < offsets.size(); i++) {
        size_t dstSize = types[i].get()->size() * this->getNumberOfTuples();
        uint8_t* dst = (uint8_t*) calloc(1, dstSize);
        uint8_t* src = baseSrcPointer + offsets[i];
        uint8_t* end = pg::brle::decode(src, src + compressedSizes[i], dst);
        size_t decompressedSize = std::distance(dst, end);
        if (decompressedSize != dstSize)
            NES_THROW_RUNTIME_ERROR("RLE decompression failed: decompressed size (" << decompressedSize << ") != original size ("
                                                                                    << dstSize << ").");
        memcpy(baseDstPointer + offsets[i], dst, decompressedSize);
        free(dst);
    }
    clearBuffer();
    memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    compressedSizes = {};
    compressionAlgorithm = CompressionAlgorithm::NONE;
    free(baseDstPointer);
}

// ===================================
// Sprintz
// ===================================
void CompressedDynamicTupleBuffer::compressSprintzHorizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) calloc(1, 1.2 * totalOriginalSize);// TODO magic number

    int64_t compressedSize = sprintz_compress_delta_8b(baseSrcPointer,
                                                       totalOriginalSize,
                                                       reinterpret_cast<int8_t*>(baseDstPointer),
                                                       this->getMemoryLayout()->getTupleSize());
    if ((size_t) compressedSize > totalOriginalSize) {
        // TODO do not compress and return original buffer
        NES_THROW_RUNTIME_ERROR("Sprintz compression failed: compressed size ("
                                << compressedSize << ") is larger than original size (" << totalOriginalSize << ").");
    }
    clearBuffer();
    compressedSizes.push_back(compressedSize);
    memcpy(baseSrcPointer, baseDstPointer, compressedSize);
    this->compressionAlgorithm = CompressionAlgorithm::SPRINTZ;
    free(baseDstPointer);
}

void CompressedDynamicTupleBuffer::decompressSprintzHorizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) calloc(1, this->getBuffer().getBufferSize());

    const int64_t decompressedSize = sprintz_decompress_delta_8b(reinterpret_cast<const int8_t*>(baseSrcPointer), baseDstPointer);
    if ((size_t) decompressedSize != totalOriginalSize)
        NES_THROW_RUNTIME_ERROR("Sprintz decompression failed: decompressed size (" << decompressedSize << ") != original size ("
                                                                                    << totalOriginalSize << ").");
    clearBuffer();
    if (offsets.size() == 1) {
        memcpy(baseSrcPointer, baseDstPointer, decompressedSize);
    } else {
        auto newOffsets = getOffsets(this->getMemoryLayout());
        auto types = this->getMemoryLayout()->getPhysicalTypes();
        for (size_t i = 0; i < offsets.size(); i++) {
            const size_t dstSize = types[i].get()->size() * this->getNumberOfTuples();
            memcpy(baseSrcPointer + newOffsets[i], baseDstPointer + offsets[i], dstSize);
        }
        offsets = newOffsets;
    }
    compressedSizes = {};
    compressionAlgorithm = CompressionAlgorithm::NONE;
    free(baseDstPointer);
}

void CompressedDynamicTupleBuffer::compressSprintzVertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) calloc(1, this->getBuffer().getBufferSize());

    int64_t compressedSize = sprintz_compress_delta_8b(baseSrcPointer,
                                                       this->getBuffer().getBufferSize(),
                                                       reinterpret_cast<int8_t*>(baseDstPointer),
                                                       1);
    if ((size_t) compressedSize > totalOriginalSize) {
        // TODO do not compress and return original buffer
        NES_THROW_RUNTIME_ERROR("Sprintz compression failed: compressed size ("
                                << compressedSize << ") is larger than original size (" << totalOriginalSize << ").");
    }
    clearBuffer();
    compressedSizes.push_back(compressedSize);
    memcpy(baseSrcPointer, baseDstPointer, compressedSize);
    this->compressionAlgorithm = CompressionAlgorithm::SPRINTZ;
    free(baseDstPointer);
}

void CompressedDynamicTupleBuffer::decompressSprintzVertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) calloc(1, this->getBuffer().getBufferSize());

    const int64_t decompressedSize = sprintz_decompress_delta_8b(reinterpret_cast<const int8_t*>(baseSrcPointer), baseDstPointer);
    if ((size_t) decompressedSize != this->getBuffer().getBufferSize())
        NES_THROW_RUNTIME_ERROR("Sprintz decompression failed: decompressed size (" << decompressedSize << ") != buffer size ("
                                                                                    << this->getBuffer().getBufferSize() << ").");
    clearBuffer();
    memcpy(baseSrcPointer, baseDstPointer, decompressedSize);
    compressedSizes = {};
    compressionAlgorithm = CompressionAlgorithm::NONE;
    free(baseDstPointer);
}

}// namespace NES::Runtime::MemoryLayouts
