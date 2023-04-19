#include "Runtime/MemoryLayout/CompressedDynamicTupleBuffer.hpp"
#include "API/Schema.hpp"
#include "Runtime/MemoryLayout/ColumnLayout.hpp"
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
    maxBufferSize = this->getBuffer().getBufferSize();
    compressionAlgorithm = CompressionAlgorithm::NONE;
    compressedSizes = std::vector<size_t>{};
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
    maxBufferSize = this->getBuffer().getBufferSize();
    this->compressionAlgorithm = CompressionAlgorithm::NONE;
    this->compressionMode = cm;
    compressedSizes = std::vector<size_t>{};
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
    compressedSizes = std::vector<size_t>{};
    offsets = getOffsets(memoryLayout);
}

CompressionAlgorithm CompressedDynamicTupleBuffer::getCompressionAlgorithm() { return compressionAlgorithm; }
CompressionMode CompressedDynamicTupleBuffer::getCompressionMode() { return compressionMode; }

std::vector<uint64_t> CompressedDynamicTupleBuffer::getOffsets(const MemoryLayoutPtr& memoryLayout) {
    if (auto rowLayout = dynamic_cast<RowLayout*>(memoryLayout.get())) {
        //offsets = rowLayout->getFieldOffSets();
        return {0};
    } else if (auto columnLayout = dynamic_cast<ColumnLayout*>(memoryLayout.get())) {
        return columnLayout->getColumnOffsets();
    }
    NES_NOT_IMPLEMENTED();
}

void CompressedDynamicTupleBuffer::clearBuffer() {
    //auto dynTupleBuffer = *this;
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    for (size_t i = 0; i < this->getBuffer().getBufferSize(); i++) {
        baseSrcPointer[i] = '\0';
    }
}

void CompressedDynamicTupleBuffer::compress(CompressionAlgorithm targetCa) { compress(targetCa, this->compressionMode); }
void CompressedDynamicTupleBuffer::compress(CompressionAlgorithm targetCa, CompressionMode targetCm) {
    this->compressionMode = targetCm;
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
    }
}

void CompressedDynamicTupleBuffer::decompressVertical() {
    switch (compressionAlgorithm) {
        case CompressionAlgorithm::NONE: break;
        case CompressionAlgorithm::LZ4: decompressLz4Vertical(); break;
        case CompressionAlgorithm::SNAPPY: decompressSnappyVertical(); break;
        case CompressionAlgorithm::FSST: decompressFsstVertical(); break;
        case CompressionAlgorithm::RLE: decompressRleVertical(); break;
    }
}

void CompressedDynamicTupleBuffer::concatColumns() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    size_t bufferSize = this->getBuffer().getBufferSize();
    char* src = new char[bufferSize];
    std::vector<uint64_t> newOffsets{0};
    // create one string to be compressed (increases chances of better compression) // TODO move outside
    strcpy(src, reinterpret_cast<const char*>(baseSrcPointer));
    size_t newOffset = 0;
    for (size_t i = 1; i < this->offsets.size(); i++) {
        const char* tmp = reinterpret_cast<const char*>(baseSrcPointer + this->offsets[i]);
        strcat(src, tmp);
        newOffset += strlen(tmp);
        newOffsets.push_back(newOffset);
    }
    clearBuffer();
    memcpy(baseSrcPointer, src, strlen(src));
    this->offsets = newOffsets;
}

// ===================================
// LZ4
// ===================================
void CompressedDynamicTupleBuffer::compressLz4Vertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) malloc(this->getBuffer().getBufferSize());// TODO? new TupleBuffer instead?
    size_t dstLength = 0;
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
        //free(compressed);
    }
    clearBuffer();
    memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    // TODO reduce buffer to `dstLength`
    if (dstLength > maxBufferSize)
        maxBufferSize = dstLength;
    this->compressionAlgorithm = CompressionAlgorithm::LZ4;
    //free(baseDstPointer);
}

void CompressedDynamicTupleBuffer::decompressLz4Vertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) malloc(maxBufferSize);
    int i = 0;
    size_t dstLength = 0;
    for (auto offset : this->offsets) {
        const char* compressed = reinterpret_cast<const char*>(baseSrcPointer + offset);
        const int compressedSize = this->compressedSizes[i];
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
        //free(decompressed);
    }
    if (dstLength > maxBufferSize)
        maxBufferSize = dstLength;
    clearBuffer();
    // TODO could overwrite allocated boundaries
    memcpy(baseSrcPointer, baseDstPointer, maxBufferSize);
    // TODO adjust buffer to `dstLength`
    this->compressedSizes = {};
    this->compressionAlgorithm = CompressionAlgorithm::NONE;
    //free(baseDstPointer);
}

void CompressedDynamicTupleBuffer::compressLz4Horizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    size_t bufferSize = this->getBuffer().getBufferSize();
    char* src = new char[bufferSize];
    strcpy(src, reinterpret_cast<const char*>(baseSrcPointer));
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
    this->compressedSizes = {compressedSize};
    this->compressionAlgorithm = CompressionAlgorithm::LZ4;
    //delete[] src;
    //free(compressed);
}

void CompressedDynamicTupleBuffer::decompressLz4Horizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    const char* compressed = reinterpret_cast<const char*>(baseSrcPointer);
    const size_t compressedSize = this->compressedSizes[0];
    const size_t dstCapacity = 3 * compressedSize;// TODO magic number
    char* const decompressed = (char*) malloc(dstCapacity);
    if (decompressed == nullptr)
        NES_THROW_RUNTIME_ERROR("Invalid destination pointer.");// TODO
    const int decompressedSize = LZ4_decompress_safe(compressed, decompressed, compressedSize, dstCapacity);
    if (decompressedSize < 0)
        NES_THROW_RUNTIME_ERROR("LZ4 decompression failed.");
    clearBuffer();
    if (this->offsets.size() == 1) {
        memcpy(baseSrcPointer, decompressed, decompressedSize);
    } else {
        auto newOffsets = getOffsets(this->getMemoryLayout());
        for (size_t i = 0; i < this->offsets.size(); i++) {
            memcpy(baseSrcPointer + newOffsets[i], decompressed + this->offsets[i], this->getBuffer().getNumberOfTuples());
        }
        this->offsets = newOffsets;
    }
    this->compressionAlgorithm = CompressionAlgorithm::NONE;
    //free(decompressed);
}

// ===================================
// Snappy
// ===================================
void CompressedDynamicTupleBuffer::compressSnappyHorizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    size_t bufferSize = this->getBuffer().getBufferSize();
    char* src = new char[bufferSize];
    strcpy(src, reinterpret_cast<const char*>(baseSrcPointer));
    size_t srcSize = strlen(src);
    std::string compressed;
    size_t compressedSize = snappy::Compress(src, srcSize, &compressed);
    compressedSizes.push_back(compressedSize);
    clearBuffer();
    memcpy(baseSrcPointer, compressed.c_str(), compressedSize);
    this->compressionAlgorithm = CompressionAlgorithm::SNAPPY;
}

void CompressedDynamicTupleBuffer::decompressSnappyHorizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    const char* compressed = reinterpret_cast<const char*>(baseSrcPointer);
    std::string uncompressed;
    bool success = snappy::Uncompress(compressed, compressedSizes[0], &uncompressed);
    if (!success)
        NES_THROW_RUNTIME_ERROR("Snappy decompression failed.");
    clearBuffer();
    if (this->offsets.size() == 1) {
        memcpy(baseSrcPointer, uncompressed.c_str(), uncompressed.size());
    } else {
        auto newOffsets = getOffsets(this->getMemoryLayout());
        for (size_t i = 0; i < this->offsets.size(); i++) {
            auto src = uncompressed.substr(this->offsets[i], this->offsets[i] + this->getBuffer().getNumberOfTuples());
            memcpy(baseSrcPointer + newOffsets[i], src.c_str(), this->getBuffer().getNumberOfTuples());
        }
        this->offsets = newOffsets;
    }
    this->compressionAlgorithm = CompressionAlgorithm::NONE;
}

void CompressedDynamicTupleBuffer::compressSnappyVertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) malloc(maxBufferSize);
    std::string compressed;
    size_t dstLength = 0;
    for (auto offset : this->offsets) {
        const char* src = reinterpret_cast<const char*>(baseSrcPointer + offset);
        size_t srcSize = strlen(src);
        size_t compressedSize = snappy::Compress(src, srcSize, &compressed);
        dstLength += compressedSize;
        memcpy(baseDstPointer + offset, compressed.c_str(), compressedSize);
        compressedSizes.push_back(compressedSize);
    }
    clearBuffer();
    memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    this->compressionAlgorithm = CompressionAlgorithm::SNAPPY;
    //free(baseDstPointer);
}

void CompressedDynamicTupleBuffer::decompressSnappyVertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) malloc(maxBufferSize);
    for (size_t i = 0; i < this->offsets.size(); i++) {
        const char* compressed = reinterpret_cast<const char*>(baseSrcPointer + offsets[i]);
        std::string uncompressed;
        bool success = snappy::Uncompress(compressed, compressedSizes[i], &uncompressed);
        if (!success)
            NES_THROW_RUNTIME_ERROR("Snappy decompression failed.");
        memcpy(baseDstPointer + offsets[i], uncompressed.c_str(), uncompressed.size());
    }
    clearBuffer();
    memcpy(baseSrcPointer, baseDstPointer, maxBufferSize);
    this->compressionAlgorithm = CompressionAlgorithm::NONE;
    //free(baseDstPointer);
}

// ===================================
// FSST
// ===================================
void CompressedDynamicTupleBuffer::compressFsstHorizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) malloc(this->getBuffer().getBufferSize());// TODO? new TupleBuffer instead?

    // prepare encoder and input data
    size_t srcLength = strlen(reinterpret_cast<const char*>(baseSrcPointer));
    unsigned char* input = baseSrcPointer;
    encoder = fsst_create(1, &srcLength, &input, false);

    // prepare compression
    unsigned char* outputPtr;
    std::string output;
    output.resize(this->getBuffer().getBufferSize());
    compressedSizes.resize(1);
    fsstOutSize = 7 + 2 * srcLength;// as specified in fsst.h

    // compress
    size_t numCompressed = fsst_compress(encoder,
                                         1,
                                         &srcLength,
                                         &input,
                                         fsstOutSize,
                                         reinterpret_cast<unsigned char*>(output.data()),
                                         compressedSizes.data(),
                                         &outputPtr);
    if (numCompressed < 1)
        NES_THROW_RUNTIME_ERROR("FSST compression failed.");
    memcpy(baseDstPointer, outputPtr, compressedSizes[0]);
    clearBuffer();
    std::memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    compressionAlgorithm = CompressionAlgorithm::FSST;
}

void CompressedDynamicTupleBuffer::decompressFsstHorizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) malloc(this->getBuffer().getBufferSize());// TODO? new TupleBuffer instead?
    for (size_t i = 0; i < this->getBuffer().getBufferSize(); i++) {// TODO src content exists but without gibberish
        baseDstPointer[i] = '\0';
    }

    // prepare decompression
    fsst_decoder_t decoder = fsst_decoder(encoder);
    auto* output = (unsigned char*) malloc(fsstOutSize);
    for (size_t i = 0; i < fsstOutSize; i++) {// TODO src content exists but without gibberish
        output[i] = '\0';
    }

    // decompress
    size_t bytesOut = fsst_decompress(&decoder, compressedSizes[0], baseSrcPointer + offsets[0], fsstOutSize, output);
    if (bytesOut < 1)
        NES_THROW_RUNTIME_ERROR("FSST decompression failed.");
    if (this->offsets.size() == 1) {
        memcpy(baseDstPointer, output, bytesOut);
    } else {
        auto oldOffsets = getOffsets(this->getMemoryLayout());
        for (size_t i = 0; i < offsets.size(); i++) {
            char* src = (char*) calloc(1, this->getNumberOfTuples());
            auto tmp = strncpy(src, reinterpret_cast<const char*>(output + offsets[i]), this->getNumberOfTuples());
            memcpy(baseDstPointer + oldOffsets[i], tmp, this->getNumberOfTuples());
        }
        this->offsets = oldOffsets;
    }
    clearBuffer();
    std::memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    compressionAlgorithm = CompressionAlgorithm::NONE;
}

void CompressedDynamicTupleBuffer::compressFsstVertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) malloc(this->getBuffer().getBufferSize());// TODO? new TupleBuffer instead?

    // prepare encoder and input data
    size_t numStrings = offsets.size();
    std::vector<size_t> srcLengths;
    srcLengths.reserve(numStrings);
    std::vector<unsigned char*> input;
    input.reserve(numStrings);
    for (size_t offset : offsets) {
        srcLengths.push_back(strlen(reinterpret_cast<const char*>(baseSrcPointer + offset)));
        input.push_back(baseSrcPointer + offset);
    }
    encoder = fsst_create(numStrings, srcLengths.data(), input.data(), false);

    // prepare compression
    std::vector<unsigned char*> outputPtr;
    outputPtr.resize(numStrings);
    std::string output;
    output.resize(this->getBuffer().getBufferSize());
    compressedSizes.resize(numStrings);
    size_t tmpFsstOutSize = 0;
    for (size_t len : srcLengths)
        tmpFsstOutSize += len;
    fsstOutSize = 7 + 2 * tmpFsstOutSize;// as specified in fsst.h

    // compress
    size_t numCompressed = fsst_compress(encoder,
                                         numStrings,
                                         srcLengths.data(),
                                         input.data(),
                                         fsstOutSize,
                                         reinterpret_cast<unsigned char*>(output.data()),
                                         compressedSizes.data(),
                                         outputPtr.data());
    if (numCompressed < 1)
        NES_THROW_RUNTIME_ERROR("FSST compression failed.");
    for (size_t i = 0; i < numStrings; i++) {
        memcpy(baseDstPointer + offsets[i], outputPtr[i], compressedSizes[i]);
    }
    clearBuffer();
    std::memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    compressionAlgorithm = CompressionAlgorithm::FSST;
}

void CompressedDynamicTupleBuffer::decompressFsstVertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) malloc(this->getBuffer().getBufferSize());// TODO? new TupleBuffer instead?
    for (size_t i = 0; i < this->getBuffer().getBufferSize(); i++) {// TODO src content exists but without gibberish
        baseDstPointer[i] = '\0';
    }

    // prepare decompression
    fsst_decoder_t decoder = fsst_decoder(encoder);
    size_t outSize = fsstOutSize / offsets.size();
    auto* output = (unsigned char*) malloc(outSize);
    for (size_t i = 0; i < outSize; i++) {// TODO src content exists but without gibberish
        output[i] = '\0';
    }

    // decompress
    for (size_t i = 0; i < offsets.size(); i++) {
        size_t bytesOut = fsst_decompress(&decoder, compressedSizes[i], baseSrcPointer + offsets[i], outSize, output);
        if (bytesOut < 1)
            NES_THROW_RUNTIME_ERROR("FSST decompression failed.");
        memcpy(baseDstPointer + offsets[i], output, bytesOut);
    }
    clearBuffer();
    std::memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    compressionAlgorithm = CompressionAlgorithm::NONE;
}

// ===================================
// RLE
// ===================================
void CompressedDynamicTupleBuffer::compressRleHorizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    size_t srcSize = std::ceil(1.2 * strlen(reinterpret_cast<const char*>(baseSrcPointer)));
    auto* baseDstPointer = (uint8_t*) malloc(srcSize);// TODO? new TupleBuffer instead?
    for (size_t i = 0; i < srcSize; i++) {            // TODO src content exists but without gibberish
        baseDstPointer[i] = '\0';
    }
    //srcSize = std::ceil(1.2 * bufferSize);
    auto end = pg::brle::encode(baseSrcPointer, baseSrcPointer + srcSize, baseDstPointer);// works
    clearBuffer();
    size_t compressedSize = strlen(reinterpret_cast<const char*>(baseDstPointer));//473
    compressedSizes.push_back(compressedSize);
    memcpy(baseSrcPointer, baseDstPointer, compressedSize);
    this->compressionAlgorithm = CompressionAlgorithm::RLE;
}

void CompressedDynamicTupleBuffer::decompressRleHorizontal() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    uint8_t* baseDstPointer = (uint8_t*) malloc(this->getBuffer().getBufferSize());// TODO? new TupleBuffer instead?
    for (size_t i = 0; i < this->getBuffer().getBufferSize(); i++) {// TODO src content exists but without gibberish
        baseDstPointer[i] = '\0';
    }
    auto end = pg::brle::decode(baseSrcPointer, baseSrcPointer + compressedSizes[0], baseDstPointer);
    clearBuffer();
    size_t uncompressedSize = strlen(reinterpret_cast<const char*>(baseDstPointer));
    compressedSizes = {0};
    if (this->offsets.size() == 1) {
        memcpy(baseSrcPointer, baseDstPointer, uncompressedSize);
    } else {
        auto oldOffsets = getOffsets(this->getMemoryLayout());
        for (size_t i = 0; i < offsets.size(); i++) {
            char* src = (char*) calloc(1, this->getNumberOfTuples());
            auto tmp = strncpy(src, reinterpret_cast<const char*>(baseDstPointer + offsets[i]), this->getNumberOfTuples());
            memcpy(baseSrcPointer + oldOffsets[i], tmp, this->getNumberOfTuples());
        }
        this->offsets = oldOffsets;
    }
    this->compressionAlgorithm = CompressionAlgorithm::NONE;
}

void CompressedDynamicTupleBuffer::compressRleVertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    auto* baseDstPointer = (uint8_t*) malloc(maxBufferSize);
    for (size_t i = 0; i < maxBufferSize; i++) {// TODO src content exists but without gibberish
        baseDstPointer[i] = '\0';
    }
    std::string compressed;
    size_t dstLength = 0;
    for (auto offset : this->offsets) {
        size_t srcSize = strlen(reinterpret_cast<const char*>(baseSrcPointer + offset));
        auto src = baseSrcPointer + offset;
        auto end = pg::brle::encode(src, src + srcSize, baseDstPointer + offset);// works
        compressedSizes.push_back(strlen(reinterpret_cast<const char*>(baseDstPointer + offset)));
    }
    clearBuffer();
    memcpy(baseSrcPointer, baseDstPointer, this->getBuffer().getBufferSize());
    this->compressionAlgorithm = CompressionAlgorithm::RLE;
}

void CompressedDynamicTupleBuffer::decompressRleVertical() {
    uint8_t* baseSrcPointer = this->getBuffer().getBuffer();
    auto* baseDstPointer = (uint8_t*) malloc(maxBufferSize);
    for (size_t i = 0; i < this->offsets.size(); i++) {
        auto src = baseSrcPointer + offsets[i];
        auto end = pg::brle::decode(src, src + compressedSizes[i], baseDstPointer + offsets[i]);
    }
    clearBuffer();
    memcpy(baseSrcPointer, baseDstPointer, maxBufferSize);
    this->compressionAlgorithm = CompressionAlgorithm::NONE;
}

}// namespace NES::Runtime::MemoryLayouts
