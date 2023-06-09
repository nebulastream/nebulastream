/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
    baseDstPointer = (uint8_t*) malloc(getBuffer().getBufferSize());
    offsets = getOffsets(memoryLayout);
    compressedSizes = std::vector<size_t>(offsets.size(), 0);
    compressionAlgorithms = std::vector<CompressionAlgorithm>(offsets.size(), CompressionAlgorithm::NONE);
    columnTypes = getMemoryLayout()->getPhysicalTypes();
    // set default compression mode
    auto rowLayout = dynamic_cast<RowLayout*>(getMemoryLayout().get());
    if (rowLayout != nullptr) {
        compressionMode = CompressionMode::HORIZONTAL;
    }
    auto columnLayout = dynamic_cast<ColumnLayout*>(getMemoryLayout().get());
    if (columnLayout != nullptr) {
        compressionMode = CompressionMode::VERTICAL;
    }
}

CompressedDynamicTupleBuffer::CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout,
                                                           TupleBuffer buffer,
                                                           const CompressionMode& compressionMode)
    : DynamicTupleBuffer(memoryLayout, std::move(buffer)) {
    baseDstPointer = (uint8_t*) malloc(getBuffer().getBufferSize());
    offsets = getOffsets(memoryLayout);
    compressedSizes = std::vector<size_t>(offsets.size(), 0);
    compressionAlgorithms = std::vector<CompressionAlgorithm>(offsets.size(), CompressionAlgorithm::NONE);
    columnTypes = getMemoryLayout()->getPhysicalTypes();
    this->compressionMode = compressionMode;
}

CompressedDynamicTupleBuffer::~CompressedDynamicTupleBuffer() { free(baseDstPointer); }

std::vector<CompressionAlgorithm> CompressedDynamicTupleBuffer::getCompressionAlgorithms() { return compressionAlgorithms; }
CompressionMode CompressedDynamicTupleBuffer::getCompressionMode() { return compressionMode; }
std::vector<uint64_t> CompressedDynamicTupleBuffer::getOffsets() { return offsets; }
std::vector<size_t> CompressedDynamicTupleBuffer::getCompressedSizes() { return compressedSizes; }
size_t CompressedDynamicTupleBuffer::getTotalCompressedSize() {
    return std::reduce(compressedSizes.begin(), compressedSizes.end());// sum
}
size_t CompressedDynamicTupleBuffer::getTotalOriginalSize() { return getMemoryLayout()->getTupleSize() * getNumberOfTuples(); }
double CompressedDynamicTupleBuffer::getCompressionRatio() {
    size_t compressedSize = 0;
    for (const auto& s : compressedSizes)
        compressedSize += s;
    return (double) getTotalOriginalSize() / (double) compressedSize;
}

void CompressedDynamicTupleBuffer::clearBuffer(uint8_t* buf, const size_t start, const size_t end) {
    for (size_t i = start; i < end; i++)
        buf[i] = '\0';
}

void CompressedDynamicTupleBuffer::clearBuffer(uint8_t* buf, const size_t size) {
    for (size_t i = 0; i < size; i++)
        buf[i] = '\0';
}

std::vector<uint64_t> CompressedDynamicTupleBuffer::getOffsets(const MemoryLayoutPtr& memoryLayout) {
    if (dynamic_cast<RowLayout*>(memoryLayout.get())) {
        return {0};
    } else if (auto columnLayout = dynamic_cast<ColumnLayout*>(memoryLayout.get())) {
        return columnLayout->getColumnOffsets();
    }
    NES_NOT_IMPLEMENTED();
}

void CompressedDynamicTupleBuffer::concatColumns() {
    if (offsets.size() == 1)
        return;
    uint8_t* baseSrcPointer = getBuffer().getBuffer();
    std::vector<size_t> newOffsets{};
    size_t newOffset = 0;
    for (size_t i = 0; i < offsets.size(); i++) {
        const size_t dstSize = columnTypes[i].get()->size() * getCapacity();
        memcpy(baseDstPointer + newOffset, baseSrcPointer + offsets[i], dstSize);
        newOffsets.push_back(newOffset);
        newOffset += dstSize;
    }
    clearBuffer(baseSrcPointer, getBuffer().getBufferSize());
    memcpy(baseSrcPointer, baseDstPointer, getBuffer().getBufferSize());
    clearBuffer(baseDstPointer, getBuffer().getBufferSize());
    offsets = newOffsets;
}

void CompressedDynamicTupleBuffer::compress(const CompressionAlgorithm ca) { compress(compressionMode, ca); }
void CompressedDynamicTupleBuffer::compress(const std::vector<CompressionAlgorithm>& cas) { compress(compressionMode, cas); }

void CompressedDynamicTupleBuffer::compress(const CompressionMode cm, const CompressionAlgorithm ca) {
    std::vector<CompressionAlgorithm> cas;
    for (size_t i = 0; i < offsets.size(); i++)
        cas.push_back(ca);
    compress(cm, {cas});
}

void CompressedDynamicTupleBuffer::compress(const CompressionMode cm, const std::vector<CompressionAlgorithm>& cas) {
    compressionMode = cm;
    const auto rowLayout = dynamic_cast<RowLayout*>(getMemoryLayout().get());
    if (rowLayout != nullptr) {
        if (cas[0] == CompressionAlgorithm::NONE)
            return;
        switch (cm) {
            case CompressionMode::HORIZONTAL: {
                offsets = getOffsets(getMemoryLayout());
                compressHorizontal(cas[0]);
                break;
            }
            case CompressionMode::VERTICAL: {
                NES_THROW_RUNTIME_ERROR("Vertical compression cannot be performed on row layout.");
            }
        }
        currColumn = 0;
        return;
    }

    const auto columnLayout = dynamic_cast<ColumnLayout*>(getMemoryLayout().get());
    if (columnLayout != nullptr) {
        switch (cm) {
            case CompressionMode::HORIZONTAL: {
                if (cas[0] == CompressionAlgorithm::NONE)
                    return;
                concatColumns();
                compressHorizontal(cas[0]);
                break;
            }
            case CompressionMode::VERTICAL: {
                if (cas.size() != offsets.size())
                    NES_THROW_RUNTIME_ERROR("Number of compression algorithms ("
                                            << cas.size() << ") does match number of offsets (" << offsets.size() << ").");
                offsets = getOffsets(getMemoryLayout());
                compressVertical(cas);
            }
        }
        currColumn = 0;
        return;
    }
    NES_THROW_RUNTIME_ERROR("Invalid memory layout.");
}

void CompressedDynamicTupleBuffer::decompress() {
    const auto rowLayout = dynamic_cast<RowLayout*>(getMemoryLayout().get());
    if (rowLayout != nullptr) {
        if (compressionAlgorithms[currColumn] == CompressionAlgorithm::NONE)
            return;
        switch (compressionMode) {
            case CompressionMode::HORIZONTAL: {
                decompressHorizontal();
                break;
            }
            case CompressionMode::VERTICAL: {
                NES_THROW_RUNTIME_ERROR("Vertical decompression cannot be performed on row layout.");
            }
        }
        memcpy(getBuffer().getBuffer(), baseDstPointer, getBuffer().getBufferSize());
        clearBuffer(baseDstPointer, getBuffer().getBufferSize());
        return;
    }

    const auto columnLayout = dynamic_cast<ColumnLayout*>(getMemoryLayout().get());
    if (columnLayout != nullptr) {
        switch (compressionMode) {
            case CompressionMode::HORIZONTAL: {
                if (compressionAlgorithms[currColumn] == CompressionAlgorithm::NONE)
                    return;
                decompressHorizontal();
                offsets = getOffsets(getMemoryLayout());
                break;
            }
            case CompressionMode::VERTICAL: {
                decompressVertical();
                break;
            }
        }
        memcpy(getBuffer().getBuffer(), baseDstPointer, getBuffer().getBufferSize());
        clearBuffer(baseDstPointer, getBuffer().getBufferSize());
        return;
    }
    NES_THROW_RUNTIME_ERROR("Invalid memory layout.");
}

void CompressedDynamicTupleBuffer::compressHorizontal(const CompressionAlgorithm& ca) {
    if (compressionAlgorithms[currColumn] == CompressionAlgorithm::NONE) {
        size_t dstSize = getBuffer().getBufferSize();
        switch (ca) {
            case CompressionAlgorithm::NONE: break;
            case CompressionAlgorithm::LZ4: compressLz4(0, dstSize); break;
            case CompressionAlgorithm::SNAPPY: compressSnappy(0, dstSize); break;
            case CompressionAlgorithm::RLE: compressRle(0, dstSize); break;
            case CompressionAlgorithm::BINARY_RLE: compressBinaryRle(0, dstSize); break;
            case CompressionAlgorithm::FSST: {
                compressFsst1(0, dstSize);
                compressedWithFsst = true;
                break;
            }
            case CompressionAlgorithm::SPRINTZ: compressSprintz(0, dstSize); break;
        }
    } else {
        // TODO future work
        NES_THROW_RUNTIME_ERROR(printf("Cannot compress from %s to %s.",
                                       getCompressionAlgorithmName(compressionAlgorithms[currColumn]),
                                       getCompressionAlgorithmName(ca)));
    }
}

void CompressedDynamicTupleBuffer::compressVertical(const std::vector<CompressionAlgorithm>& cas) {
    for (auto ca : cas) {
        const size_t end = offsets[currColumn] + getNumberOfTuples() * columnTypes[currColumn].get()->size();
        switch (ca) {
            case CompressionAlgorithm::NONE: break;
            case CompressionAlgorithm::LZ4: compressLz4(offsets[currColumn], end); break;
            case CompressionAlgorithm::SNAPPY: compressSnappy(offsets[currColumn], end); break;
            case CompressionAlgorithm::RLE: compressRle(offsets[currColumn], end); break;
            case CompressionAlgorithm::BINARY_RLE: compressBinaryRle(offsets[currColumn], end); break;
            case CompressionAlgorithm::FSST: {
                // FSST is a special case, since it compresses over all columns simultaneously
                if (!compressedWithFsst) {
                    std::vector<size_t> fsstColumns;
                    fsstColumns.reserve(offsets.size());
                    size_t i = 0;
                    for (auto ca2 : cas) {
                        if (ca2 == CompressionAlgorithm::FSST)
                            fsstColumns.push_back(i);
                        i++;
                    }
                    if (fsstColumns.size() == 1)
                        compressFsst1(offsets[currColumn], end);
                    if (fsstColumns.size() > 1)
                        compressFsstN(fsstColumns);
                    compressedWithFsst = true;
                }
                break;
            }
            case CompressionAlgorithm::SPRINTZ: compressSprintz(offsets[currColumn], end); break;
        }
        currColumn++;
    }
    currColumn = 0;
}

void CompressedDynamicTupleBuffer::decompressHorizontal() {
    size_t dstSize = getBuffer().getBufferSize();
    switch (compressionAlgorithms[currColumn]) {
        case CompressionAlgorithm::NONE: break;
        case CompressionAlgorithm::LZ4: decompressLz4(0, dstSize); break;
        case CompressionAlgorithm::SNAPPY: decompressSnappy(0, dstSize); break;
        case CompressionAlgorithm::RLE: decompressRle(0, dstSize); break;
        case CompressionAlgorithm::BINARY_RLE: decompressBinaryRle(0, dstSize); break;
        case CompressionAlgorithm::FSST: {
            decompressFsst1(0, dstSize);
            compressedWithFsst = false;
            break;
        }
        case CompressionAlgorithm::SPRINTZ: decompressSprintz(0, dstSize); break;
    }
}

void CompressedDynamicTupleBuffer::decompressVertical() {
    for (auto ca : compressionAlgorithms) {
        const size_t dstSize = getNumberOfTuples() * columnTypes[currColumn].get()->size();
        switch (ca) {
            case CompressionAlgorithm::NONE: {
                memcpy(baseDstPointer + offsets[currColumn], getBuffer().getBuffer() + offsets[currColumn], dstSize);
                break;
            }
            case CompressionAlgorithm::LZ4: decompressLz4(offsets[currColumn], dstSize); break;
            case CompressionAlgorithm::SNAPPY: decompressSnappy(offsets[currColumn], dstSize); break;
            case CompressionAlgorithm::FSST: {
                // FSST is a special case, since it compresses over all columns simultaneously
                if (compressedWithFsst) {
                    std::vector<size_t> fsstColumns;
                    fsstColumns.reserve(offsets.size());
                    size_t i = 0;
                    for (auto ca2 : compressionAlgorithms) {
                        if (ca2 == CompressionAlgorithm::FSST)
                            fsstColumns.push_back(i);
                        i++;
                    }
                    if (fsstColumns.size() == 1)
                        decompressFsst1(offsets[currColumn], dstSize);
                    if (fsstColumns.size() > 1)
                        decompressFsstN(fsstColumns);
                    compressedWithFsst = false;
                }
                compressionAlgorithms[currColumn] = CompressionAlgorithm::NONE;
                break;
            }
            case CompressionAlgorithm::RLE: decompressRle(offsets[currColumn], dstSize); break;
            case CompressionAlgorithm::BINARY_RLE: decompressBinaryRle(offsets[currColumn], dstSize); break;
            case CompressionAlgorithm::SPRINTZ: decompressSprintz(offsets[currColumn], dstSize); break;
        }
        currColumn++;
    }
    currColumn = 0;
}

// ===================================
// LZ4
// ===================================
void CompressedDynamicTupleBuffer::compressLz4(const size_t start, const size_t end) {
    uint8_t* baseSrcPointer = getBuffer().getBuffer() + start;
    const int srcSize = int(end - start);
    auto src = reinterpret_cast<const char*>(baseSrcPointer);
    const int dstCapacity = LZ4_compressBound(srcSize);
    char* compressed = (char*) calloc(1, dstCapacity);
    const size_t compressedSize = LZ4_compress_default(src, compressed, srcSize, dstCapacity);
    if (compressedSize <= 0)
        NES_THROW_RUNTIME_ERROR("LZ4 compression failed.");
    if (compressedSize > (size_t) srcSize) {
        // TODO do not compress and return original buffer
        NES_THROW_RUNTIME_ERROR("LZ4 compression failed: compressed size ("
                                << compressedSize << ") is larger than original size (" << srcSize << ").");
    }
    clearBuffer(getBuffer().getBuffer(), offsets[currColumn], offsets[currColumn] + srcSize);
    memcpy(baseSrcPointer, compressed, compressedSize);
    compressedSizes[currColumn] = compressedSize;
    compressionAlgorithms[currColumn] = CompressionAlgorithm::LZ4;
    free(compressed);
}

void CompressedDynamicTupleBuffer::decompressLz4(const size_t start, size_t dstSize) {
    const char* compressed = reinterpret_cast<const char*>(getBuffer().getBuffer() + start);
    char* decompressed = (char*) malloc(dstSize);
    const int decompressedSize = LZ4_decompress_safe(compressed, decompressed, (int) compressedSizes[currColumn], (int) dstSize);
    if (decompressedSize <= 0)
        NES_THROW_RUNTIME_ERROR("LZ4 decompression failed.");
    if ((size_t) decompressedSize != dstSize)
        NES_THROW_RUNTIME_ERROR("LZ4 decompression failed: decompressed size (" << decompressedSize << ") != original size ("
                                                                                << dstSize << ").");
    const auto rowLayout = dynamic_cast<RowLayout*>(getMemoryLayout().get());
    if (rowLayout != nullptr) {
        memcpy(baseDstPointer, decompressed, decompressedSize);
    } else {
        const auto origOffsets = getOffsets(getMemoryLayout());
        if (compressionMode == CompressionMode::VERTICAL) {
            memcpy(baseDstPointer + origOffsets[currColumn], decompressed, dstSize);
        } else {
            for (size_t i = 0; i < offsets.size(); i++) {
                dstSize = columnTypes[i].get()->size() * getNumberOfTuples();
                memcpy(baseDstPointer + origOffsets[i], decompressed + offsets[i], dstSize);
            }
        }
    }
    compressedSizes[currColumn] = 0;
    compressionAlgorithms[currColumn] = CompressionAlgorithm::NONE;
    free(decompressed);
}

// ===================================
// Snappy
// ===================================
void CompressedDynamicTupleBuffer::compressSnappy(const size_t start, const size_t end) {
    uint8_t* baseSrcPointer = getBuffer().getBuffer() + start;
    const int srcSize = int(end - start);
    auto src = reinterpret_cast<const char*>(baseSrcPointer);
    const size_t dstCapacity = snappy::MaxCompressedLength(srcSize);
    char* compressed = (char*) calloc(1, dstCapacity);
    size_t compressedSize = 0;
    snappy::RawCompress(src, srcSize, compressed, &compressedSize);
    if (compressedSize <= 0)
        NES_THROW_RUNTIME_ERROR("Snappy compression failed.");
    if (compressedSize > (size_t) srcSize) {
        // TODO do not compress and return original buffer
        NES_THROW_RUNTIME_ERROR("Snappy compression failed: compressed size ("
                                << compressedSize << ") is larger than original size (" << srcSize << ").");
    }
    clearBuffer(getBuffer().getBuffer(), offsets[currColumn], offsets[currColumn] + srcSize);
    memcpy(baseSrcPointer, compressed, compressedSize);
    compressedSizes[currColumn] = compressedSize;
    compressionAlgorithms[currColumn] = CompressionAlgorithm::SNAPPY;
    free(compressed);
}

void CompressedDynamicTupleBuffer::decompressSnappy(const size_t start, size_t dstSize) {
    const char* compressed = reinterpret_cast<const char*>(getBuffer().getBuffer() + start);
    char* decompressed = (char*) calloc(offsets.size(), dstSize);
    bool success = snappy::RawUncompress(compressed, compressedSizes[currColumn], decompressed);
    if (!success)
        NES_THROW_RUNTIME_ERROR("Snappy decompression failed.");
    const auto rowLayout = dynamic_cast<RowLayout*>(getMemoryLayout().get());
    if (rowLayout != nullptr) {
        memcpy(baseDstPointer, decompressed, dstSize);
    } else {
        const auto origOffsets = getOffsets(getMemoryLayout());
        if (compressionMode == CompressionMode::VERTICAL) {
            memcpy(baseDstPointer + origOffsets[currColumn], decompressed, dstSize);
        } else {
            for (size_t i = 0; i < offsets.size(); i++) {
                dstSize = columnTypes[i].get()->size() * getNumberOfTuples();
                memcpy(baseDstPointer + origOffsets[i], decompressed + offsets[i], dstSize);
            }
        }
    }
    compressedSizes[currColumn] = 0;
    compressionAlgorithms[currColumn] = CompressionAlgorithm::NONE;
    free(decompressed);
}

// ===================================
// RLE
// ===================================
namespace rle {
static size_t compress(const uint8_t* src, size_t srcLen, uint8_t* out) {
    // TODO refactor redundant writes
    uint8_t value = src[0];
    uint8_t count = 1;
    size_t compressedSize = 0;
    for (size_t i = 1; i < srcLen; i++) {
        if (src[i] == value) {
            count++;
            if (count == 255) {
                memcpy(out, &count, 1);
                out++;
                memcpy(out, &value, 1);
                out++;
                compressedSize += 2;
                count = 0;
            }
        } else {
            memcpy(out, &count, 1);
            out++;
            memcpy(out, &value, 1);
            out++;
            compressedSize += 2;
            value = src[i];
            count = 1;
        }
    }
    memcpy(out, &count, 1);
    out++;
    memcpy(out, &value, 1);
    compressedSize += 2;
    return compressedSize;
}

static size_t decompress(const uint8_t* src, size_t srcLen, uint8_t* out) {
    uint8_t count = src[0];
    uint8_t value = src[1];
    size_t decompressedSize = 0;
    size_t i = 0;
    while (i < srcLen) {
        for (uint8_t j = 0; j < count; j++) {
            memcpy(out, &value, 1);
            out++;
            decompressedSize++;
        }
        i += 2;
        count = src[i];
        value = src[i + 1];
    }
    return decompressedSize;
}
}// namespace rle

void CompressedDynamicTupleBuffer::compressRle(size_t start, size_t end) {
    uint8_t* baseSrcPointer = getBuffer().getBuffer() + start;
    size_t srcSize = end - start;
    size_t compressedSize = rle::compress(baseSrcPointer, srcSize, baseDstPointer);
    if (compressedSize > srcSize) {
        // TODO do not compress and return original buffer
        NES_THROW_RUNTIME_ERROR("RLE compression failed: compressed size ("
                                << compressedSize << ") is larger than original size (" << srcSize << ").");
    }
    clearBuffer(getBuffer().getBuffer(), offsets[currColumn], offsets[currColumn] + srcSize);
    memcpy(baseSrcPointer, baseDstPointer, compressedSize);
    compressedSizes[currColumn] = compressedSize;
    compressionAlgorithms[currColumn] = CompressionAlgorithm::RLE;
}

void CompressedDynamicTupleBuffer::decompressRle(size_t start, size_t dstSize) {
    uint8_t* decompressed = (uint8_t*) calloc(1, dstSize);
    size_t decompressedSize = rle::decompress(getBuffer().getBuffer() + start, compressedSizes[currColumn], decompressed);
    if (decompressedSize != dstSize)
        NES_THROW_RUNTIME_ERROR("RLE decompression failed: decompressed size (" << decompressedSize << ") != original size ("
                                                                                << dstSize << ").");
    const auto rowLayout = dynamic_cast<RowLayout*>(getMemoryLayout().get());
    if (rowLayout != nullptr) {
        memcpy(baseDstPointer, decompressed, decompressedSize);
    } else {
        const auto origOffsets = getOffsets(getMemoryLayout());
        if (compressionMode == CompressionMode::VERTICAL) {
            memcpy(baseDstPointer + origOffsets[currColumn], decompressed, dstSize);
        } else {
            for (size_t i = 0; i < offsets.size(); i++) {
                dstSize = columnTypes[i].get()->size() * getCapacity();
                memcpy(baseDstPointer + origOffsets[i], decompressed + offsets[i], dstSize);
            }
        }
    }
    compressedSizes[currColumn] = 0;
    compressionAlgorithms[currColumn] = CompressionAlgorithm::NONE;
    free(decompressed);
}

// ===================================
// BINARY_RLE
// ===================================
void CompressedDynamicTupleBuffer::compressBinaryRle(size_t start, size_t end) {
    uint8_t* baseSrcPointer = getBuffer().getBuffer() + start;
    const int srcSize = int(end - start);
    const size_t dstSize = std::ceil(1.16 * (double) srcSize);// as specified in docs
    uint8_t* compressedEnd =
        pg::brle::encode(baseSrcPointer, baseSrcPointer + srcSize, baseDstPointer);// TODO always write to same position
    const size_t compressedSize = std::distance(baseDstPointer, compressedEnd);
    if (compressedSize > (size_t) srcSize) {
        // TODO do not compress and return original buffer
        NES_THROW_RUNTIME_ERROR("Binary RLE compression failed: compressed size ("
                                << compressedSize << ") is larger than original size (" << srcSize << ").");
    }
    clearBuffer(getBuffer().getBuffer(), offsets[currColumn], offsets[currColumn] + srcSize);
    memcpy(baseSrcPointer, baseDstPointer, compressedSize);
    compressedSizes[currColumn] = compressedSize;
    compressionAlgorithms[currColumn] = CompressionAlgorithm::BINARY_RLE;
    //free(compressedEnd);
}

void CompressedDynamicTupleBuffer::decompressBinaryRle(size_t start, size_t dstSize) {
    const uint8_t* compressed = getBuffer().getBuffer() + start;
    uint8_t* decompressed = (uint8_t*) calloc(1, dstSize);
    uint8_t* decompressedEnd = pg::brle::decode(compressed, compressed + compressedSizes[currColumn], decompressed);
    const size_t decompressedSize = std::distance(decompressed, decompressedEnd);
    if ((size_t) decompressedSize != dstSize)
        NES_THROW_RUNTIME_ERROR("Binary RLE decompression failed: decompressed size ("
                                << decompressedSize << ") != original size (" << dstSize << ").");
    const auto rowLayout = dynamic_cast<RowLayout*>(getMemoryLayout().get());
    if (rowLayout != nullptr) {
        memcpy(baseDstPointer, decompressed, decompressedSize);
    } else {
        const auto origOffsets = getOffsets(getMemoryLayout());
        if (compressionMode == CompressionMode::VERTICAL) {
            memcpy(baseDstPointer + origOffsets[currColumn], decompressed, dstSize);
        } else {
            for (size_t i = 0; i < offsets.size(); i++) {
                dstSize = columnTypes[i].get()->size() * getCapacity();
                memcpy(baseDstPointer + origOffsets[i], decompressed + offsets[i], dstSize);
            }
        }
    }
    compressedSizes[currColumn] = 0;
    compressionAlgorithms[currColumn] = CompressionAlgorithm::NONE;
    free(decompressed);
}

// ===================================
// FSST
// ===================================
void CompressedDynamicTupleBuffer::compressFsst1(const size_t start, const size_t end) {
    uint8_t* baseSrcPointer = getBuffer().getBuffer() + start;
    size_t srcSize = int(end - start);

    // prepare compression
    fsstEncoder = fsst_create(1, &srcSize, &baseSrcPointer, false);
    size_t compressedSize = 0;
    fsstOutSize = 7 + 2 * srcSize;// as specified in fsst.h
    auto compressed = (unsigned char*) malloc(fsstOutSize);
    std::string output;
    output.resize(fsstOutSize);

    // compress
    size_t numCompressed = fsst_compress(fsstEncoder,
                                         1,
                                         &srcSize,
                                         &baseSrcPointer,
                                         fsstOutSize,
                                         reinterpret_cast<unsigned char*>(output.data()),
                                         &compressedSize,
                                         &compressed);
    if (numCompressed < 1)
        NES_THROW_RUNTIME_ERROR("FSST compression failed.");
    if (compressedSize > srcSize) {
        // TODO do not compress and return original buffer
        NES_THROW_RUNTIME_ERROR("FSST compression failed: compressed size ("
                                << compressedSize << ") is larger than original size (" << srcSize << ").");
    }
    clearBuffer(getBuffer().getBuffer(), offsets[currColumn], offsets[currColumn] + srcSize);
    memcpy(baseSrcPointer, compressed, compressedSize);
    compressedSizes[currColumn] = compressedSize;
    compressionAlgorithms[currColumn] = CompressionAlgorithm::FSST;
    //free(compressed);
}

void CompressedDynamicTupleBuffer::decompressFsst1(const size_t start, size_t dstSize) {
    auto compressed = reinterpret_cast<unsigned char*>(getBuffer().getBuffer() + start);

    // prepare decompression
    fsst_decoder_t decoder = fsst_decoder(fsstEncoder);
    auto* decompressed = (unsigned char*) calloc(1, fsstOutSize);

    // decompress
    const size_t decompressedSize = fsst_decompress(&decoder, compressedSizes[currColumn], compressed, fsstOutSize, decompressed);
    if (decompressedSize < 1)
        NES_THROW_RUNTIME_ERROR("FSST decompression failed.");
    if ((size_t) decompressedSize != dstSize)
        NES_THROW_RUNTIME_ERROR("FSST decompression failed: decompressed size (" << decompressedSize << ") != original size ("
                                                                                 << dstSize << ").");
    const auto rowLayout = dynamic_cast<RowLayout*>(getMemoryLayout().get());
    if (rowLayout != nullptr) {
        memcpy(baseDstPointer, decompressed, dstSize);
    } else {
        const auto origOffsets = getOffsets(getMemoryLayout());
        if (compressionMode == CompressionMode::VERTICAL) {
            memcpy(baseDstPointer + origOffsets[currColumn], decompressed, dstSize);
        } else {
            for (size_t i = 0; i < offsets.size(); i++) {
                dstSize = columnTypes[i].get()->size() * getNumberOfTuples();
                memcpy(baseDstPointer + origOffsets[i], decompressed + offsets[i], dstSize);
            }
        }
    }

    compressedSizes[currColumn] = 0;
    compressedSizes = std::vector<size_t>(offsets.size(), 0);
    fsstOutSize = 0;
    free(decompressed);
}

void CompressedDynamicTupleBuffer::compressFsstN(const std::vector<size_t>& fsstColumns) {
    // prepare encoder and input data
    size_t numColumns = fsstColumns.size();
    std::vector<size_t> srcLengths;
    srcLengths.reserve(numColumns);
    std::vector<unsigned char*> input;
    input.reserve(numColumns);
    for (auto i : fsstColumns) {
        size_t srcSize = columnTypes[i].get()->size() * getCapacity();
        srcLengths.push_back(srcSize);
        input.push_back(getBuffer().getBuffer() + offsets[i]);
    }
    fsstEncoder = fsst_create(numColumns, srcLengths.data(), input.data(), false);

    // prepare compression
    size_t tmpFsstOutSize = 0;
    for (size_t len : srcLengths) {
        tmpFsstOutSize += len;
    }
    fsstOutSize = 7 + 2 * tmpFsstOutSize;// as specified in fsst.h
    std::vector<unsigned char*> compressed;
    compressed.resize(numColumns);
    std::vector<size_t> compressedSizesFsst;
    compressedSizesFsst.reserve(fsstColumns.size());
    std::string output;
    output.resize(fsstOutSize);

    // compress
    const size_t numCompressed = fsst_compress(fsstEncoder,
                                               numColumns,
                                               srcLengths.data(),
                                               input.data(),
                                               fsstOutSize,
                                               reinterpret_cast<unsigned char*>(output.data()),
                                               compressedSizesFsst.data(),
                                               compressed.data());
    if (numCompressed < 1)
        NES_THROW_RUNTIME_ERROR("FSST compression failed.");
    for (size_t i = 0; i < srcLengths.size(); i++) {
        if (compressedSizesFsst[i] > srcLengths[i]) {
            // TODO do not compress and return original buffer
            NES_THROW_RUNTIME_ERROR("FSST compression failed: compressed size ("
                                    << compressedSizesFsst[i] << ") is larger than original size (" << srcLengths[i] << ").");
        }
    }
    size_t j = 0;
    for (auto fsstColIdx : fsstColumns) {
        compressedSizes[fsstColIdx] = compressedSizesFsst[j];
        j++;
    }

    size_t i = 0;
    for (size_t fsstCol : fsstColumns) {
        auto dest = getBuffer().getBuffer() + offsets[fsstCol];
        memcpy(dest, compressed[i], compressedSizes[fsstCol]);
        if (fsstCol == offsets.size() - 1) {
            clearBuffer(getBuffer().getBuffer(), offsets[fsstCol] + compressedSizes[i], getBuffer().getBufferSize());
            break;
        }
        clearBuffer(getBuffer().getBuffer(), offsets[fsstCol] + compressedSizes[i], offsets[fsstCol + 1]);
        i++;
    }
    for (auto fsstColIdx : fsstColumns)
        compressionAlgorithms[fsstColIdx] = CompressionAlgorithm::FSST;
}

void CompressedDynamicTupleBuffer::decompressFsstN(const std::vector<size_t>& fsstColumns) {
    uint8_t* baseSrcPointer = getBuffer().getBuffer();

    // prepare decompression
    fsst_decoder_t decoder = fsst_decoder(fsstEncoder);
    const size_t outSize = fsstOutSize / fsstColumns.size();
    auto* output = (unsigned char*) calloc(1, outSize);

    // decompress
    auto origOffsets = getOffsets(getMemoryLayout());
    for (auto fsstCol : fsstColumns) {
        const size_t decompressedSize =
            fsst_decompress(&decoder, compressedSizes[fsstCol], baseSrcPointer + offsets[fsstCol], outSize, output);
        if (decompressedSize < 1)
            NES_THROW_RUNTIME_ERROR("FSST decompression failed.");
        size_t origSize = columnTypes[fsstCol].get()->size() * getCapacity();
        if (decompressedSize != origSize)
            NES_THROW_RUNTIME_ERROR("FSST decompression failed: decompressed size (" << decompressedSize << ") != original size ("
                                                                                     << origSize << ").");
        memcpy(baseDstPointer + origOffsets[fsstCol], output, decompressedSize);
    }
    for (auto fsstCol : fsstColumns)
        std::memcpy(baseSrcPointer + origOffsets[fsstCol],
                    baseDstPointer + origOffsets[fsstCol],
                    columnTypes[fsstCol].get()->size() * getCapacity());
    for (auto fsstColIdx : fsstColumns)
        compressedSizes[fsstColIdx] = 0;
    fsstEncoder = {};
    fsstOutSize = 0;
    free(output);
}

// ===================================
// Sprintz
// ===================================
void CompressedDynamicTupleBuffer::compressSprintz(const size_t start, const size_t end) {
    uint8_t* baseSrcPointer = getBuffer().getBuffer() + start;
    const int srcSize = int(end - start);
    int8_t* compressed = (int8_t*) calloc(1, 1.2 * srcSize);// TODO magic number
    uint16_t numDimensions = offsets.size();
    if (dynamic_cast<ColumnLayout*>(getMemoryLayout().get()))
        numDimensions = 1;
    int64_t compressedSize = sprintz_compress_delta_8b(baseSrcPointer, srcSize, compressed, numDimensions, true);
    if (compressedSize > (int64_t) srcSize) {
        // TODO do not compress and return original buffer
        NES_THROW_RUNTIME_ERROR("Sprintz compression failed: compressed size ("
                                << compressedSize << ") is larger than original size (" << srcSize << ").");
    }
    clearBuffer(getBuffer().getBuffer(), offsets[currColumn], offsets[currColumn] + srcSize);
    memcpy(baseSrcPointer, compressed, compressedSize);
    compressedSizes[currColumn] = compressedSize;
    compressionAlgorithms[currColumn] = CompressionAlgorithm::SPRINTZ;
    free(compressed);
}

void CompressedDynamicTupleBuffer::decompressSprintz(const size_t start, size_t dstSize) {
    int8_t* compressed = reinterpret_cast<int8_t*>(getBuffer().getBuffer() + start);
    uint8_t* decompressed = (uint8_t*) calloc(1, dstSize);
    const int64_t decompressedSize = sprintz_decompress_delta_8b(compressed, decompressed);
    if ((size_t) decompressedSize != dstSize)
        NES_THROW_RUNTIME_ERROR("Sprintz decompression failed: decompressed size (" << decompressedSize << ") != original size ("
                                                                                    << dstSize << ").");
    const auto rowLayout = dynamic_cast<RowLayout*>(getMemoryLayout().get());
    if (rowLayout != nullptr) {
        memcpy(baseDstPointer, decompressed, decompressedSize);
    } else {
        const auto origOffsets = getOffsets(getMemoryLayout());
        if (compressionMode == CompressionMode::VERTICAL) {
            memcpy(baseDstPointer + origOffsets[currColumn], decompressed, dstSize);
        } else {
            for (size_t i = 0; i < offsets.size(); i++) {
                dstSize = columnTypes[i].get()->size() * getNumberOfTuples();
                memcpy(baseDstPointer + origOffsets[i], decompressed + offsets[i], dstSize);
            }
        }
    }
    compressedSizes[currColumn] = 0;
    compressionAlgorithms[currColumn] = CompressionAlgorithm::NONE;
    free(decompressed);
}

}// namespace NES::Runtime::MemoryLayouts
