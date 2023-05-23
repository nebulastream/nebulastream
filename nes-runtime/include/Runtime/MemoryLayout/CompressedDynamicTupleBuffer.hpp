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

#ifndef NES_COMPRESSEDDYNAMICTUPLEBUFFER_HPP
#define NES_COMPRESSEDDYNAMICTUPLEBUFFER_HPP

#include "DynamicTupleBuffer.hpp"
#include "Runtime/MemoryLayout/Compression/fsst.h"
#include <cstring>
#include <lz4.h>
#include <snappy.h>
#include <string>
#include <utility>

namespace NES::Runtime::MemoryLayouts {
enum class CompressionAlgorithm { NONE, LZ4, SNAPPY, BINARY_RLE, FSST, SPRINTZ };
enum class CompressionMode { HORIZONTAL, VERTICAL };

const char* getCompressionAlgorithmName(enum CompressionAlgorithm ca) {
    switch (ca) {
        case CompressionAlgorithm::NONE: return "None";
        case CompressionAlgorithm::LZ4: return "LZ4";
        case CompressionAlgorithm::SNAPPY: return "Snappy";
        case CompressionAlgorithm::BINARY_RLE: return "BINARY RLE";
        case CompressionAlgorithm::FSST: return "FSST";
        case CompressionAlgorithm::SPRINTZ: return "Sprintz";
    }
}

/**
 * @brief Wrapper class to represent a DynamicTupleBuffer with additional objects for compression.
 */
class CompressedDynamicTupleBuffer : public DynamicTupleBuffer {
  public:
    explicit CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout, TupleBuffer buffer);
    explicit CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout,
                                          TupleBuffer buffer,
                                          const CompressionMode& compressionMode);
    ~CompressedDynamicTupleBuffer();

    void setNumberOfTuples(uint64_t value);

    std::vector<CompressionAlgorithm> getCompressionAlgorithms();
    CompressionMode getCompressionMode();
    std::vector<uint64_t> getOffsets();
    std::vector<size_t> getCompressedSize();
    double getCompressionRatio();

    void compress(CompressionAlgorithm ca);
    void compress(const std::vector<CompressionAlgorithm>& cas);
    void compress(CompressionMode targetCm, CompressionAlgorithm ca);
    /**
     * Forward compression call to corresponding function depending on compression mode (targetCm) and compression algorithm(s) (cas).
     * If 1 compression algorithm is provided, it compresses the whole buffer with it.
     * If n compression algorithms are provided, it compresses the corresponding column with the compression algorithm (-> n == #columns).
     */
    void compress(CompressionMode targetCm, const std::vector<CompressionAlgorithm>& cas);
    /**
     * Forward decompression call to corresponding function depending on compression mode and compression algorithm(s).
     */
    void decompress();

  private:
    uint8_t* baseDstPointer;
    std::vector<CompressionAlgorithm> compressionAlgorithms;
    CompressionMode compressionMode;
    std::vector<size_t> offsets;
    std::vector<std::shared_ptr<PhysicalType>> columnTypes;
    size_t currColumn = 0;
    std::vector<size_t> compressedSizes;
    size_t totalOriginalSize;
    bool compressedWithFsst = false;
    fsst_encoder_t* fsstEncoder = nullptr;
    size_t fsstOutSize = 0;

    /**
     * Set values in buffer to NULL for given range.
     * @param buf buffer to clear
     * @param start start position
     * @param end end position
     */
    static void clearBuffer(uint8_t* buf, size_t start, size_t end);
    /**
     * Set values in buffer to NULL from beginning to specified end.
     * @param buf buffer to clear
     * @param size number of bytes to clear
     */
    static void clearBuffer(uint8_t* buf, size_t size);
    /**
     * Return original column offsets from memory layout.
     */
    static std::vector<size_t> getOffsets(const MemoryLayoutPtr& memoryLayout);
    /**
     * Concatenates column into one consecutive sequence.
     * This method essentially removes NULL values that separate the columns and thus my increase the compression ratio.
     */
    void concatColumns();
    /**
     * Selects compression function based on compression algorithm (ca).
     * 'Horizontal' meaning that it compresses a consecutive sequence of bytes.
     */
    void compressHorizontal(const CompressionAlgorithm& ca);
    /**
     * Selects compression function based on compression algorithm (ca).
     * 'Vertical' meaning that it applies the compression function to each column individually.
     */
    void compressVertical(const std::vector<CompressionAlgorithm>& cas);
    /**
     * Selects decompression function based on compression algorithm (class member).
     * 'Horizontal' meaning that it decompresses a consecutive sequence of bytes.
     */
    void decompressHorizontal();
    /**
     * Selects decompression function based on compression algorithm (class member).
     * 'Vertical' meaning that it applies the compression function to each column individually.
     */
    void decompressVertical();

    void compressLz4(size_t start, size_t end);
    void decompressLz4(size_t start, size_t dstSize);
    void compressSnappy(size_t start, size_t end);
    void decompressSnappy(size_t start, size_t dstSize);
    void compressBinaryRle(size_t start, size_t end);
    void decompressBinaryRle(size_t start, size_t dstSize);
    void compressFsst1(size_t start, size_t dstSize);
    void decompressFsst1(size_t start, size_t dstSize);
    void compressFsstN(const std::vector<size_t>& fsstColumns);
    void decompressFsstN(const std::vector<size_t>& fsstColumns);
    void compressSprintz(size_t start, size_t end);
    void decompressSprintz(size_t start, size_t dstSize);
};

}// namespace NES::Runtime::MemoryLayouts

#endif//NES_COMPRESSEDDYNAMICTUPLEBUFFER_HPP
