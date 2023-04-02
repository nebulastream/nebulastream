#ifndef NES_COMPRESSEDDYNAMICTUPLEBUFFER_HPP
#define NES_COMPRESSEDDYNAMICTUPLEBUFFER_HPP

#include "Compression.hpp"
#include "DynamicTupleBuffer.hpp"
#include <cstring>
#include <lz4.h>
#include <snappy.h>
#include <string>
#include <utility>

namespace NES::Runtime::MemoryLayouts {
enum class CompressionAlgorithm { NONE, LZ4, SNAPPY, RLE };
enum class CompressionMode { FULL_BUFFER, COLUMN_WISE };

const char* getCompressionAlgorithmName(enum CompressionAlgorithm ca) {
    switch (ca) {
        case CompressionAlgorithm::NONE: return "None";
        case CompressionAlgorithm::LZ4: return "LZ4";
        case CompressionAlgorithm::SNAPPY: return "Snappy";
        case CompressionAlgorithm::RLE: return "RLE";
    }
}

/**
 * @brief Wrapper class to represent a DynamicTupleBuffer with additional objects for compression.
 */
class CompressedDynamicTupleBuffer : public DynamicTupleBuffer {
  public:
    explicit CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout, TupleBuffer buffer);
    CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout, TupleBuffer buffer, CompressionMode cm);
    CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout,
                                 TupleBuffer buffer,
                                 CompressionAlgorithm ca,
                                 CompressionMode cm);

    /// @brief Copy constructor
    CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout,
                                 TupleBuffer buffer,
                                 const CompressedDynamicTupleBuffer& other)
        : DynamicTupleBuffer(memoryLayout, buffer) {// TODO this one does not copy
        this->compressionAlgorithm = other.compressionAlgorithm;
        this->compressionMode = other.compressionMode;
        this->maxBufferSize = other.maxBufferSize;
        this->offsets = other.offsets;
        this->lz4CompressedSizes = other.lz4CompressedSizes;
    }

    CompressionAlgorithm getCompressionAlgorithm();
    CompressionMode getCompressionMode();

    void compress(CompressionAlgorithm targetCa);
    void compress(CompressionAlgorithm targetCa, CompressionMode targetCm);
    void decompress();

  private:
    CompressionAlgorithm compressionAlgorithm;
    CompressionMode compressionMode;
    size_t maxBufferSize;
    std::vector<uint64_t> offsets;
    std::vector<size_t> lz4CompressedSizes;

    std::vector<uint64_t> getOffsets(const MemoryLayoutPtr& memoryLayout);
    void clearBuffer();
    void compressLz4FullBuffer();
    void decompressLz4FullBuffer();
    void compressLz4ColumnWise();
    void decompressLz4ColumnWise();
};

}// namespace NES::Runtime::MemoryLayouts

#endif//NES_COMPRESSEDDYNAMICTUPLEBUFFER_HPP
