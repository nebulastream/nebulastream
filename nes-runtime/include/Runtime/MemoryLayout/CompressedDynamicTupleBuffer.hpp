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
enum class CompressionAlgorithm { NONE, LZ4, SNAPPY, FSST, RLE };
enum class CompressionMode { HORIZONTAL, VERTICAL };

const char* getCompressionAlgorithmName(enum CompressionAlgorithm ca) {
    switch (ca) {
        case CompressionAlgorithm::NONE: return "None";
        case CompressionAlgorithm::LZ4: return "LZ4";
        case CompressionAlgorithm::SNAPPY: return "Snappy";
        case CompressionAlgorithm::FSST: return "FSST";
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
        this->compressedSizes = other.compressedSizes;
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
    std::vector<uint64_t> offsets;// TODO mismatch to MemoryLayout offsets
    std::vector<size_t> compressedSizes;
    fsst_encoder_t* encoder;
    size_t fsstOutSize;

    std::vector<uint64_t> getOffsets(const MemoryLayoutPtr& memoryLayout);
    void clearBuffer();
    void clearBuffer(size_t start);
    void compressHorizontal(CompressionAlgorithm targetCa);
    void compressVertical(CompressionAlgorithm targetCa);
    void decompressHorizontal();
    void decompressVertical();
    void compressLz4Vertical();
    void decompressLz4Vertical();
    void compressLz4Horizontal();
    void decompressLz4Horizontal();
    void concatColumns();
    void compressSnappyVertical();
    void decompressSnappyVertical();
    void compressSnappyHorizontal();
    void decompressSnappyHorizontal();
    void compressFsstHorizontal();
    void decompressFsstHorizontal();
    void compressFsstVertical();
    void decompressFsstVertical();
    void compressRleHorizontal();
    void decompressRleHorizontal();
    void compressRleVertical();
    void decompressRleVertical();
};

}// namespace NES::Runtime::MemoryLayouts

#endif//NES_COMPRESSEDDYNAMICTUPLEBUFFER_HPP
