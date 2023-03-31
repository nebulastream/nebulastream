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

/**
 * @brief Wrapper class to represent a DynamicTupleBuffer with additional objects for compression.
 */
class CompressedDynamicTupleBuffer : public DynamicTupleBuffer {
  public:
    explicit CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout, TupleBuffer buffer);
    CompressedDynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout,
                                 TupleBuffer buffer,
                                 CompressionAlgorithm compressionAlgorithm,
                                 CompressionMode compressionMode);

    CompressionAlgorithm compressionAlgorithm;
    CompressionMode compressionMode;
    bool compressed;
    std::vector<uint64_t> offsets;
    std::vector<int> lz4CompressedSizes;

  private:
    std::vector<uint64_t> getOffsets(const MemoryLayoutPtr& memoryLayout);
};

class Compressor {
  public:
    static void compress(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf);

  private:
    static void compressLz4(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf);
    static void compressLz4Columnar(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf);
    static void compressRLE(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf);
};

class Decompressor {
  public:
    static void decompress(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf);

  private:
    static void decompressLz4(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf);
    static void decompressLz4Columnar(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf);
    static void decompressRLE(CompressedDynamicTupleBuffer& inBuf, CompressedDynamicTupleBuffer& outBuf);
};

}// namespace NES::Runtime::MemoryLayouts

#endif//NES_COMPRESSEDDYNAMICTUPLEBUFFER_HPP
