#ifndef NES_RUNTIME_INCLUDE_RUNTIME_MEMORYLAYOUT_COMPRESSIONALGORITHMS_HPP
#define NES_RUNTIME_INCLUDE_RUNTIME_MEMORYLAYOUT_COMPRESSIONALGORITHMS_HPP

#include "DynamicTupleBuffer.hpp"
#include <cstring>
#include <lz4.h>
#include <snappy.h>
#include <string>

namespace NES::Runtime::MemoryLayouts {
enum class CompressionAlgorithm { NONE, LZ4, SNAPPY, RLE };// TODO? set first do 1 for encoding?

class Compress {
  public:
    // RowLayout
    static int LZ4(DynamicTupleBuffer inBuf, DynamicTupleBuffer outBuf) {
        uint8_t* baseSrcPointer = inBuf.getBuffer().getBuffer();
        uint8_t* baseDstPointer = outBuf.getBuffer().getBuffer();
        const char* src = reinterpret_cast<const char*>(baseSrcPointer);
        const int srcSize = strlen(src);
        const int maxDstSize = LZ4_compressBound(srcSize);// TODO handle if > buffer size
        char* compressed = (char*) malloc((size_t) maxDstSize);
        if (compressed == nullptr)
            throw std::logic_error("not implemented");// TODO
        const int compressedSize = LZ4_compress_default(src, compressed, srcSize, outBuf.getCapacity());
        if (compressedSize <= 0)
            throw std::logic_error("not implemented");// TODO
        // free up memory
        compressed = (char*) realloc(compressed, (size_t) compressedSize);// TODO? always same as buffer size
        memcpy(baseDstPointer, compressed, compressedSize);
        return compressedSize;
    }
    // ColumnLayout
    static std::vector<int>
    LZ4(DynamicTupleBuffer inBuf, DynamicTupleBuffer outBuf, const std::vector<uint64_t>& offsets) {// TODO
        uint8_t* baseSrcPointer = inBuf.getBuffer().getBuffer();
        uint8_t* baseDstPointer = outBuf.getBuffer().getBuffer();
        std::vector<int> compressedSizes;
        for (auto offset : offsets) {
            const char* src = reinterpret_cast<const char*>(baseSrcPointer + offset);
            const int srcSize = strlen(src);
            const int maxDstSize = LZ4_compressBound(srcSize);// TODO handle if > buffer size
            char* compressed = (char*) malloc((size_t) maxDstSize);
            if (compressed == nullptr)
                throw std::logic_error("not implemented");// TODO
            const int compressedSize = LZ4_compress_default(src, compressed, srcSize, outBuf.getCapacity());
            if (compressedSize <= 0)
                throw std::logic_error("not implemented");// TODO
            compressedSizes.push_back(compressedSize);
            // free up memory
            compressed = (char*) realloc(compressed, (size_t) compressedSize);// TODO? always same as buffer size
            memcpy(baseDstPointer + offset, compressed, compressedSize);
        }
        return compressedSizes;
    }
    static void Snappy(DynamicTupleBuffer inBuf, DynamicTupleBuffer outBuf) {// TODO
        TupleBuffer inTupleBuffer = inBuf.getBuffer();
        const char* in = reinterpret_cast<const char*>(inTupleBuffer.getBuffer());
        size_t size = strlen(in);
        std::string compressed;// TODO use const char*? see `RawCompress()`
        //size_t compressedSize = snappy::Compress(in, size, &compressed); TODO linker issue
        TupleBuffer outTupleBuffer = outBuf.getBuffer();
        uint8_t* out = outTupleBuffer.getBuffer();
        memcpy(out, compressed.c_str(), compressed.size());
    }
    // RowLayout
    static void RLE(DynamicTupleBuffer inBuf,
                    DynamicTupleBuffer outBuf) {// TODO? pass inBuf as const reference?
        uint8_t* baseSrcPointer = inBuf.getBuffer().getBuffer();
        uint8_t* baseDstPointer = outBuf.getBuffer().getBuffer();
        const char* in = reinterpret_cast<const char*>(baseSrcPointer);
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
        memcpy(baseDstPointer, temp.c_str(), temp.size());// TODO handle case if buffer is too small
    }
    // ColumnLayout
    static void RLE(DynamicTupleBuffer inBuf,
                    DynamicTupleBuffer outBuf,
                    const std::vector<uint64_t>& offsets) {// TODO? pass inBuf as const reference?
        uint8_t* baseSrcPointer = inBuf.getBuffer().getBuffer();
        uint8_t* baseDstPointer = outBuf.getBuffer().getBuffer();
        for (auto offset : offsets) {
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
    }
};

class Decompress {
  public:
    // RowLayout
    static void LZ4(DynamicTupleBuffer inBuf, DynamicTupleBuffer outBuf, const int& compressedSize) {
        uint8_t* baseSrcPointer = inBuf.getBuffer().getBuffer();
        uint8_t* baseDstPointer = outBuf.getBuffer().getBuffer();
        const char* compressed = reinterpret_cast<const char*>(baseSrcPointer);
        //const int compressedSize = strlen(compressed); // TODO output of LZ4_compressBound()

        char* const decompressed = (char*) malloc(compressedSize * 2);
        if (decompressed == nullptr)
            throw std::logic_error("not implemented");// TODO
        const int decompressedSize = LZ4_decompress_safe(compressed, decompressed, compressedSize, outBuf.getCapacity());
        if (decompressedSize < 0)
            throw std::logic_error("not implemented");// TODO
        memcpy(baseDstPointer, decompressed, decompressedSize);
    }
    // ColumnLayout
    static void LZ4(DynamicTupleBuffer inBuf,
                    DynamicTupleBuffer outBuf,
                    const std::vector<uint64_t>& offsets,
                    const std::vector<int>& compressedSizes) {// TODO)
        uint8_t* baseSrcPointer = inBuf.getBuffer().getBuffer();
        uint8_t* baseDstPointer = outBuf.getBuffer().getBuffer();
        int i = 0;
        for (auto offset : offsets) {
            const char* compressed = reinterpret_cast<const char*>(baseSrcPointer + offset);
            const int compressedSize = compressedSizes[i];
            char* const decompressed = (char*) malloc(compressedSize * 2);
            if (decompressed == nullptr)
                throw std::logic_error("not implemented");// TODO
            const int decompressedSize = LZ4_decompress_safe(compressed, decompressed, compressedSize, outBuf.getCapacity());
            if (decompressedSize < 0)
                throw std::logic_error("not implemented");// TODO
            memcpy(baseDstPointer + offset, decompressed, decompressedSize);
            i++;
        }
    }
    static void Snappy() { throw std::logic_error("not implemented"); }
    // RowLayout
    static void RLE(DynamicTupleBuffer inBuf, DynamicTupleBuffer outBuf) {
        uint8_t* baseSrcPointer = inBuf.getBuffer().getBuffer();
        uint8_t* baseDstPointer = outBuf.getBuffer().getBuffer();
        const char* in = reinterpret_cast<const char*>(baseSrcPointer);
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
        memcpy(baseDstPointer, temp.c_str(), temp.size());// TODO handle case if buffer is too small
    }
    // ColumnLayout
    static void RLE(DynamicTupleBuffer inBuf, DynamicTupleBuffer outBuf, const std::vector<uint64_t>& offsets) {
        uint8_t* baseSrcPointer = inBuf.getBuffer().getBuffer();
        uint8_t* baseDstPointer = outBuf.getBuffer().getBuffer();
        for (auto offset : offsets) {
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
    }

  private:
    static bool alphaOrSpace(const char c) { return isalpha(c) || c == ' '; }
};

//===--------------------------------------------------------------------===//
// OLD Snappy
//===--------------------------------------------------------------------===//
//void CompressSnapp(std::string inp, std::string outp) { snappy::Compress(inp.c_str(), inp.size(), &outp); }

}// namespace NES::Runtime::MemoryLayouts
#endif//NES_RUNTIME_INCLUDE_RUNTIME_MEMORYLAYOUT_COMPRESSIONALGORITHMS_HPP
