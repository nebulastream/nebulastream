#ifndef NES_NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_CRC32HASH_HPP_
#define NES_NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_CRC32HASH_HPP_
#include <Util/Experimental/Hash.hpp>
namespace NES::Experimental {

class CRC32Hash : public Hash<CRC32Hash> {
  public:
    inline auto hashKey(uint64_t k, hash_t seed) const {
        // inline hash_t hashKey(uint64_t k, uint64_t seed) const {
        uint64_t result1 = _mm_crc32_u64(seed, k);
        uint64_t result2 = _mm_crc32_u64(0x04c11db7, k);
        return ((result2 << 32) | result1) * 0x2545F4914F6CDD1Dull;
    }
    inline uint64_t hashKey(uint64_t k) const { return hashKey(k, 0); }

    inline uint64_t hashKey(const void* key, int len, uint64_t seed) const {
        auto data = reinterpret_cast<const uint8_t*>(key);
        uint64_t s = seed;
        while (len >= 8) {
            s = hashKey(*reinterpret_cast<const uint64_t*>(data), s);
            data += 8;
            len -= 8;
        }
        if (len >= 4) {
            s = hashKey((uint32_t) * reinterpret_cast<const uint32_t*>(data), s);
            data += 4;
            len -= 4;
        }
        switch (len) {
            case 3: s ^= ((uint64_t) data[2]) << 16;
            case 2: s ^= ((uint64_t) data[1]) << 8;
            case 1: s ^= data[0];
        }
        return s;
    }
};

}// namespace NES::Experimental

#endif//NES_NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_CRC32HASH_HPP_
