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

#ifndef NES_NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_MURMURHASH3_HPP_
#define NES_NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_MURMURHASH3_HPP_
#include <Util/Experimental/Hash.hpp>
namespace NES::Experimental {

inline uint32_t rotl32(uint32_t x, int8_t r) { return (x << r) | (x >> (32 - r)); }
inline uint64_t rotl64(uint64_t x, int8_t r) { return (x << r) | (x >> (64 - r)); }

FORCE_INLINE uint32_t fmix32(uint32_t h) {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

#define ROTL32(x, y) rotl32(x, y)
#define ROTL64(x, y) rotl64(x, y)

// 32-bit hashes
class MurMurHash3 : public Hash<MurMurHash3> {
  public:
    inline auto hashKey(uint32_t k, hash_t seed) const {
        uint32_t h1 = seed;
        const uint32_t c1 = 0xcc9e2d51;
        const uint32_t c2 = 0x1b873593;

        uint32_t k1 = k;

        k1 *= c1;
        k1 = ROTL32(k1, 15);
        k1 *= c2;

        h1 ^= k1;
        h1 = ROTL32(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;

        return fmix32(h1);
    }
    inline hash_t hashKey(uint64_t k) const { return hashKey(k, 0); }

    inline hash_t hashKey(uint64_t k, hash_t seed) const {
        // inline hash_t hashKey(uint64_t k, hash_t seed) const {
        uint32_t h1 = seed;
        const uint32_t c1 = 0xcc9e2d51;
        const uint32_t c2 = 0x1b873593;

        uint32_t k1 = k;
        uint32_t k2 = k >> 32;

        k1 *= c1;
        k1 = ROTL32(k1, 15);
        k1 *= c2;

        h1 ^= k1;
        h1 = ROTL32(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;

        k2 *= c1;
        k1 = ROTL32(k1, 15);
        k2 *= c2;

        h1 ^= k2;
        h1 = ROTL32(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;

        return fmix32(h1);
    }
    FORCE_INLINE uint32_t getblock32(const uint32_t* p, int i) const { return p[i]; }
    hash_t hashKey(const void* key, int len, hash_t seed) const {
        // from: https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
        const uint8_t* data = (const uint8_t*) key;
        const int nblocks = len / 4;

        uint32_t h1 = seed;

        const uint32_t c1 = 0xcc9e2d51;
        const uint32_t c2 = 0x1b873593;

        //----------
        // body

        const uint32_t* blocks = (const uint32_t*) (data + nblocks * 4);

        for (int i = -nblocks; i; i++) {
            uint32_t k1 = getblock32(blocks, i);

            k1 *= c1;
            k1 = ROTL32(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = ROTL32(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        //----------
        // tail

        const uint8_t* tail = (const uint8_t*) (data + nblocks * 4);

        uint32_t k1 = 0;

        switch (len & 3) {
            case 3: k1 ^= tail[2] << 16;
            case 2: k1 ^= tail[1] << 8;
            case 1:
                k1 ^= tail[0];
                k1 *= c1;
                k1 = ROTL32(k1, 15);
                k1 *= c2;
                h1 ^= k1;
        };

        //----------
        // finalization

        h1 ^= len;

        h1 = fmix32(h1);

        return h1;
    }
};

}// namespace NES::Experimental

#endif//NES_NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_MURMURHASH3_HPP_
