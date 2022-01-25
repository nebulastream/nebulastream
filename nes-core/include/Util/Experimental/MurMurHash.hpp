/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_MURMURHASH_HPP_
#define NES_NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_MURMURHASH_HPP_
#include <Util/Experimental/Hash.hpp>
namespace NES::Experimental {

class MurMurHash : public Hash<MurMurHash> {
  public:
    inline hash_t hashKey(uint64_t k) const { return hashKey(k, 0); }
    inline hash_t hashKey(uint64_t k, hash_t seed) const {
        // MurmurHash64A
        const uint64_t m = 0xc6a4a7935bd1e995;
        const int r = 47;
        uint64_t h = seed ^ 0x8445d61a4e774912 ^ (8 * m);
        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
        h ^= h >> r;
        h *= m;
        h ^= h >> r;
        return h;
    }

    hash_t hashKey(const void* key, int len, hash_t seed) const {
        // MurmurHash64A
        // MurmurHash2, 64-bit versions, by Austin Appleby
        // https://github.com/aappleby/smhasher/blob/master/src/MurmurHash2.cpp
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        const uint64_t m = 0xc6a4a7935bd1e995;
        const int r = 47;

        uint64_t h = seed ^ (len * m);

        const uint64_t* data = (const uint64_t*) key;
        const uint64_t* end = data + (len / 8);

        while (data != end) {
            uint64_t k = *data++;

            k *= m;
            k ^= k >> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        const unsigned char* data2 = (const unsigned char*) data;

        switch (len & 7) {
            case 7: h ^= uint64_t(data2[6]) << 48;
            case 6: h ^= uint64_t(data2[5]) << 40;
            case 5: h ^= uint64_t(data2[4]) << 32;
            case 4: h ^= uint64_t(data2[3]) << 24;
            case 3: h ^= uint64_t(data2[2]) << 16;
            case 2: h ^= uint64_t(data2[1]) << 8;
            case 1: h ^= uint64_t(data2[0]); h *= m;
        };

        h ^= h >> r;
        h *= m;
        h ^= h >> r;

        return h;
    }
};

}
#endif//NES_NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_MURMURHASH_HPP_
