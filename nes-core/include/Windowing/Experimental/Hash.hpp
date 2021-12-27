#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_HASH_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_HASH_HPP_

#include <x86intrin.h>
namespace NES::Experimental {


template<typename CHILD>
class Hash {

    const CHILD* impl() const { return static_cast<const CHILD*>(this); }

  public:
    static const uint64_t SEED = 902850234;
    using hash_t = std::uint64_t;
    /// Integers
    inline hash_t operator()(uint64_t x, hash_t seed) const { return impl()->hashKey(x, seed); }
    inline hash_t operator()(int64_t x, hash_t seed) const { return impl()->hashKey(x, seed); }
    inline hash_t operator()(uint32_t x, hash_t seed) const { return impl()->hashKey(x, seed); }
    inline hash_t operator()(int32_t x, hash_t seed) const { return impl()->hashKey(x, seed); }
    inline hash_t operator()(int8_t x, hash_t seed) const { return impl()->hashKey(x, seed); }

    template<typename... T>
    inline hash_t operator()(std::tuple<T...>&& x, hash_t seed) const {
        hash_t hash = seed;
        auto& self = *impl();
        auto f = [&](auto& x) {
            hash = self(x, hash);
        };
        for_each_in_tuple(x, f);
        return hash;
    }
    template<typename... T>
    inline hash_t operator()(const std::tuple<T...>& x, hash_t seed) const {
        hash_t hash = seed;
        auto& self = *impl();
        auto f = [&](const auto& x) {
            hash = self(x, hash);
        };
        for_each_in_tuple(x, f);
        return hash;
    }
};

class StdHash {
  public:
    using hash_t = size_t;
    /// Integers
    inline hash_t operator()(uint64_t x, hash_t seed) const { return std::hash<decltype(x)>()(x) ^ (seed << 7) ^ (seed >> 2); }
    inline hash_t operator()(int64_t x, hash_t seed) const { return std::hash<decltype(x)>()(x) ^ (seed << 7) ^ (seed >> 2); }
    inline hash_t operator()(uint32_t x, hash_t seed) const { return std::hash<decltype(x)>()(x) ^ (seed << 7) ^ (seed >> 2); }
    inline hash_t operator()(int32_t x, hash_t seed) const { return std::hash<decltype(x)>()(x) ^ (seed << 7) ^ (seed >> 2); }

    /// Pointers
    inline hash_t operator()(void* ptr, hash_t seed) const { return std::hash<void*>()(ptr) ^ (seed << 7) ^ (seed >> 2); }

    template<typename X>
    inline hash_t operator()(const X& x, hash_t seed) const {
        return std::hash<X>()(x) ^ (seed << 7) ^ (seed >> 2);
    }
};

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

inline uint32_t rotl32(uint32_t x, int8_t r) { return (x << r) | (x >> (32 - r)); }
inline uint64_t rotl64(uint64_t x, int8_t r) { return (x << r) | (x >> (64 - r)); }

#define FORCE_INLINE inline __attribute__((always_inline))
#define ROTL32(x, y) rotl32(x, y)
#define ROTL64(x, y) rotl64(x, y)

FORCE_INLINE uint32_t fmix32(uint32_t h) {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

// 32-bit hashes
class MurMurHash3 : public Hash<MurMurHash3> {
  public:
    inline auto hashKey(uint32_t k, hash_t seed) const  {
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
    FORCE_INLINE uint32_t getblock32 ( const uint32_t * p, int i ) const {
        return p[i];
    }
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

#endif// NES_INCLUDE_RUNTIME_TUPLE_BUFFER_HPP_