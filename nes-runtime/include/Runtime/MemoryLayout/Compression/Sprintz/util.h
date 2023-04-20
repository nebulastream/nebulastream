//
//  util.h
//  Compress
//
//  Created by DB on 11/4/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#ifndef util_h
#define util_h

#ifndef MAX
    #define MAX(x, y) ( ((x) > (y)) ? (x) : (y) )
#endif
#ifndef MIN
    #define MIN(x, y) ( ((x) < (y)) ? (x) : (y) )
#endif

#if __cpp_constexpr >= 201304
    #define CONSTEXPR constexpr
#else
    #define CONSTEXPR
#endif

#include <algorithm>  // for std::swap
#include <string.h>

#include "immintrin.h"  // TODO memrep impl without avx2

#define DIV_ROUND_UP(X, Y) ( ((X) / (Y)) + (((X) % (Y)) > 0) )

#define CHECK_INT_UINT_TYPES_VALID(int_t, uint_t)               \
    static_assert(sizeof(uint_t) == sizeof(int_t),              \
        "uint type and int type sizes must be the same!");      \
    static_assert(sizeof(uint_t) == 1 || sizeof(uint_t) == 2,   \
        "Only element sizes of 1 and 2 bytes are supported!");


template<int elem_sz> struct ElemSzTraits {};
template<> struct ElemSzTraits<1> {
    typedef uint64_t bitwidth_t;
    typedef int16_t counter_t;
};
template<> struct ElemSzTraits<2> {
    typedef uint32_t bitwidth_t;
    typedef int32_t counter_t;
};

template<typename T, typename T2>
static CONSTEXPR inline T round_up_to_multiple(T x, T2 multipleof) {
    T remainder = x % multipleof;
    return remainder ? (x + multipleof - remainder) : x;
}

template<typename T, typename T2>
static CONSTEXPR inline auto div_round_up(T x, T2 y) -> decltype(x + y) {
    return (x / y) + ((x % y) > 0);
    // T remainder = x % multipleof;
    // return remainder ? (x + multipleof - remainder) : x;
}


template<typename int_t>
static inline int_t icopysign(int_t sign_of, int_t val) {
    int_t mask = sign_of >> ((8 * sizeof(int_t)) - 1);
    int_t maybe_negated = (val ^ mask) - mask;
    return sign_of != 0 ? maybe_negated : 0; // let compiler optimize this
}

static inline int8_t copysign_i8(int8_t sign_of, int8_t val) {
    int8_t mask = sign_of >> 7; // technically UB, but sane compilers do this
    int8_t maybe_negated = (val ^ mask) - mask;
    return sign_of != 0 ? maybe_negated : 0; // let compiler optimize this
}

/* Allocate aligned memory in a portable way.
 *
 * Memory allocated with aligned alloc *MUST* be freed using _aligned_free.
 *
 * @param alignment The number of bytes to which memory must be aligned. This
 *  value *must* be <= 255.
 * @param bytes The number of bytes to allocate.
 * @param zero If true, the returned memory will be zeroed. If false, the
 *  contents of the returned memory are undefined.
 * @returns A pointer to `size` bytes of memory, aligned to an `alignment`-byte
 *  boundary.
 */
static inline void* _aligned_alloc(size_t alignment, size_t size, bool zero) {
    size_t request_size = size + alignment;
    char* buf = (char*)(zero ? calloc(1, request_size) : malloc(request_size));

    uint64_t remainder = ((uint64_t)buf) % alignment;
    size_t offset = alignment - remainder;
    // if (offset == 0) { offset += alignment; }
    char* ret = buf + (unsigned char)offset;

    // store how many extra bytes we allocated in the byte just before the
    // pointer we return
    *(unsigned char*)(ret - 1) = offset;

    return (void*)ret;
}

/* Free memory allocated with _aligned_alloc */
static inline void _aligned_free(void* aligned_ptr) {
    int offset = *(((char*)aligned_ptr) - 1);
    free(((char*)aligned_ptr) - offset);
}

template<typename DataT, int AlignBytes=32>
class aligned_array {
public:

    explicit aligned_array(size_t size=0, bool zero=true):
        _data(size ?
            (DataT*)_aligned_alloc(AlignBytes, size * sizeof(DataT), zero) :
            nullptr),
        _size(size)
    {}
    aligned_array(aligned_array&& other) noexcept :
        aligned_array()
    {
        swap(*this, other);
    }
    //     _data(std::move(other._data)),
    //     _size(other.size)
    // {}
    aligned_array& operator=(aligned_array other) {
        swap(*this, other);
        return *this;
    }

    ~aligned_array() {
        // printf("freeing aligned array...");
        if (_data != nullptr) { _aligned_free(_data); }
        // printf("freed aligned array!");
    }

    DataT& operator[](size_t idx) { return _data[idx]; }
    const DataT& operator[](size_t idx) const { return _data[idx]; }

    size_t size() const { return _size; }
    DataT* data() { return _data; }
    const DataT* data() const { return _data; }

    friend void swap(aligned_array& first, aligned_array& second) {
        using std::swap;
        swap(first._size, second._size);
        swap(first._data, second._data);
    }

private:
    DataT* _data;
    size_t _size;
};

/**
 * Extends _mm256_shuffle_epi to map 4bit indices to 2B values
 *
 * To do this, the low bytes and high bytes of the values
 * must be passed in as separate tables. Returns the values associated
 * with the low 128b lane as out0, and with the high 128b lane as out1
 *
 * Conceptually, this emulates returning one 512-element vector of epi16s
 * where:
 *      `out[i] = table[idxs[i]]`
 * with `out` the full output vector and `table` a 16 entry table of 16b values.
 *
 * Example (with 4B vectors, 2B lanes):
 *
 * tbl_low  = [0, 1, 2, 3 | 0, 1, 2, 3]
 * tbl_high = [0, 0, 4, 4 | 0, 0, 4, 4]
 * idxs     = [1, 0, 2, 3 | 0, 2, 2, 1]
 *
 * out0 = [(1 + 0 << 8), (0 + 0 << 8), (2 + 4 << 8), (3 + 4 << 8)]
 * out1 = [(0 + 0 << 8), (2 + 4 << 8), (2 + 4 << 8), (1 + 0 << 8)]
 *
 * @param tbl_low Low bytes of the entries in the notional 16b table
 * @param tbl_high High bytes of the entries in the notional 16b table
 * @param idxs The indices to look up in the table (like normal shuffle)
 * @param out0 Epi16 array of values associated with the low 128b of `idxs`
 * @param out1 Epi16 array of values associated with the high 128b of `idxs`
 */
static inline void mm256_shuffle_epi8_to_epi16(const __m256i& tbl_low,
    const __m256i& tbl_high, const __m256i& idxs, __m256i& out0, __m256i& out1)
{
    __m256i vals_low = _mm256_shuffle_epi8(tbl_low, idxs);
    __m256i vals_high = _mm256_shuffle_epi8(tbl_high, idxs);
    __m256i first_third_u64s = _mm256_unpacklo_epi8(vals_low, vals_high);
    __m256i second_fourth_u64s = _mm256_unpackhi_epi8(vals_low, vals_high);
    out0 = _mm256_permute2x128_si256(
        first_third_u64s, second_fourth_u64s, 0 + (2 << 4));
    out1 = _mm256_permute2x128_si256(
        first_third_u64s, second_fourth_u64s, 1 + (3 << 4));
}

inline void memrep(void* dest_, const void* src_, int32_t in_nbytes,
                    int32_t ncopies)
// inline void memrep(uint8_t* dest, const uint8_t* src, int32_t in_nbytes,
//                     int32_t ncopies)
{
    if (in_nbytes < 1 || ncopies < 1) { return; }
    uint8_t* dest = (uint8_t*)dest_;
    const uint8_t* src = (const uint8_t*)src_;

    static const int vector_sz = 32;

    // uint8_t* orig_dest = dest;

    // printf("------------------------\n");
    // printf("requested in_nbytes, ncopies = %d, %d\n", in_nbytes, ncopies);

    // ------------------------ nbytes > vector_sz
    if (in_nbytes > vector_sz) {
        size_t nvectors = (in_nbytes / vector_sz) + ((in_nbytes % vector_sz) > 0);
        size_t trailing_nbytes = in_nbytes % vector_sz;
        trailing_nbytes += trailing_nbytes == 0 ? vector_sz : 0;
        for (size_t i = 0; i < ncopies - 1; i++) {
            for (size_t v = 0; v < nvectors - 1; v++) {
                __m256i x = _mm256_loadu_si256(
                    (const __m256i*)(src + v * vector_sz));
                _mm256_storeu_si256((__m256i*)dest, x);
                dest += vector_sz;
            }
            __m256i x = _mm256_loadu_si256(
                (const __m256i*)(src + (nvectors - 1) * vector_sz));
            _mm256_storeu_si256((__m256i*)dest, x);
            dest += trailing_nbytes;
        }
        // for last copy, don't vectorize to avoid writing past end
        memcpy(dest, src, in_nbytes);
        return;
    }

    // ------------------------ 1 <= in_nbytes <= vector_sz

    uint32_t total_nvectors = (ncopies * in_nbytes) / vector_sz;

    // if data fills less than two vectors, just memcpy
    // if (total_nvectors <= (1 << 30)) { // TODO rm after debug
    // if (total_nvectors <= 2) { // TODO uncomment after debug
    if (total_nvectors < 1) {
        for (size_t i = 0; i < ncopies; i++) {
            memcpy(dest, src, in_nbytes);
            dest += in_nbytes;
        }
    }

    // populate a vector with as many copies of the data as possible
    __m256i v;
    switch (in_nbytes) {
    case 1:
        v = _mm256_set1_epi8(*(const uint8_t*)src); break;
    case 2:
        v = _mm256_set1_epi16(*(const uint16_t*)src); break;
    case 3:
        memcpy(dest, src, 3); memcpy(dest + 3, src, 3);
        memcpy(dest + 6, dest, 6); memcpy(dest + 12, dest, 12);
        memcpy(dest + 24, dest, 6); // bytes 24-30
        // memcpy(dest, src, 4); memcpy(dest + 3, src, 4);
        // memcpy(dest + 6, dest, 6); memcpy(dest + 12, dest, 12);
        v = _mm256_loadu_si256((const __m256i*)dest);
        // printf("initial stuff we wrote: "); dump_bytes(dest, 32);
        dest += 8 * in_nbytes;
        ncopies -= 8;
        break;
    case 4:
        v = _mm256_set1_epi32(*(const uint32_t*)src); break;
    case 5:
        memcpy(dest, src, in_nbytes);
        memcpy(dest + in_nbytes, dest, in_nbytes);
        memcpy(dest + 2*in_nbytes, dest, 2*in_nbytes);
        memcpy(dest + 4*in_nbytes, dest, 2*in_nbytes); // bytes 20-30
        v = _mm256_loadu_si256((const __m256i*)dest);
        dest += 6 * in_nbytes;
        ncopies -= 6;
        break;
    case 6:
        memcpy(dest, src, in_nbytes);
        memcpy(dest + in_nbytes, dest, in_nbytes);
        memcpy(dest + 2*in_nbytes, dest, 2*in_nbytes);
        memcpy(dest + 4*in_nbytes, dest, in_nbytes); // bytes 24-30
        v = _mm256_loadu_si256((const __m256i*)dest);
        dest += 5 * in_nbytes;
        ncopies -= 5;
        break;
    case 7:
        memcpy(dest, src, in_nbytes);
        memcpy(dest + in_nbytes, dest, in_nbytes);
        memcpy(dest + 2*in_nbytes, dest, 2*in_nbytes);
        v = _mm256_loadu_si256((const __m256i*)dest);
        dest += 4 * in_nbytes;
        ncopies -= 4;
        break;
    case 8:
        v = _mm256_set1_epi64x(*(const uint64_t*)src); break;
    case 9: case 10:
        memcpy(dest, src, in_nbytes);
        memcpy(dest + in_nbytes, dest, in_nbytes);
        memcpy(dest + 2*in_nbytes, dest, in_nbytes); // third copy
        v = _mm256_loadu_si256((const __m256i*)dest);
        dest += 3 * in_nbytes;
        ncopies -= 3;
        break;
    case 11: case 12: case 13: case 14: case 15:
        memcpy(dest, src, in_nbytes);
        memcpy(dest + in_nbytes, dest, in_nbytes);
        v = _mm256_loadu_si256((const __m256i*)dest);
        dest += 2 * in_nbytes;
        ncopies -= 2;
        break;
    case 16:
        v = _mm256_broadcastsi128_si256(_mm_loadu_si128((const __m128i*)src));
        break;
    case 32:
        v = _mm256_loadu_si256((const __m256i*)src); break;
    default: // 17 through 31
        memcpy(dest, src, in_nbytes);
        v = _mm256_loadu_si256((const __m256i*)dest);
        dest += in_nbytes;
        ncopies -= 1;
        break;
    }

    // printf("vector we're copying: "); dump_m256i(v);

    // compute number of vectors to write (ncopies might have changed) and
    // write them
    total_nvectors = (ncopies * in_nbytes) / vector_sz;
    uint8_t copies_per_vector = vector_sz / in_nbytes;
    uint8_t stride_nbytes = copies_per_vector * in_nbytes;
    for (size_t i = 0; i < total_nvectors; i++) {
        _mm256_storeu_si256((__m256i*)dest, v);
        dest += stride_nbytes;
    }

    // copy remaining bytes using memcpy (to avoid writing past end)
    uint32_t written_ncopies = copies_per_vector * total_nvectors;
    uint32_t remaining_ncopies = ncopies - written_ncopies;
    // printf("total nvectors, copies per vector = %d, %d\n", total_nvectors, copies_per_vector);
    // printf("stride_nbytes, written_ncopies, remaining_ncopies = %d, %d, %d\n", stride_nbytes, written_ncopies, remaining_ncopies);
    for (uint32_t i = 0; i < remaining_ncopies; i++) {
        memcpy(dest, src, in_nbytes);
        dest += in_nbytes;
    }

    // printf("everything we wrote: "); dump_bytes(orig_dest, (int)(dest - orig_dest));
}


#endif /* util_h */
