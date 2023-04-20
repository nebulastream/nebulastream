//
//  bitpack.cpp
//  Compress
//
//  Created by DB on 9/16/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

//
//  bitpack.h
//  Compress
//
//  Created by DB on 9/16/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#ifndef bitpack_h
#define bitpack_h

#include "bitpack.h"

#include "immintrin.h" // for pext, pdep
#include "smmintrin.h"  // for _mm_minpos_epu16

#include "debug_utils.hpp" // TODO rm
#include "macros.h"
#include "util.h"

#include <assert.h>

// #define _TILE_BYTE(byte)                                                    \
// (byte << 0 | byte << 8 | byte << 16 | byte << 24 |                          \
// byte << 32 | byte << 40 | byte << 48 | byte << 56)

// #define TILE_BYTE(byte) _TILE_BYTE(((uint64_t)byte))

// #define _TILE_SHORT(short)                                                  \
// (short << 0 | short << 16 | short << 32 | short << 48)

// #define TILE_SHORT(short) _TILE_SHORT(((uint64_t)short))


static const uint8_t _NBITS_COST_I8[256] = {
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 5, 5, 5,
    5, 5, 5, 5, 5, 4, 4, 4, 4, 3, 3, 2, 1, 0, 2, 3, 3, 4, 4, 4, 4, 5, 5,
    5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8};
static const uint8_t* NBITS_COST_I8 = _NBITS_COST_I8 + 128; // so offsets can be signed

static const uint8_t NBITS_COST_U8[256] = {
    0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5,
    5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8};

static const uint8_t NBITS_MASKS_U8[256] = { // note: 7b and 8b both map to 255
     0,   1,   3,   3,   7,   7,   7,   7,  15,  15,  15,  15,  15,
    15,  15,  15,  31,  31,  31,  31,  31,  31,  31,  31,  31,  31,
    31,  31,  31,  31,  31,  31,  63,  63,  63,  63,  63,  63,  63,
    63,  63,  63,  63,  63,  63,  63,  63,  63,  63,  63,  63,  63,
    63,  63,  63,  63,  63,  63,  63,  63,  63,  63,  63,  63, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255
};

static const uint8_t _NBITS_MASKS_I8[256] = { // note: 7b and 8b both map to 255
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255,  63,  63,  63,  63,  63,  63,  63,  63,
    63,  63,  63,  63,  63,  63,  63,  63,  31,  31,  31,  31,  31,
    31,  31,  31,  15,  15,  15,  15,   7,   7,   3,   1,   0,   3,
     7,   7,  15,  15,  15,  15,  31,  31,  31,  31,  31,  31,  31,
    31,  63,  63,  63,  63,  63,  63,  63,  63,  63,  63,  63,  63,
    63,  63,  63,  63, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
   255, 255, 255, 255, 255, 255, 255, 255, 255
};
// so offsets can be signed
static const uint8_t* NBITS_MASKS_I8 = _NBITS_MASKS_I8 + 128;

#define _TILE_BYTE(byte)                                                    \
    (byte << 0 | byte << 8 | byte << 16 | byte << 24 |                      \
    byte << 32 | byte << 40 | byte << 48 | byte << 56)

#define TILE_BYTE(byte) _TILE_BYTE(((uint64_t)byte))

#define _TILE_SHORT(short)                                                  \
    (short << 0 | short << 16 | short << 32 | short << 48)

#define TILE_SHORT(short) _TILE_SHORT(((uint64_t)short))

// this one allows specifying both seven and 8 bits
static const uint64_t kBitpackMasks_any_nbits[9] = {
    0,
    TILE_BYTE(0x01), TILE_BYTE(0x03), TILE_BYTE(0x07), TILE_BYTE(0x0f),
    TILE_BYTE(0x1f), TILE_BYTE(0x3f), TILE_BYTE(0x7f), TILE_BYTE(0xff),
};

// this one has idx 7 indicate 8 bits, so that it's always possible
// to encode enough
// NOTE to self: if you get SIGABRTs, you might be using index 8 here
static const uint64_t kBitpackMasks8[9] = {
    0,
    TILE_BYTE(0x01), TILE_BYTE(0x03), TILE_BYTE(0x07), TILE_BYTE(0x0f),
    TILE_BYTE(0x1f), TILE_BYTE(0x3f), TILE_BYTE(0xff), TILE_BYTE(0xff),
};

static const uint64_t kBitUnpackMasks8[9] = {
    0,
    TILE_BYTE(0x01) << 7, TILE_BYTE(0x03) << 6, TILE_BYTE(0x07) << 5,
    TILE_BYTE(0x0f) << 4, TILE_BYTE(0x1f) << 3, TILE_BYTE(0x3f) << 2,
    TILE_BYTE(0xff) << 0, TILE_BYTE(0xff) << 0,
};

// like above, second-highest value (here 0x7fff) is replaced with highest value
static const uint64_t kBitpackMasks16[17] = {
    0,
    TILE_SHORT(0x01), TILE_SHORT(0x03), TILE_SHORT(0x07), TILE_SHORT(0x0f),
    TILE_SHORT(0x1f), TILE_SHORT(0x3f), TILE_SHORT(0x7f), TILE_SHORT(0xff),
    TILE_SHORT(0x01ff), TILE_SHORT(0x03ff), TILE_SHORT(0x07ff), TILE_SHORT(0x0fff),
    TILE_SHORT(0x1fff), TILE_SHORT(0x3fff), TILE_SHORT(0xffff), TILE_SHORT(0xffff),
};

// uint64_t _prevent_unused_warnings() {
//     uint64_t x = kBitpackMasks8[0];
//     uint64_t y = kBitUnpackMasks8[0];
//     uint64_t z = kBitpackMasks16[0];
//     return x ^ y ^ z;
// }

static const __m256i nbits_to_mask_8b = _mm256_setr_epi8(
    0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0xff,
    0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // unused
    0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0xff,
    0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00); // unused

static const __m256i nbits_to_mask_16b_low = _mm256_setr_epi8(
    0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff,   // 0-8
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,               // 9-15
    0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff,   // 0-8
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff);              // 9-15
static const __m256i nbits_to_mask_16b_high = _mm256_setr_epi8(
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,         // 0-7
    0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0xff,         // 8-15
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,         // 0-7
    0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0xff);        // 8-15


// ------------------------------------------------ Computing needed nbits

//uint8_t needed_nbits_epi16x8_v1(__m128i v) {
//    // TODO even faster to zigzag and sub fromm 255 16 at once, then minpos
//    // on each half
//
//    static __m128i max_vals = _mm_set1_epi16(0xffff);
//
//    // zigzag encode
//    v = _mm_xor_si128(_mm_srai_epi16(v, 15), _mm_slli_epi16(v, 1));
//    // quasi-zigzag encode; lsb is wrong, but doesn't matter
//    // v = _mm_xor_epi16(_mm128_srai_epi16(v, 15), _mm_slli_epi16(v, 1));
//
//    __m128i subbed = _mm_subs_epu16(max_vals, v); // turn max value into min value
//
//    // extract minimum value
//    __m128i minval_and_idx = _mm_minpos_epu16(subbed);
//    uint16_t minval = (uint16_t) _mm_extract_epi16(minval_and_idx, 0);
//    // uint16_t idx = (uint16_t) _mm_extract_epi16(minval_and_idx, 1);
//    uint16_t maxval = 0xffff - minval;
//
//    return maxval ? _bit_scan_reverse(maxval) + 1 : 0; // bsr undefined if input all 0s
//}

static inline uint8_t needed_nbits_epi16x8(__m128i v) {
    int all_zeros = _mm_test_all_zeros(v, v);
    if (all_zeros) { return 0; }

    // get everything negative
    __m128i all_ones = _mm_cmpeq_epi16(v, v);
    v = _mm_xor_si128(v, _mm_cmpgt_epi16(v, all_ones));

    // invert and extract minimum value; now has a leading 0
    __m128i minval_and_idx = _mm_minpos_epu16(v);
    __m128i neg_minval_etc = _mm_xor_si128(minval_and_idx, all_ones);

    // this version uses andn to omit the above xor and mask upper 2B of u32
    uint16_t maxval = (uint16_t) _mm_extract_epi16(neg_minval_etc, 0);
    return 33 - __builtin_clz((uint32_t)maxval); // 33 because treats as uint32

    // this version has to do the xor, but doesn't have to andn
    //    uint32_t maxval = (uint32_t) _mm_extract_epi32(neg_minval_etc, 0);
    //    maxval = (maxval << 16) | (maxval >> 16); // compiles to ROL or ROR
    //    return 17 - __builtin_clz(maxval);
}

static inline uint8_t needed_nbits_epi8x8(const __m128i v) {
    return needed_nbits_epi16x8(_mm_cvtepi8_epi16(v));
}

static inline uint8_t needed_nbits_i16x8(const int16_t* x) {
    __m128i v = _mm_loadu_si128((__m128i*)x);
    return needed_nbits_epi16x8(v);
}

static inline uint8_t needed_nbits_i8x8(const int8_t* x) {
    __m128i v = _mm_loadu_si128((__m128i*)x);
    return needed_nbits_epi8x8(v);
}

static inline uint8_t needed_nbits_i8x8_simple(const int8_t* x) {
    uint8_t max_nbits = NBITS_COST_I8[*x];
    const int8_t* end = x + 8;
    x++;
    for ( ; x < end; x++) {
        max_nbits = MAX(max_nbits, NBITS_COST_I8[*x]);
    }
    return max_nbits;
}
static inline uint8_t needed_nbits_i16x8_simple(const int16_t* x) {
    int16_t val = *x;
    bool all_zeros = val == 0;

    val ^= val >> 15;  // flip bits if negative
    uint8_t min_nlz = __builtin_clz(val);
    const int16_t* end = x + 8;
    x++;
    for ( ; x < end; x++) {
        val = *x;
        all_zeros &= val == 0;
        val ^= val >> 15;
        min_nlz = MIN(min_nlz, __builtin_clz(val));
    }
    return all_zeros ? 0: 33 - min_nlz;
}
static inline uint8_t needed_nbits_u16x8_simple(
    const uint16_t* x, int block_sz=8)
{
    uint16_t val = *x;

    uint8_t min_nlz = __builtin_clz(val);
    const uint16_t* end = x + block_sz;
    x++;
    for ( ; x < end; x++) {
        val = *x;
        min_nlz = MIN(min_nlz, __builtin_clz(val));
    }
    uint8_t ret = 32 - min_nlz;
    return ret == 15 ? 16 : ret; // count 15 bits as 16 bits
}

// ------------------------------------------------ zigzag

// static inline uint8_t zigzag_encode_i16(int8_t x) {
//     return (x << 1) ^ (x >> 7);
// }

// static inline int8_t zigzag_decode_i16(uint8_t x) {
//     return (x >> 1) ^ -(x & 0x01);
// }

// NOTE to self: this will yield subtlely wrong behavior if the
// expression (x) has side effects; use the functions below instead for
// new code
#define ZIGZAG_ENCODE_SCALAR(x) ( ((x) << 1) ^ ((x) >> (8 * sizeof(x) - 1)) )
#define ZIGZAG_DECODE_SCALAR(x) ( ((x) >> 1) ^ -((x) & 0x01) )

static inline uint8_t zigzag_encode_8b(int8_t x) {
    return (x << 1) ^ (x >> 7);
}
static inline int8_t zigzag_decode_8b(uint8_t x) {
    return (x >> 1) ^ -(x & 0x01);
}

static inline uint16_t zigzag_encode_16b(int16_t x) {
    return (x << 1) ^ (x >> 15);
}
static inline int16_t zigzag_decode_16b(uint16_t x) {
    return (x >> 1) ^ -(x & 0x01);
}

// __attribute__((always_inline)) + static in header is only thing that seems
// to actually force it to get inlined
// extern inline __m256i mm256_zigzag_encode_epi8(const __m256i& x) {
SPRINTZ_FORCE_INLINE static __m256i mm256_zigzag_encode_epi8(const __m256i& x) {
    static const __m256i zeros = _mm256_setzero_si256();
    static const __m256i ones = _mm256_set1_epi8(1);
    __m256i invert_mask = _mm256_cmpgt_epi8(zeros, x);
    __m256i shifted = _mm256_andnot_si256(ones, _mm256_slli_epi64(x, 1));
    return _mm256_xor_si256(invert_mask, shifted);
}

// extern inline __m256i mm256_zigzag_decode_epi8(const __m256i& x) {
SPRINTZ_FORCE_INLINE static __m256i mm256_zigzag_decode_epi8(const __m256i& x) {
    static const __m256i zeros = _mm256_setzero_si256();
    static const __m256i high_bits_one = _mm256_set1_epi8(-128);
    __m256i shifted = _mm256_andnot_si256(
        high_bits_one, _mm256_srli_epi64(x, 1));
    __m256i invert_mask = _mm256_cmpgt_epi8(zeros, _mm256_slli_epi64(x, 7));
    return _mm256_xor_si256(invert_mask, shifted);
}

// TODO uncomment and impl these functions
SPRINTZ_FORCE_INLINE static __m256i mm256_zigzag_encode_epi16(const __m256i& x)
{
    static const __m256i zeros = _mm256_setzero_si256();
    static const __m256i ones = _mm256_set1_epi16(1);
    __m256i invert_mask = _mm256_cmpgt_epi16(zeros, x);
    __m256i shifted = _mm256_andnot_si256(ones, _mm256_slli_epi64(x, 1));
    return _mm256_xor_si256(invert_mask, shifted);
}

SPRINTZ_FORCE_INLINE static __m256i mm256_zigzag_decode_epi16(const __m256i& x)
{
    static const __m256i zeros = _mm256_setzero_si256();
    static const __m256i high_bits_one = _mm256_set1_epi16(-128*256);
    __m256i shifted = _mm256_andnot_si256(
        high_bits_one, _mm256_srli_epi64(x, 1));
    __m256i invert_mask = _mm256_cmpgt_epi16(zeros, _mm256_slli_epi64(x, 15));
    return _mm256_xor_si256(invert_mask, shifted);
}

// ------------------------------------------------ horz bit packing
// (These functions are basically for debugging / validating bitpacking consts)

static inline uint64_t compress8b_bitpack(const uint8_t* src, uint64_t in_sz, uint8_t* dest,
                            uint8_t nbits)
{
    static const int block_sz = 8;
    // static const int group_sz = 4;

    if (nbits == 0) { return 0; }

    // assert(in_sz % block_sz == 0);
    uint64_t nblocks = in_sz / block_sz;

    uint8_t* orig_dest = dest;
    uint64_t mask = kBitpackMasks_any_nbits[nbits];
    // dest[0] = nbits;

    // std::cout << "using nbits: " << (uint16_t)nbits << "\n";
    // std::cout << "using pext mask: ";
    // dumpBits(mask);

    // actually, no; vectorizing this part is nontrivial because what
    // we're writing is only a weird number of bytes; what we should
    // actually do is handle each nbits case separately, if we bother
    // optimizing this futher at all
    //
    // __m256i fourVals = _mm256_setzero_si256();
    // for (uint64_t b = 0; b < nblocks; b += 4) {
    //     uint64_t data0 = *(uint64_t*)(&src[block_sz*(b + 0)]);
    //     uint64_t data1 = *(uint64_t*)(&src[block_sz*(b + 1)]);
    //     uint64_t data2 = *(uint64_t*)(&src[block_sz*(b + 2)]);
    //     uint64_t data3 = *(uint64_t*)(&src[block_sz*(b + 3)]);
    //     uint64_t packed0 = _pext_u64(data0, mask);
    //     uint64_t packed1 = _pext_u64(data1, mask);
    //     uint64_t packed2 = _pext_u64(data2, mask);
    //     uint64_t packed3 = _pext_u64(data3, mask);

    //     dest += nbits;
    // }

    for (uint64_t b = 0; b < nblocks; b++) {
        uint64_t data = *(uint64_t*)(src);
        uint64_t packed = _pext_u64(data, mask);
        *((uint64_t*)dest) = packed;
        dest += nbits;
        src += block_sz;
    }

    size_t remaining_len = in_sz % block_sz;
    memcpy(dest, src, remaining_len);

    return dest + remaining_len - orig_dest;
}
static inline uint64_t decompress8b_bitpack(const uint8_t* src, uint64_t in_sz, uint8_t* dest,
                              uint8_t nbits)
{
    static const int block_sz = 8;
    static const int group_sz = 0; // set this to 0 to not partially unroll loops

    if (nbits == 0) { return 0; }

    // assert(in_sz % nbits == 0);

    uint64_t nblocks = in_sz / nbits;

//    uint64_t ngroups = 0;
//    if (group_sz > 0) {
//        ngroups = nblocks / group_sz;
//    }
    auto safe_group_sz = group_sz > 0 ? group_sz : 1; // avoid UB
    uint64_t ngroups = group_sz > 0 ? nblocks / safe_group_sz : 0;

    uint8_t* orig_dest = dest;
    uint64_t mask = kBitpackMasks_any_nbits[nbits];

    // TODO if more complicated stuff doesn't help (which it doesn't look
    // like it's going to), revert to simplest impl

#define MAIN_LOOP(nbits)                                                    \
    for (uint64_t g = 0; g < ngroups; g++) {                                \
        for (int b = 0; b < group_sz; b++) {                                \
            uint64_t unpacked = _pdep_u64(*(uint64_t*)src, mask);           \
            *((uint64_t*)(dest + (b * block_sz))) = unpacked;               \
            src += nbits;                                                   \
        }                                                                   \
        src += nbits * group_sz;                                            \
        dest += block_sz * group_sz;                                        \
    }                                                                       \
    for (uint64_t b = ngroups * group_sz * block_sz; b < nblocks; b++) {    \
        uint64_t unpacked = _pdep_u64(*(uint64_t*)src, mask);               \
        *((uint64_t*)dest) = unpacked;                                      \
        src += nbits;                                                       \
        dest += block_sz;                                                   \
    }

#ifdef SWITCH_ON_NBITS
    switch(nbits) {
        case 1: MAIN_LOOP(1); break;
        case 2: MAIN_LOOP(2); break;
        case 3: MAIN_LOOP(3); break;
        case 4: MAIN_LOOP(4); break;
        case 5: MAIN_LOOP(5); break;
        case 6: MAIN_LOOP(6); break;
        case 7: MAIN_LOOP(7); break;
        case 8: MAIN_LOOP(8); break;
        default: break;
    }
#else
    // MAIN_LOOP(nbits);
#undef MAIN_LOOP

    #ifdef VECTOR_STORES
        __m256i fourVals = _mm256_setzero_si256();
        for (uint64_t g = 0; g < ngroups; g++) {
            for (int b = 0; b < group_sz; b++) {
                uint64_t unpacked = _pdep_u64(*(uint64_t*)(src + b * nbits), mask);
                fourVals = _mm256_insert_epi64(fourVals, unpacked, b);
                // src += nbits;
            }
            _mm256_storeu_si256((__m256i*)dest, fourVals);
            src += nbits * group_sz;
            dest += block_sz * group_sz;
        }
    #else
        //    __m256i fourVals = _mm256_setzero_si256();
        for (uint64_t g = 0; g < ngroups; g++) {
            for (int b = 0; b < group_sz; b++) {
                uint64_t unpacked = _pdep_u64(*(uint64_t*)(src + b * nbits), mask);
                *((uint64_t*)(dest + (b * block_sz))) = unpacked;
                // src += nbits;
            }
            src += nbits * group_sz;
            dest += block_sz * group_sz;
        }
    #endif
    for (uint64_t b = ngroups * group_sz * block_sz; b < nblocks; b++) {
        uint64_t unpacked = _pdep_u64(*(uint64_t*)src, mask);
        *((uint64_t*)dest) = unpacked;
        src += nbits;
        dest += block_sz;
    }
#endif
        // return dest - orig_dest;

        size_t orig_len = (in_sz * 8) / nbits;
        size_t remaining_len = orig_len % block_sz;
        memcpy(dest, src, remaining_len);

        return dest + remaining_len - orig_dest;
    }

// #undef MAX
// #undef MIN

#endif /* bitpack_h */
