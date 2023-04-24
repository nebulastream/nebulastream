//
//  entropy.cpp
//  Compress
//
//  Created by DB on 10/24/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#include "Runtime/MemoryLayout/Compression/Sprintz/entropy.hpp"

#include <stdint.h>
#include <string.h> // for memcpy

#ifndef MAX
    #define MAX(x, y) ( ((x) > (y)) ? (x) : (y) )
#endif
#ifndef MIN
    #define MIN(x, y) ( ((x) < (y)) ? (x) : (y) )
#endif


typedef struct _EncodeInfo {
    uint16_t code;
    uint8_t nbits;
} EncodeInfo;

typedef struct _DecodeInfo {
    uint8_t distance;
    uint8_t nbits;
} DecodeInfo;

static const EncodeInfo kDistanceEncode_128_10b[128] {

};
static const DecodeInfo kDistanceDecode_128_10b[1024] {

};

static const EncodeInfo kDistanceEncode_256_10b[256] {

};
// static const DecodeInfo kDistanceDecode_256_10b[1024] {  // TODO uncomment

// };

template<int NumCodes, int MaxBits>
static inline constexpr EncodeInfo lookup_encode([[maybe_unused]] uint16_t index) {
    return EncodeInfo{};
}

template<>
[[maybe_unused]] inline constexpr EncodeInfo lookup_encode<128, 10>(uint16_t index) {
    return kDistanceEncode_128_10b[index];
}
template<>
[[maybe_unused]] inline constexpr EncodeInfo lookup_encode<256, 10>(uint16_t index) {
    return kDistanceEncode_256_10b[index];
}

// template<int NumCodes, int MaxBits>
// static inline constexpr EncodeInfo lookup_decode(uint16_t index) {
//     return EncodeInfo{};
// }

// template<>
// inline constexpr EncodeInfo lookup_decode<128, 10>(uint16_t index) {
//     return kDistanceDecode_128_10b[index];
// }
// template<>
// inline constexpr EncodeInfo lookup_decode<256, 10>(uint16_t index) {
//     return kDistanceDecode_256_10b[index];
// }

// TODO determine max input length such that nothing can overflow here
// TODO func to give you max dest buffer size you could possibly need (should
//  just be ceil(max_nbits / 8. * len))
template<int HistoryLen=128, int PrefixNumBits=2>
uint32_t kayak_encode(const uint8_t* src, int32_t len, uint8_t* dest,
    bool do_copy=true, bool write_params=true)
{
    typedef int32_t idx_t;

    static const uint8_t data_nbits = 8;
    static const uint8_t max_nbits = PrefixNumBits + data_nbits;
    // static const uint32_t M = (1 << max_nbits) - (1 << data_nbits);
    static const uint32_t M = HistoryLen;
    static const uint16_t min_literal = M;

    uint8_t* orig_dest = dest;

    if (write_params) {  // store info about input and encoding
        *(uint32_t*)dest = len;
        dest += 4;
        *(uint16_t*)dest = HistoryLen;
        dest += 2;
        *(uint8_t*)dest = (PrefixNumBits - 1); // XXX must be between 1 and 16
        *(uint8_t*)dest |= ((uint8_t)do_copy) << 7;
        dest += 1;
    }

    idx_t copy_len = 0;
    if (do_copy) { // ensure that we can look back at least M values
        idx_t copy_len = MIN(M, len);
        memcpy(dest, src, copy_len);
        src += copy_len;
        dest += copy_len;
    }

    idx_t remaining_len = len - copy_len;
    idx_t lag = M;
    uint32_t offset_nbits = 0;
    for (idx_t i = 0; i < remaining_len; i++) {
        uint8_t val = src[i];
        idx_t k = i - lag;

        // TODO don't brute force search for next occurence of val
        uint16_t code;
        uint8_t code_nbits;
        for (idx_t idx = k ; idx < i; idx++) {
            if (src[idx] == val) {
                idx_t distance = idx - k;
                lag = i - idx;
                // EncodeInfo info = kDistanceLut_128_10b[distance]; // TODO don't hardcode
                EncodeInfo info = lookup_encode<HistoryLen, PrefixNumBits + 8>(distance);
                code = info.code;
                code_nbits = info.nbits;
                goto found_val;
            }
        }
        for (idx_t idx = i - M; idx < k; idx++) {
            if (src[idx] == val) {
                idx_t distance = (i - k) + (idx - (i - M));
                lag = i - idx;
                // EncodeInfo info = kDistanceLut_128_10b[distance];
                EncodeInfo info = lookup_encode<HistoryLen, PrefixNumBits + 8>(distance);
                code = info.code;
                code_nbits = info.nbits;
                goto found_val;
            }
        }
        // didn't find val
        code = min_literal | val;
        code_nbits = max_nbits;

    found_val:
        // OR this code into the output buffer (which we assume is zeroed)
        uint32_t write_code = ((uint32_t)code) << offset_nbits;
        *(uint32_t*)dest |= write_code;
        offset_nbits += code_nbits;
        dest += offset_nbits / 8;
        offset_nbits = offset_nbits % 8;
    }

    return (uint32_t)(orig_dest - dest);
}

uint32_t kayak_decode(const uint8_t* src, int32_t len, uint8_t* dest,
    uint16_t history_len, uint8_t max_nbits, bool do_copy=true)
{
    typedef int32_t idx_t;

    const uint16_t M = history_len; // abbreviate + match the math
    const uint16_t min_literal = M;
    const uint32_t code_mask = ((uint32_t)1 << max_nbits) - 1;

    // printf("received M = %d\n", M); // TODO rm

    // TODO separate loop for i < M so we don't have to do this memcpy (and
    // can therefore encode really short stuff)
    idx_t copy_len = 0;
    if (do_copy) {
        idx_t copy_len = MIN(M, len);
        memcpy(dest, src, copy_len);
        src += copy_len;
        dest += copy_len;
    }

    const DecodeInfo* decode_lut = kDistanceDecode_128_10b; // TODO don't hardcode this

    idx_t remaining_len = len - copy_len;
    idx_t lag = M;
    uint32_t offset_nbits = 0;
    for (idx_t i = 0; i < remaining_len; i++) {
        uint16_t code = ((*(uint32_t*)src) >> offset_nbits) & code_mask;
        DecodeInfo info = decode_lut[code];
        uint8_t distance = info.distance;

        if (distance < min_literal) {
            lag -= distance;
            if (lag < 0) { lag += M; }
            dest[i] = dest[i - lag];
        } else {
            dest[i] = distance & 0xff; // just strip off prefix bits
        }
        offset_nbits += info.nbits;
        src += offset_nbits / 8;
        offset_nbits = offset_nbits % 8;
    }

    return len;
}

uint32_t kayak_decode(const uint8_t* src, uint8_t* dest) {
    // read info from compressed data
    int32_t len = *(uint32_t*)src;
    src += 4;
    uint16_t history_len = *(uint16_t*)src;
    src += 2;
    uint8_t prefix_nbits = (*(uint8_t*)src & 0x0f) + 1;
    bool did_copy = (*(uint8_t*)src & 0x80) >> 7;
    src += 1;

    uint8_t max_nbits = prefix_nbits + 8;
    return kayak_decode(src, len, dest, history_len, max_nbits, did_copy);
}
