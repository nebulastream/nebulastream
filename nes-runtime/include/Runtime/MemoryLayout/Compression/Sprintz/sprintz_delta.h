//
//  sprintz2.h
//  Compress
//
//  Created by DB on 7/3/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#ifndef sprintz_delta_h
#define sprintz_delta_h

#include <stdint.h>

#include "macros.h"
#include "query.hpp"

#define USE_X86_INTRINSICS
#define USE_AVX2

#ifdef USE_AVX2
    static_assert(__AVX2__, "AVX 2 is required! Try --march=native or -mavx2");
    #define USE_X86_INTRINSICS
#endif

// ------------------------ no preprocessing (just bitpacking)

int64_t compress_rowmajor_8b(const uint8_t* src, uint32_t len, int8_t* dest,
                            uint16_t ndims, bool write_size=true);
int64_t decompress_rowmajor_8b(const int8_t* src, uint8_t* dest);

int64_t compress_rowmajor_16b(const uint16_t* src, uint32_t len, int16_t* dest,
                            uint16_t ndims, bool write_size=true);
int64_t decompress_rowmajor_16b(const int16_t* src, uint16_t* dest);


// ------------------------ delta coding only

int64_t compress_rowmajor_delta_8b(const uint8_t* src, uint32_t len, int8_t* dest,
                                  uint16_t ndims, bool write_size=true);
int64_t decompress_rowmajor_delta_8b(const int8_t* src, uint8_t* dest);

int64_t compress_rowmajor_delta_16b(const uint16_t* src, uint32_t len,
    int16_t* dest, uint16_t ndims, bool write_size=true);
int64_t decompress_rowmajor_delta_16b(const int16_t* src, uint16_t* dest);

// ------------------------ delta + run length encoding

// 8b
int64_t compress_rowmajor_delta_rle_8b(const uint8_t* src, uint32_t len,
    int8_t* dest, uint16_t ndims, bool write_size=true);

SPRINTZ_FORCE_INLINE int64_t decompress_rowmajor_delta_rle_8b(
    const int8_t* src, uint8_t* dest, uint16_t ndims, uint32_t ngroups,
    uint16_t remaining_len);

int64_t decompress_rowmajor_delta_rle_8b(const int8_t* src, uint8_t* dest);

// 16b
int64_t compress_rowmajor_delta_rle_16b(const uint16_t* src, uint32_t len,
    int16_t* dest, uint16_t ndims, bool write_size=true);

SPRINTZ_FORCE_INLINE int64_t decompress_rowmajor_delta_rle_16b(
    const int16_t* src, uint16_t* dest, uint16_t ndims, uint32_t ngroups,
    uint16_t remaining_len);

int64_t decompress_rowmajor_delta_rle_16b(const int16_t* src, uint16_t* dest);

// ------------------------ delta + rle low dimensional

// 8b
int64_t compress_rowmajor_delta_rle_lowdim_8b(const uint8_t* src, uint32_t len,
    int8_t* dest, uint16_t ndims, bool write_size=true);

SPRINTZ_FORCE_INLINE int64_t decompress_rowmajor_delta_rle_lowdim_8b(
    const int8_t* src, uint8_t* dest, uint16_t ndims, uint64_t ngroups,
    uint16_t remaining_len);

int64_t decompress_rowmajor_delta_rle_lowdim_8b(
    const int8_t* src, uint8_t* dest);

// 16b
int64_t compress_rowmajor_delta_rle_lowdim_16b(const uint16_t* src, uint32_t len,
    int16_t* dest, uint16_t ndims, bool write_size=true);

SPRINTZ_FORCE_INLINE int64_t decompress_rowmajor_delta_rle_lowdim_16b(
    const int16_t* src, uint16_t* dest, uint16_t ndims, uint64_t ngroups,
    uint16_t remaining_len);

int64_t decompress_rowmajor_delta_rle_lowdim_16b(
    const int16_t* src, uint16_t* dest);

// ------------------------------------------------ querying

// operates directly on compressed data
int64_t query_rowmajor_delta_rle_8b(const int8_t* src, uint8_t* dest,
    const QueryParams& qparams);
int64_t query_rowmajor_delta_rle_16b(const int16_t* src, uint16_t* dest,
    const QueryParams& qparams);


#endif
