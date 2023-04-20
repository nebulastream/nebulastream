//
//  sprintz_xff.h
//  Compress
//
//  Created by DB on 7/3/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#ifndef sprintz_xff_h
#define sprintz_xff_h

#include <stdint.h>

#include "macros.h"
#include "query.hpp"

#define USE_X86_INTRINSICS
#define USE_AVX2

#ifdef USE_AVX2
    static_assert(__AVX2__, "AVX 2 is required! Try --march=native or -mavx2");
    #define USE_X86_INTRINSICS
#endif

// ------------------------ just xff

// 8b
int64_t compress8b_rowmajor_xff(const uint8_t* src, uint64_t len,
    int8_t* dest, uint16_t ndims, bool write_size=true);

int64_t decompress8b_rowmajor_xff(const int8_t* src, uint8_t* dest);


// 16b
int64_t compress_rowmajor_xff_16b(const uint8_t* src, uint32_t len,
    int8_t* dest, uint16_t ndims, bool write_size=true);

int64_t decompress_rowmajor_xff_16b(const int16_t* src, uint16_t* dest);

// ------------------------ xff + run length encoding

// 8b
int64_t compress_rowmajor_xff_rle_8b(const uint8_t* src, uint32_t len,
    int8_t* dest, uint16_t ndims, bool write_size=true);

SPRINTZ_FORCE_INLINE int64_t decompress_rowmajor_xff_rle_8b(
    const int8_t* src, uint8_t* dest, uint16_t ndims, uint32_t ngroups,
    uint16_t remaining_len);

int64_t decompress_rowmajor_xff_rle_8b(const int8_t* src, uint8_t* dest);

// 16b
int64_t compress_rowmajor_xff_rle_16b(const uint16_t* src, uint32_t len,
    int16_t* dest, uint16_t ndims, bool write_size=true);

SPRINTZ_FORCE_INLINE int64_t decompress_rowmajor_xff_rle_16b(
    const int16_t* src, uint16_t* dest, uint16_t ndims, uint32_t ngroups,
    uint16_t remaining_len);

int64_t decompress_rowmajor_xff_rle_16b(const int16_t* src, uint16_t* dest);


// ------------------------ xff + rle low dimensional

// 8b
int64_t compress_rowmajor_xff_rle_lowdim_8b(const uint8_t* src, uint32_t len,
    int8_t* dest, uint16_t ndims, bool write_size=true);

SPRINTZ_FORCE_INLINE int64_t decompress_rowmajor_xff_rle_lowdim_8b(
    const int8_t* src, uint8_t* dest, uint16_t ndims, uint32_t ngroups,
    uint16_t remaining_len);

int64_t decompress_rowmajor_xff_rle_lowdim_8b(const int8_t* src, uint8_t* dest);

// 16b
int64_t compress_rowmajor_xff_rle_lowdim_16b(const uint16_t* src, uint32_t len,
    int16_t* dest, uint16_t ndims, bool write_size=true);

SPRINTZ_FORCE_INLINE int64_t decompress_rowmajor_xff_rle_lowdim_16b(
    const int16_t* src, uint16_t* dest, uint16_t ndims, uint32_t ngroups,
    uint16_t remaining_len);

int64_t decompress_rowmajor_xff_rle_lowdim_16b(const int16_t* src, uint16_t* dest);


// ------------------------------------------------ querying

// run queries directly on compressed data
// operates directly on compressed data
int64_t query_rowmajor_xff_rle_8b(const int8_t* src, uint8_t* dest,
    const QueryParams& qparams);
int64_t query_rowmajor_xff_rle_16b(const int16_t* src, uint16_t* dest,
    const QueryParams& qparams);

#endif
