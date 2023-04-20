//
//  sprintz_8b.cpp
//  Compress
//
//  Created by DB on 12/4/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#include "Runtime/MemoryLayout/Compression/Sprintz/sprintz.h"

#include <stdio.h>

#include "Runtime/MemoryLayout/Compression/Sprintz/format.h"
#include "Runtime/MemoryLayout/Compression/Sprintz/sprintz_delta.h"
#include "Runtime/MemoryLayout/Compression/Sprintz/sprintz_xff.h"

#define LOW_DIMS_CASE
#define CASE(X)

#define FOUR_CASES(START) \
    CASE((START)); CASE((START+1)); CASE((START+2)); CASE((START+3));

#define SIXTEEN_CASES(START)                                                \
    FOUR_CASES(START); FOUR_CASES(START + 4);                               \
    FOUR_CASES(START + 8); FOUR_CASES(START + 12);

#define CASES_5_AND_UP(DEFAULT_CALL)                                        \
    FOUR_CASES(5); FOUR_CASES(9); FOUR_CASES(13);                           \
    SIXTEEN_CASES(16 + 1); SIXTEEN_CASES(32 + 1); SIXTEEN_CASES(48 + 1);    \
    default:                                                                \
        return (DEFAULT_CALL);

#define SWITCH_ON_NDIMS(NDIMS, DEFAULT_CALL)                                \
    switch (NDIMS) {                                                        \
        case 0: printf("Received invalid ndims %d\n", NDIMS); break;        \
        LOW_DIMS_CASE(1); LOW_DIMS_CASE(2);                                 \
        LOW_DIMS_CASE(3); LOW_DIMS_CASE(4);                                 \
        CASES_5_AND_UP(DEFAULT_CALL)                                        \
    };                                                                      \
    return -1; /* unreachable */

#define SWITCH_ON_NDIMS_16B(NDIMS, DEFAULT_CALL)                            \
    switch (NDIMS) {                                                        \
        case 0: printf("Received invalid ndims %d\n", NDIMS); break;        \
        LOW_DIMS_CASE(1); LOW_DIMS_CASE(2);                                 \
        CASE(3); CASE(4);                                                   \
        CASES_5_AND_UP(DEFAULT_CALL)                                        \
    };                                                                      \
    return -1; /* unreachable */

#undef LOW_DIMS_CASE
#undef CASE

// ================================================================ 8b delta

int64_t sprintz_compress_delta_8b(const uint8_t* src, uint32_t len, int8_t* dest,
                                  uint16_t ndims, bool write_size)
{
    // #undef LOW_DIMS_CASE
    #define LOW_DIMS_CASE(NDIMS)                                    \
        case NDIMS: return compress_rowmajor_delta_rle_lowdim_8b(    \
            src, len, dest, NDIMS, write_size);

    #define CASE(NDIMS)                                             \
        case NDIMS: return compress_rowmajor_delta_rle_8b(           \
            src, len, dest, NDIMS, write_size);

    SWITCH_ON_NDIMS(ndims, compress_rowmajor_delta_rle_8b(
        src, len, dest, ndims, write_size));

    #undef LOW_DIMS_CASE
    #undef CASE
}
int64_t sprintz_decompress_delta_8b(const int8_t* src, uint8_t* dest) {
    uint16_t ndims;
    uint32_t ngroups;
    uint16_t remaining_len;
    src += read_metadata_rle_8b(src, &ndims, &ngroups, &remaining_len);

    #define LOW_DIMS_CASE(NDIMS)                                        \
        case NDIMS: return decompress_rowmajor_delta_rle_lowdim_8b(      \
            src, dest, NDIMS, ngroups, remaining_len);

    #define CASE(NDIMS)                                                 \
        case NDIMS: return decompress_rowmajor_delta_rle_8b(             \
            src, dest, NDIMS, ngroups, remaining_len);

    SWITCH_ON_NDIMS(ndims, decompress_rowmajor_delta_rle_8b(
        src, dest, ndims, ngroups, remaining_len));

    #undef LOW_DIMS_CASE
    #undef CASE
}

// ================================================================ 8b xff

int64_t sprintz_compress_xff_8b(const uint8_t* src, uint32_t len, int8_t* dest,
                                  uint16_t ndims, bool write_size)
{
    #define LOW_DIMS_CASE(NDIMS)                                    \
        case NDIMS: return compress_rowmajor_xff_rle_lowdim_8b(      \
            src, len, dest, NDIMS, write_size);

    #define CASE(NDIMS)                                             \
        case NDIMS: return compress_rowmajor_xff_rle_8b(             \
            src, len, dest, NDIMS, write_size);

    SWITCH_ON_NDIMS(ndims, compress_rowmajor_xff_rle_8b(
        src, len, dest, ndims, write_size));

    #undef LOW_DIMS_CASE
    #undef CASE
}
int64_t sprintz_decompress_xff_8b(const int8_t* src, uint8_t* dest) {
    uint16_t ndims;
    uint32_t ngroups;
    uint16_t remaining_len;
    src += read_metadata_rle_8b(src, &ndims, &ngroups, &remaining_len);

    #define LOW_DIMS_CASE(NDIMS)                                    \
        case NDIMS: return decompress_rowmajor_xff_rle_lowdim_8b(    \
            src, dest, NDIMS, ngroups, remaining_len);

    #define CASE(NDIMS)                                             \
        case NDIMS: return decompress_rowmajor_xff_rle_8b(           \
            src, dest, NDIMS, ngroups, remaining_len);

    SWITCH_ON_NDIMS(ndims, decompress_rowmajor_xff_rle_8b(
        src, dest, ndims, ngroups, remaining_len));

    #undef LOW_DIMS_CASE
    #undef CASE
}

// ================================================================ 16b delta

int64_t sprintz_compress_delta_16b(const uint16_t* src, uint32_t len,
    int16_t* dest, uint16_t ndims, bool write_size)
{
    // #undef LOW_DIMS_CASE
    #define LOW_DIMS_CASE(NDIMS)                                    \
        case NDIMS: return compress_rowmajor_delta_rle_lowdim_16b(  \
            src, len, dest, NDIMS, write_size);

    #define CASE(NDIMS)                                             \
        case NDIMS: return compress_rowmajor_delta_rle_16b(         \
            src, len, dest, NDIMS, write_size);

    SWITCH_ON_NDIMS_16B(ndims, compress_rowmajor_delta_rle_16b(
        src, len, dest, ndims, write_size));

    #undef LOW_DIMS_CASE
    #undef CASE
}
int64_t sprintz_decompress_delta_16b(const int16_t* src, uint16_t* dest) {
    uint16_t ndims;
    uint32_t ngroups;
    uint16_t remaining_len;
    src += read_metadata_rle(src, &ndims, &ngroups, &remaining_len);

    #define LOW_DIMS_CASE(NDIMS)                                        \
        case NDIMS: return decompress_rowmajor_delta_rle_lowdim_16b(    \
            src, dest, NDIMS, ngroups, remaining_len);

    #define CASE(NDIMS)                                                 \
        case NDIMS: return decompress_rowmajor_delta_rle_16b(           \
            src, dest, NDIMS, ngroups, remaining_len);

    SWITCH_ON_NDIMS_16B(ndims, decompress_rowmajor_delta_rle_16b(
        src, dest, ndims, ngroups, remaining_len));

    #undef LOW_DIMS_CASE
    #undef CASE
}


// ================================================================ 16b xff

int64_t sprintz_compress_xff_16b(const uint16_t* src, uint32_t len,
    int16_t* dest, uint16_t ndims, bool write_size)
{
    #define LOW_DIMS_CASE(NDIMS)                                    \
        case NDIMS: return compress_rowmajor_xff_rle_lowdim_16b(    \
            src, len, dest, NDIMS, write_size);

    #define CASE(NDIMS)                                             \
        case NDIMS: return compress_rowmajor_xff_rle_16b(           \
            src, len, dest, NDIMS, write_size);

    SWITCH_ON_NDIMS_16B(ndims, compress_rowmajor_xff_rle_16b(
        src, len, dest, ndims, write_size));

    #undef LOW_DIMS_CASE
    #undef CASE
}
int64_t sprintz_decompress_xff_16b(const int16_t* src, uint16_t* dest) {
    uint16_t ndims;
    uint32_t ngroups;
    uint16_t remaining_len;
    src += read_metadata_rle(src, &ndims, &ngroups, &remaining_len);

    #define LOW_DIMS_CASE(NDIMS)                                    \
        case NDIMS: return decompress_rowmajor_xff_rle_lowdim_16b(  \
            src, dest, NDIMS, ngroups, remaining_len);

    #define CASE(NDIMS)                                             \
        case NDIMS: return decompress_rowmajor_xff_rle_16b(         \
            src, dest, NDIMS, ngroups, remaining_len);

    SWITCH_ON_NDIMS_16B(ndims, decompress_rowmajor_xff_rle_16b(
        src, dest, ndims, ngroups, remaining_len));

    #undef LOW_DIMS_CASE
    #undef CASE
}

