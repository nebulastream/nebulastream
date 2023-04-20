//
//  format.hpp
//  Compress
//
//  Created by DB on 2017-12-5.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#ifndef format_hpp
#define format_hpp

#include <stdint.h>

#include "util.h"  // just for DIV_ROUND_UP

// template<typename int_t>
// uint16_t write_metadata_rle(int_t* dest, uint16_t ndims, uint32_t ngroups,
//     uint16_t remaining_len);

// template<typename int_t>
// uint16_t read_metadata_rle(const int_t* src, uint16_t* p_ndims,
//     uint64_t* p_ngroups, uint16_t* p_remaining_len);

// template<typename int_t>
// uint16_t write_metadata_simple(int_t* dest, uint16_t ndims, uint32_t len);

// template<typename int_t>
// uint16_t read_metadata_simple(const int_t* src, uint16_t* p_ndims,
//     uint32_t* p_len);

#define kMetaDataLenBytesRle 8
#define kMetaDataLenBytesSimple 6
#define kMetaDataLenBytesSimple1d 4

template<typename int_t>
static inline uint16_t write_metadata_rle(int_t* orig_dest, uint16_t ndims,
    uint32_t ngroups, uint16_t remaining_len)
{
    int8_t* dest8 = (int8_t*)orig_dest;
    *(uint32_t*)dest8 = ngroups;
    *(uint16_t*)(dest8 + 4) = (uint16_t)remaining_len;
    *(uint16_t*)(dest8 + 6) = ndims;

    return DIV_ROUND_UP(kMetaDataLenBytesRle, sizeof(int_t));
}

template<typename int_t>
static inline uint16_t read_metadata_rle(const int_t* src, uint16_t* p_ndims,
    uint32_t* p_ngroups, uint16_t* p_remaining_len)
{
    static const uint8_t elem_sz = sizeof(int_t);
    const int8_t* src8 = (int8_t*)src;
    // static const uint32_t len_nbytes = 4;
    // uint64_t one = 1; // make next line legible
    // uint64_t len_mask = (one << (8 * len_nbytes)) - 1;
    // *p_ngroups = (*(uint64_t*)src) & len_mask;
    *p_ngroups = *(uint32_t*)src;
    *p_remaining_len = (*(uint16_t*)(src8 + 4));
    *p_ndims = (*(uint16_t*)(src8 + 6));

    return DIV_ROUND_UP(kMetaDataLenBytesRle, sizeof(int_t));
}

template<typename int_t>
static inline uint16_t write_metadata_simple(int_t* dest, uint16_t ndims, uint32_t len) {
    static const uint8_t elem_sz = sizeof(int_t);
    uint8_t* dest8 = (uint8_t*)dest;
    *(uint32_t*)dest8 = len;
    *(uint16_t*)(dest8 + 4) = ndims;

    return DIV_ROUND_UP(kMetaDataLenBytesSimple, sizeof(int_t));
}

template<typename int_t>
static inline uint16_t read_metadata_simple(const int_t* src, uint16_t* p_ndims,
    uint32_t* p_len)
{
    static const uint8_t elem_sz = sizeof(int_t);
    uint8_t* src8 = (uint8_t*)src;
    *p_len = *(uint32_t*)src8;
    *p_ndims = *(uint16_t*)(src8 + 4);

    return DIV_ROUND_UP(kMetaDataLenBytesSimple, sizeof(int_t));
    // uint8_t len_nbytes = kMetaDataLenBytesSimple;
    // return (len_nbytes / elem_sz) + ((len_nbytes % elem_sz) > 0);
}

template<typename int_t>
static inline uint16_t write_metadata_simple1d(int_t* dest, uint32_t len)
{
    *(uint32_t*)dest = len;
    return DIV_ROUND_UP(kMetaDataLenBytesSimple1d, sizeof(int_t));
}
template<typename int_t>
static inline uint16_t read_metadata_simple1d(const int_t* src, uint32_t* p_len)
{
    *p_len = *(uint32_t*)src;
    return DIV_ROUND_UP(kMetaDataLenBytesSimple1d, sizeof(int_t));
}

// ------------------------------------------------ 8b wrappers

uint16_t write_metadata_rle_8b(int8_t* dest, uint16_t ndims, uint32_t ngroups,
    uint16_t remaining_len);
uint16_t read_metadata_rle_8b(const int8_t* src, uint16_t* p_ndims,
    uint32_t* p_ngroups, uint16_t* p_remaining_len);

uint16_t write_metadata_simple_8b(int8_t* dest, uint16_t ndims, uint32_t len);
uint16_t read_metadata_simple_8b(const int8_t* src, uint16_t* p_ndims,
    uint32_t* p_len);

uint16_t write_metadata_simple1d_8b(int8_t* dest, uint32_t len);
uint16_t read_metadata_simple1d_8b(const int8_t* src, uint32_t* p_len);

// ------------------------------------------------ 16b wrappers

uint16_t write_metadata_rle_16b(int8_t* dest, uint16_t ndims, uint32_t ngroups,
    uint16_t remaining_len);
uint16_t read_metadata_rle_16b(const int8_t* src, uint16_t* p_ndims,
    uint64_t* p_ngroups, uint16_t* p_remaining_len);

uint16_t write_metadata_simple_16b(int16_t* dest, uint16_t ndims, uint32_t len);
uint16_t read_metadata_simple_16b(const int16_t* src, uint16_t* p_ndims,
    uint32_t* p_len);

uint16_t write_metadata_simple1d_16b(int16_t* dest, uint32_t len);
uint16_t read_metadata_simple1d_16b(const int16_t* src, uint32_t* p_len);

#endif /* format_hpp */
