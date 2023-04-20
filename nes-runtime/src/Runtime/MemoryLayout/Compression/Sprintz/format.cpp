//
//  format.cpp
//  Compress
//
//  Created by DB on 12/5/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#include "Runtime/MemoryLayout/Compression/Sprintz/format.h"

// static const uint16_t kMetaDataLenBytesRle = 8;
// static const uint16_t kMetaDataLenBytesSimple = 6;

// ------------------------------------------------ 8b wrappers

uint16_t write_metadata_rle_8b(int8_t* orig_dest, uint16_t ndims,
    uint32_t ngroups, uint16_t remaining_len)
{
    return write_metadata_rle(orig_dest, ndims, ngroups, remaining_len);
}
uint16_t read_metadata_rle_8b(const int8_t* src, uint16_t* p_ndims,
    uint32_t* p_ngroups, uint16_t* p_remaining_len)
{
    return read_metadata_rle(src, p_ndims, p_ngroups, p_remaining_len);
}

uint16_t write_metadata_simple_8b(int8_t* orig_dest, uint16_t ndims,
    uint32_t len)
{
    return write_metadata_simple(orig_dest, ndims, len);
}
uint16_t read_metadata_simple_8b(const int8_t* src, uint16_t* p_ndims,
    uint32_t* p_len)
{
    return read_metadata_simple(src, p_ndims, p_len);
}

uint16_t write_metadata_simple1d_8b(int8_t* orig_dest, uint32_t len) {
    return write_metadata_simple1d(orig_dest, len);
}
uint16_t read_metadata_simple1d_8b(const int8_t* src, uint32_t* p_len) {
    return read_metadata_simple1d(src, p_len);
}

// ------------------------------------------------ 16b wrappers

uint16_t write_metadata_rle_16b(int16_t* orig_dest, uint16_t ndims,
    uint32_t ngroups, uint16_t remaining_len)
{
    return write_metadata_rle(orig_dest, ndims, ngroups, remaining_len);
}
uint16_t read_metadata_rle_16b(const int16_t* src, uint16_t* p_ndims,
    uint32_t* p_ngroups, uint16_t* p_remaining_len)
{
    return read_metadata_rle(src, p_ndims, p_ngroups, p_remaining_len);
}

uint16_t write_metadata_simple_16b(int16_t* orig_dest, uint16_t ndims,
    uint32_t len)
{
    return write_metadata_simple(orig_dest, ndims, len);
}
uint16_t read_metadata_simple_16b(const int16_t* src, uint16_t* p_ndims,
    uint32_t* p_len)
{
    return read_metadata_simple(src, p_ndims, p_len);
}

uint16_t write_metadata_simple1d_16b(int16_t* orig_dest, uint32_t len) {
    return write_metadata_simple1d(orig_dest, len);
}
uint16_t read_metadata_simple1d_16b(const int16_t* src, uint32_t* p_len) {
    return read_metadata_simple1d(src, p_len);
}

// #undef DIV_ROUND_UP
