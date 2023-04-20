//
//  delta.hpp
//  Compress
//
//  Created by DB on 11/1/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#ifndef delta_hpp
#define delta_hpp

#include <stdint.h>

// ------------------------------------------------ 8b

// delta coding
uint32_t encode_delta_rowmajor_8b(const uint8_t* src, uint32_t len, int8_t* dest,
    uint16_t ndims, bool write_size=true);
uint32_t decode_delta_rowmajor_8b(const int8_t* src, uint32_t len, uint8_t* dest,
    uint16_t ndims);
uint32_t decode_delta_rowmajor_inplace_8b(uint8_t* buff, uint32_t len,
    uint16_t ndims);
uint32_t decode_delta_rowmajor_8b(const int8_t* src, uint8_t* dest);

// // delta coding + run-length encoding
// uint32_t encode_delta_rle_rowmajor(const uint8_t* src, uint32_t len, int8_t* dest,
//     uint16_t ndims, bool write_size=true);
// uint32_t decode_delta_rle_rowmajor(const int8_t* src, uint32_t len, uint8_t* dest,
//     uint16_t ndims);
// uint32_t decode_delta_rle_rowmajor_inplace(uint8_t* buff, uint32_t len,
//     uint16_t ndims);
// uint32_t decode_delta_rle_rowmajor(const int8_t* src, uint8_t* dest);

// double delta coding
uint32_t encode_doubledelta_rowmajor_8b(const uint8_t* src, uint32_t len,
    int8_t* dest, uint16_t ndims, bool write_size=true);
uint32_t decode_doubledelta_rowmajor_8b(const int8_t* src, uint32_t len,
    uint8_t* dest, uint16_t ndims);
uint32_t decode_doubledelta_rowmajor_inplace_8b(uint8_t* buff, uint32_t len,
    uint16_t ndims);
uint32_t decode_doubledelta_rowmajor_8b(const int8_t* src, uint8_t* dest);

// inline int64_t encode_doubledelta_serial(const uint8_t* src, uint32_t len,
//     int8_t* dest, uint16_t lag, bool needs_initial_cpy=true,
//     int8_t* prev_deltas=nullptr);
// inline int32_t decode_doubledelta_serial(const int8_t* src, uint32_t len,
//     uint8_t* dest, uint16_t lag, int8_t* prev_deltas=nullptr);

// ------------------------------------------------ 16b

// delta coding
uint32_t encode_delta_rowmajor_16b(const uint16_t* src, uint32_t len, int16_t* dest,
    uint16_t ndims, bool write_size=true);
uint32_t decode_delta_rowmajor_16b(const int16_t* src, uint32_t len, uint16_t* dest,
    uint16_t ndims);
uint32_t decode_delta_rowmajor_inplace_16b(uint16_t* buff, uint32_t len,
    uint16_t ndims);
uint32_t decode_delta_rowmajor_16b(const int16_t* src, uint16_t* dest);

// double delta coding
uint32_t encode_doubledelta_rowmajor_16b(const uint16_t* src, uint32_t len, int16_t* dest,
    uint16_t ndims, bool write_size=true);
uint32_t decode_doubledelta_rowmajor_16b(const int16_t* src, uint32_t len, uint16_t* dest,
    uint16_t ndims);
uint32_t decode_doubledelta_rowmajor_inplace_16b(uint16_t* buff, uint32_t len,
    uint16_t ndims);
uint32_t decode_doubledelta_rowmajor_16b(const int16_t* src, uint16_t* dest);


#endif /* delta_hpp */
