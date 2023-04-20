//
//  predict.hpp
//  Compress
//
//  Created by DB on 11/4/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#ifndef predict_hpp
#define predict_hpp

#include <stdint.h>

// 8b
uint32_t encode_xff_rowmajor_8b(const uint8_t* src, uint32_t len, int8_t* dest,
                                uint16_t ndims, bool write_size=true);
uint32_t decode_xff_rowmajor_8b(const int8_t* src, uint32_t len, uint8_t* dest,
                                uint16_t ndims);
uint32_t decode_xff_rowmajor_inplace_8b(uint8_t* buff, uint32_t len,
                                      uint16_t ndims);
uint32_t decode_xff_rowmajor_8b(const int8_t* src, uint8_t* dest);

// 16b
uint32_t encode_xff_rowmajor_16b(const uint16_t* src, uint32_t len,
    int16_t* dest, uint16_t ndims, bool write_size=true);
uint32_t decode_xff_rowmajor_16b(const int16_t* src, uint32_t len,
    uint16_t* dest, uint16_t ndims);
uint32_t decode_xff_rowmajor_inplace_16b(uint16_t* buff, uint32_t len,
                                         uint16_t ndims);
uint32_t decode_xff_rowmajor_16b(const int16_t* src, uint16_t* dest);

#endif /* predict_hpp */
