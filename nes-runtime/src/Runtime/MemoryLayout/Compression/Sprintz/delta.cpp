//
//  delta.cpp
//  Compress
//
//  Created by DB on 11/1/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#include "Runtime/MemoryLayout/Compression/Sprintz/delta.h"

#include <stdio.h>
#include <assert.h>
#include <string.h>

#include "immintrin.h"

// #include "debug_utils.hpp" // TODO rm
#include "Runtime/MemoryLayout/Compression/Sprintz/format.h"
#include "Runtime/MemoryLayout/Compression/Sprintz/util.h"

// #define CHECK_INT_UINT_TYPES_VALID(uint_t, int_t)           \
//     static_assert(sizeof(uint_t) == sizeof(int_t),          \
//         "uint type and int type sizes must be the same!");


#define CHECK_INT_UINT_TYPES_VALID(int_t, uint_t)               \
    static_assert(sizeof(uint_t) == sizeof(int_t),              \
        "uint type and int type sizes must be the same!");      \
    static_assert(sizeof(uint_t) == 1 || sizeof(uint_t) == 2,   \
        "Only element sizes of 1 and 2 bytes are supported!");  \


template<typename uint_t, typename int_t>
uint32_t encode_delta_rowmajor(const uint_t* src, uint32_t len,
    int_t* dest, uint16_t ndims, bool write_size)
{
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(uint_t);
    static const uint8_t vector_sz = 32 / elem_sz;
    static const uint8_t block_sz = 8;

    int_t* orig_dest = dest;

    uint32_t block_sz_elems = block_sz * ndims;
    uint32_t nrows = len / ndims;
    uint32_t nblocks = nrows / block_sz;
    uint32_t padded_ndims = round_up_to_multiple(ndims, vector_sz);
    uint16_t nvectors = padded_ndims / vector_sz + ((padded_ndims % vector_sz) > 0);

    uint_t* prev_vals_ar = (uint_t*)calloc(vector_sz * elem_sz, nvectors);

    uint16_t metadata_len = 0;
    if (write_size) {
        metadata_len = write_metadata_simple(dest, ndims, len);
        dest += metadata_len;
        orig_dest = dest;
    }

    // printf("-------- compression (len = %lld, ndims = %d)\n", (int64_t)len, ndims);
    // printf("saw original data:\n"); dump_bytes(src, len, ndims);
    // printf("saw original data:\n"); dump_bytes(src, ndims * 8, ndims);

    // nblocks = 0; // TODO rm

    // uint16_t overrun_ndims = ndims % vector_sz;
    // uint32_t npad_bytes = overrun_ndims * block_sz;

    // uint32_t npad_blocks = (npad_bytes / block_sz_elems) + ((npad_bytes % block_sz_elems) > 0);
    // nblocks -= MIN(nblocks, npad_blocks);
    // // if (len < vector_sz * block_sz) { nblocks = 0; }

    // printf("overrun ndims: %d\n", overrun_ndims);

    // ensure we don't write past the end of the output
    uint16_t overrun_ndims = vector_sz - (ndims % vector_sz);
    uint32_t trailing_nelements = len % block_sz_elems;
    if (nblocks > 1 && overrun_ndims > trailing_nelements) { nblocks -= 1; }
    // if (nblocks > 1) { nblocks -= 1; }

    // printf("using nblocks, nvectors: %d, %d\n", nblocks, nvectors);

    // nblocks = 0;

    for (uint32_t b = 0; b < nblocks; b++) { // for each block
        for (int32_t v = nvectors - 1; v >= 0; v--) { // for each stripe
            __m256i* prev_vals_ptr = (__m256i*)(prev_vals_ar + v * vector_sz);
            __m256i prev_vals = _mm256_loadu_si256(prev_vals_ptr);
            for (uint8_t i = 0; i < block_sz; i++) {
                const uint_t* in_ptr = src + i * ndims + v * vector_sz;
                int_t* out_ptr = dest + i * ndims + v * vector_sz;
                __m256i vals = _mm256_loadu_si256((__m256i*)in_ptr);
                __m256i vdeltas = elem_sz == 1 ? // only supports u8, u16
                    _mm256_sub_epi8(vals, prev_vals) :
                    _mm256_sub_epi16(vals, prev_vals);
                _mm256_storeu_si256((__m256i*)out_ptr, vdeltas);
                prev_vals = vals;
            }
            _mm256_storeu_si256((__m256i*)(prev_vals_ptr), prev_vals);
        } // for each vector
        src += block_sz_elems;
        dest += block_sz_elems;
    } // for each block

    // delta code trailing elements serially; note that if we jump straight
    // to this section, we need to not read past the beginning of the input
    if (nblocks == 0) {
        uint32_t cpy_len = MIN(ndims, len);
        memcpy(dest, src, cpy_len * elem_sz);
        dest += ndims;
        src += ndims;
    }
    // printf("copying trailing %d bytes\n", (int)((orig_dest + len) - dest));
    for (; dest < (orig_dest + len); ++dest) {
        *dest = *(src) - *(src - ndims);
        src++;
    }

    free(prev_vals_ar);
    return len + metadata_len;
}
uint32_t encode_delta_rowmajor_8b(const uint8_t* src, uint32_t len,
    int8_t* dest, uint16_t ndims, bool write_size)
{
    return encode_delta_rowmajor(src, len, dest, ndims, write_size);
}
uint32_t encode_delta_rowmajor_16b(const uint16_t* src, uint32_t len,
    int16_t* dest, uint16_t ndims, bool write_size)
{
    return encode_delta_rowmajor(src, len, dest, ndims, write_size);
}

template<int ndims, typename int_t, typename uint_t>
uint32_t decode_delta_rowmajor_small_ndims(const int_t* src, uint32_t len,
    uint_t* dest)
{
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);

    uint_t* orig_dest = dest;

    uint32_t cpy_len = MIN(ndims, len);
    memcpy(dest, src, cpy_len * sizeof(int_t));
    dest += ndims;
    src += ndims;
    for (; dest < (orig_dest + len); ++dest) {
        *dest = *src + *(dest - ndims);
        src++;
    }
    return len;
}

template<typename int_t, typename uint_t>
inline int64_t decode_delta_serial(const int_t* src, uint_t* dest,
    const uint_t* dest_end, uint16_t lag, bool needs_initial_cpy)
{
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);

    int64_t len = (int64_t)(dest_end - dest);
    if (len < 1) { return -1; }
    if (lag < 1) { return -2; }

    if (needs_initial_cpy) {
        int64_t cpy_len = MIN(lag, len);
        memcpy(dest, src, cpy_len * sizeof(int_t));
        dest += lag;
        src += lag;
    }
    for (; dest < dest_end; ++dest) {
        *dest = *src + *(dest - lag);
        src++;
    }
    return len;
}

template<typename int_t, typename uint_t>
uint32_t decode_delta_rowmajor_large_ndims(const int_t* src, uint32_t len,
    uint_t* dest, uint16_t ndims)
{
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(int_t);
    static const uint8_t vector_sz = 32 / elem_sz;
    static const uint8_t block_sz = 8;
    uint_t* orig_dest = dest;

    if (ndims == 0) { return 0; }

    uint32_t block_sz_elems = block_sz * ndims;
    uint32_t nrows = len / ndims;
    uint32_t nblocks = nrows / block_sz;
    uint32_t padded_ndims = round_up_to_multiple(ndims, vector_sz);
    uint16_t nvectors = padded_ndims / vector_sz + ((padded_ndims % vector_sz) > 0);

    uint_t* prev_vals_ar = (uint_t*)calloc(vector_sz * elem_sz, nvectors);

    // ensure we don't write past the end of the output
    uint16_t overrun_ndims = vector_sz - (ndims % vector_sz);
    uint32_t trailing_nelements = len % block_sz_elems;
    if (nblocks > 1 && overrun_ndims > trailing_nelements) { nblocks -= 1; }

    for (uint32_t b = 0; b < nblocks; b++) { // for each block
        const int_t* block_in_ptr = src + (nvectors - 1) * vector_sz;
        uint_t* block_out_ptr = dest + (nvectors - 1) * vector_sz;
        for (int32_t v = nvectors - 1; v >= 0; v--) { // for each stripe
            __m256i* prev_vals_ptr = (__m256i*)(prev_vals_ar + v * vector_sz);
            __m256i prev_vals = _mm256_loadu_si256(prev_vals_ptr);
            const int_t* in_ptr = block_in_ptr;
            uint_t* out_ptr = block_out_ptr;
            for (uint8_t i = 0; i < block_sz; i++) {
                __m256i errs = _mm256_loadu_si256((__m256i*)in_ptr);
                __m256i vals = elem_sz == 1 ? // only supports i8 and i16
                    _mm256_add_epi8(errs, prev_vals) :
                    _mm256_add_epi16(errs, prev_vals);
                _mm256_storeu_si256((__m256i*)out_ptr, vals);
                prev_vals = vals;
                in_ptr += ndims;
                out_ptr += ndims;
            }
            _mm256_storeu_si256((__m256i*)(prev_vals_ptr), prev_vals);
            block_in_ptr -= vector_sz;
            block_out_ptr -= vector_sz;
        } // for each vector
        src += block_sz_elems;
        dest += block_sz_elems;
    } // for each block

    // undo delta coding for trailing elements serially
    decode_delta_serial(src, dest, orig_dest + len, ndims, nblocks == 0);

    free(prev_vals_ar);
    return len;
}

// TODO same body as prev func; maybe put in macro?
template<int ndims, typename int_t, typename uint_t>
uint32_t decode_delta_rowmajor(const int_t* src, uint32_t len, uint_t* dest) {
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(int_t);
    static const uint8_t vector_sz = 32 / elem_sz;
    static const uint8_t block_sz = 8;
    uint_t* orig_dest = dest;
    // const int8_t* orig_src = src;

    uint32_t block_sz_elems = block_sz * ndims;
    uint32_t nrows = len / ndims;
    uint32_t nblocks = nrows / block_sz;
    uint32_t padded_ndims = round_up_to_multiple(ndims, vector_sz);
    uint16_t nvectors = padded_ndims / vector_sz + ((padded_ndims % vector_sz) > 0);

    uint_t* prev_vals_ar = (uint_t*)calloc(vector_sz * elem_sz, nvectors);

    // printf("-------- decompression (len = %lld, ndims = %d)\n", (int64_t)len, ndims);
    // printf("saw compressed data:\n");
    // dump_bytes(src, len, ndims);
    // dump_bytes(src, ndims * 8, ndims);

    // ensure we don't write past the end of the output
    uint16_t overrun_ndims = vector_sz - (ndims % vector_sz);
    uint32_t trailing_nelements = len % block_sz_elems;
    if (nblocks > 1 && overrun_ndims > trailing_nelements) { nblocks -= 1; }

    // nblocks = 0;
    // printf("using nblocks: %d\n", nblocks);

    for (uint32_t b = 0; b < nblocks; b++) { // for each block
        const int_t* block_in_ptr = src + (nvectors - 1) * vector_sz;
        uint_t* block_out_ptr = dest + (nvectors - 1) * vector_sz;
        for (int32_t v = nvectors - 1; v >= 0; v--) { // for each stripe
            __m256i* prev_vals_ptr = (__m256i*)(prev_vals_ar + v * vector_sz);
            __m256i prev_vals = _mm256_loadu_si256(prev_vals_ptr);
            const int_t* in_ptr = block_in_ptr;
            uint_t* out_ptr = block_out_ptr;
            for (uint8_t i = 0; i < block_sz; i++) {
                // const int8_t* in_ptr = src + i * ndims + v * vector_sz;
                // uint8_t* out_ptr = dest + i * ndims + v * vector_sz;
                // const int8_t* in_ptr = block_in_ptr + i * ndims;
                // uint8_t* out_ptr = block_out_ptr + i * ndims;
                // if (b == 0) { printf("---- i = %d (offset %d)\n", i, (int)(in_ptr - orig_src)); }
                __m256i errs = _mm256_loadu_si256((__m256i*)in_ptr);
                __m256i vals = elem_sz == 1 ? // only supports i8 and i16
                    _mm256_add_epi8(errs, prev_vals) :
                    _mm256_add_epi16(errs, prev_vals);
                // if (b == 0) { printf("vals:     "); dump_m256i(vals); }
                _mm256_storeu_si256((__m256i*)out_ptr, vals);
                prev_vals = vals;
                in_ptr += ndims;
                out_ptr += ndims;

                // __builtin_prefetch(in_ptr + block_sz_elems, 1);
                // __builtin_prefetch(out_ptr + block_sz_elems, 1);
            }
            _mm256_storeu_si256((__m256i*)(prev_vals_ptr), prev_vals);
            block_in_ptr -= vector_sz;
            block_out_ptr -= vector_sz;
        } // for each vector
        src += block_sz_elems;
        dest += block_sz_elems;
    } // for each block

    // undo delta coding for trailing elements serially
    decode_delta_serial(src, dest, orig_dest + len, ndims, nblocks == 0);

    // printf("reconstructed input:\n");
    // dump_bytes(orig_dest, ndims * 8, ndims);

    free(prev_vals_ar);
    return len;
}

template<typename int_t, typename uint_t>
uint32_t decode_delta_rowmajor(const int_t* src, uint32_t len, uint_t* dest,
    uint16_t ndims)
{
    #define CASE(NDIMS) \
        case NDIMS: return decode_delta_rowmajor<NDIMS>(src, len, dest); break;

    #define FOUR_CASES(START) \
        CASE((START)); CASE((START+1)); CASE((START+2)); CASE((START+3));

    #define SIXTEEN_CASES(START)                        \
        FOUR_CASES(START); FOUR_CASES(START + 4);       \
        FOUR_CASES(START + 8); FOUR_CASES(START + 12);

    switch (ndims) {
        case 0: return decode_delta_rowmajor_small_ndims<0>(src, len, dest); break;
        case 1: return decode_delta_rowmajor_small_ndims<1>(src, len, dest); break;
        case 2: return decode_delta_rowmajor_small_ndims<2>(src, len, dest); break;
        CASE(3); CASE(4);
        FOUR_CASES(5); FOUR_CASES(9); FOUR_CASES(13); // cases 5-16
        SIXTEEN_CASES(16 + 1); SIXTEEN_CASES(32 + 1); SIXTEEN_CASES(48 + 1);
        default:
            return decode_delta_rowmajor_large_ndims(src, len, dest, ndims);
    }
    return 0; // This can never happen

    #undef CASE
    #undef FOUR_CASES
    #undef SIXTEEN_CASES
}
uint32_t decode_delta_rowmajor_8b(const int8_t* src, uint32_t len, uint8_t* dest,
    uint16_t ndims)
{
    return decode_delta_rowmajor(src, len, dest, ndims);
}
uint32_t decode_delta_rowmajor_16b(const int16_t* src, uint32_t len, uint16_t* dest,
    uint16_t ndims)
{
    return decode_delta_rowmajor(src, len, dest, ndims);
}

template<typename int_t, typename uint_t>
uint32_t decode_delta_rowmajor_inplace(uint_t* buff, uint32_t len,
                                       uint16_t ndims)
{
    // TODO might go a bit faster if we batch the copies
    //
    // static const uint8_t vector_sz = 32;
    // static const uint8_t block_sz = 8;
    //
    // uint32_t block_sz_elems = block_sz * ndims;
    // uint32_t nrows = len / ndims;
    // uint32_t nblocks = nrows / block_sz;
    //
    // static const uint8_t batch_nblocks = 8;
    // uint32_t batch_size_elems = block_sz_elems * batch_nblocks;
    // uint8_t* tmp = (uint8_t*)malloc(batch_size_elems + vector_sz);
    // decode_delta_rowmajor((int8_t*)buff, len, tmp, ndims);

    uint_t* tmp = (uint_t*)malloc(len * sizeof(int_t));
    uint32_t sz = decode_delta_rowmajor((int_t*)buff, len, tmp, ndims);
    memcpy(buff, tmp, sz * sizeof(int_t));
    free(tmp);
    return sz;
}

uint32_t decode_delta_rowmajor_inplace_8b(uint8_t* buff, uint32_t len,
                                          uint16_t ndims)
{
    return decode_delta_rowmajor_inplace<int8_t>(buff, len, ndims);
}
uint32_t decode_delta_rowmajor_inplace_16b(uint16_t* buff, uint32_t len,
                                          uint16_t ndims)
{
   return decode_delta_rowmajor_inplace<int16_t>(buff, len, ndims);
}


template<typename int_t, typename uint_t>
uint32_t decode_delta_rowmajor(const int_t* src, uint_t* dest) {
    uint16_t ndims;
    uint32_t len;
    src += read_metadata_simple(src, &ndims, &len);
    return decode_delta_rowmajor(src, len, dest, ndims);
}
uint32_t decode_delta_rowmajor_8b(const int8_t* src, uint8_t* dest) {
    return decode_delta_rowmajor(src, dest);
}
uint32_t decode_delta_rowmajor_16b(const int16_t* src, uint16_t* dest) {
   return decode_delta_rowmajor(src, dest);
}

// ================================================================ double delta

// ------------------------------------------------ serial version

template<typename uint_t, typename int_t>
inline int64_t encode_doubledelta_serial(const uint_t* src, uint32_t len,
    int_t* dest, uint16_t lag, const uint_t* prev_vals,
    int_t* prev_deltas)
{
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(int_t);

    if (len < 1) { return -1; }
    if (lag < 1) { return -2; }

    // if no array of previous deltas (or values) is supplied, allocate
    // a new one and initialize it to 0
    bool own_prev_vals = prev_vals == NULL;
    bool own_prev_deltas = prev_deltas == NULL;
    if (own_prev_vals) {
        prev_vals = (uint_t*)calloc(lag * elem_sz, 1);
    }
    if (own_prev_deltas) {
        prev_deltas = (int_t*)calloc(lag * elem_sz, 1);
    }

    // enccode first row using prev_vals array passed in (or initialized to 0)
    uint32_t initial_len = MIN(lag, len);
    for (uint16_t j = 0; j < initial_len; j++) {
        int_t delta = *src - prev_vals[j];
        int_t err = delta - prev_deltas[j];
        *dest = err;
        prev_deltas[j] = delta;
        src++;
        dest++;
    }
    uint32_t remaining_len = len - initial_len;

    // main loop
    uint32_t nrows = remaining_len / lag;
    for (uint32_t i = 0; i < nrows; i++) {
        for (uint16_t j = 0; j < lag; j++) {
            int_t delta = *src - *(src - lag);
            int_t err = delta - prev_deltas[j];
            *dest = err;
            prev_deltas[j] = delta;
            src++;
            dest++;
        }
    }
    // handle trailing incomplete row if present
    remaining_len -= nrows * lag;
    for (uint16_t j = 0; j < remaining_len; j++) {
        int_t delta = *src - *(src - lag);
        int_t err = delta - prev_deltas[j];
        *dest = err;
        prev_deltas[j] = delta;
        src++;
        dest++;
    }

    if (own_prev_vals) { free((uint_t*)prev_vals); }
    if (own_prev_deltas) { free(prev_deltas); }
    return len;
}

template<typename uint_t, typename int_t>
inline int32_t decode_doubledelta_serial(const int_t* src, uint32_t len,
    uint_t* dest, uint16_t lag, const uint_t* prev_vals, int_t* prev_deltas)
{
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(int_t);

    if (len < 1) { return -1; }
    if (lag < 1) { return -2; }

    // if no array of previous deltas (or values) is supplied, allocate
    // a new one and initialize it to 0
    bool own_prev_vals = prev_vals == NULL;
    bool own_prev_deltas = prev_deltas == NULL;
    if (own_prev_vals) {
        prev_vals = (uint_t*)calloc(lag * elem_sz, 1);
    }
    if (own_prev_deltas) {
        prev_deltas = (int_t*)calloc(lag * elem_sz, 1);
    }

    // decode first row using prev_vals array passed in (or initialized to 0)
    uint32_t initial_len = MIN(lag, len);
    for (uint16_t j = 0; j < initial_len; j++) {
        int_t err = *src;
        int_t delta = err + prev_deltas[j];
        *dest = delta + prev_vals[j];
        prev_deltas[j] = delta;
        src++;
        dest++;
    }
    uint32_t remaining_len = len - initial_len;

    // main loop
    uint32_t nrows = remaining_len / lag;
    for (uint32_t i = 0; i < nrows; i++) {
        for (uint16_t j = 0; j < lag; j++) {
            int_t err = *src;
            int_t delta = err + prev_deltas[j];
            uint_t prev_val = *(dest - lag);
            *dest = prev_val + delta;
            prev_deltas[j] = delta;
            src++;
            dest++;
        }
    }
    // handle possible trailing elements in the last row
    remaining_len -= nrows * lag;
    for (uint16_t j = 0; j < remaining_len; j++) {
        int_t err = *src;
        int_t delta = err + prev_deltas[j];
        uint_t prev_val = *(dest - lag);
        *dest = prev_val + delta;
        prev_deltas[j] = delta;
        src++;
        dest++;
    }

    if (own_prev_vals) { free((uint_t*)prev_vals); }
    if (own_prev_deltas) { free(prev_deltas); }
    return len;
}

// ------------------------------------------------ vectorized version

template<typename uint_t, typename int_t>
uint32_t encode_doubledelta_rowmajor(const uint_t* src, uint32_t len,
    int_t* dest, uint16_t ndims, bool write_size)
{
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(int_t);
    static const uint8_t vector_sz = 32 / elem_sz;
    static const uint8_t block_sz = 8;

    int_t* orig_dest = dest;

    uint32_t block_sz_elems = block_sz * ndims;
    uint32_t nrows = len / ndims;
    uint32_t nblocks = nrows / block_sz;
    uint32_t padded_ndims = round_up_to_multiple(ndims, vector_sz);
    uint16_t nvectors = padded_ndims / vector_sz + ((padded_ndims % vector_sz) > 0);

    uint_t* prev_vals_ar = (uint_t*)calloc(2 * vector_sz * elem_sz, nvectors);
    int_t* prev_deltas_ar = (int_t*)prev_vals_ar + vector_sz * nvectors;

    uint16_t metadata_len = 0;
    if (write_size) {
        metadata_len = write_metadata_simple(dest, ndims, len);
        dest += metadata_len;
        orig_dest = dest; // NOTE: we don't do this in any other function
    }

    // printf("-------- compression (len = %lld, ndims = %d)\n", (int64_t)len, ndims);
    // printf("saw original data:\n"); dump_bytes(src, len, ndims);
    // printf("saw original data:\n"); dump_bytes(src, ndims * 8, ndims);

    // ensure we don't write past the end of the output
    uint16_t overrun_ndims = vector_sz - (ndims % vector_sz);
    uint32_t trailing_nelements = len % block_sz_elems;
    if (nblocks > 1 && overrun_ndims > trailing_nelements) { nblocks -= 1; }

    for (uint32_t b = 0; b < nblocks; b++) { // for each block
        for (int32_t v = nvectors - 1; v >= 0; v--) { // for each stripe
            __m256i* prev_vals_ptr = (__m256i*)(prev_vals_ar + v * vector_sz);
            __m256i* prev_deltas_ptr = (__m256i*)(prev_deltas_ar + v * vector_sz);
            __m256i prev_vals = _mm256_loadu_si256(prev_vals_ptr);
            __m256i prev_deltas = _mm256_loadu_si256(prev_deltas_ptr);
            for (uint8_t i = 0; i < block_sz; i++) {
                const uint_t* in_ptr = src + i * ndims + v * vector_sz;
                int_t* out_ptr = dest + i * ndims + v * vector_sz;
                __m256i vals = _mm256_loadu_si256((__m256i*)in_ptr);

                __m256i vdeltas = elem_sz == 1 ?
                    _mm256_sub_epi8(vals, prev_vals) :
                    _mm256_sub_epi16(vals, prev_vals);
                __m256i verrs = elem_sz == 1 ?
                    _mm256_sub_epi8(vdeltas, prev_deltas) :
                    _mm256_sub_epi16(vdeltas, prev_deltas);

                _mm256_storeu_si256((__m256i*)out_ptr, verrs);
                prev_deltas = vdeltas;
                prev_vals = vals;
            }
            // TODO could replicate this loop for 0th iter and avoid storing
            // prev_vals here (just read from input)
            _mm256_storeu_si256((__m256i*)prev_vals_ptr, prev_vals);
            _mm256_storeu_si256((__m256i*)prev_deltas_ptr, prev_deltas);
        } // for each vector
        src += block_sz_elems;
        dest += block_sz_elems;
    } // for each block

    uint32_t remaining_len = len - (uint32_t)(dest - orig_dest);
    int_t* use_prev_deltas = nullptr;
    const uint_t* use_prev_vals = nullptr;
    if (nblocks > 0) {
        use_prev_vals = src - ndims;
        use_prev_deltas = prev_deltas_ar;
    }
    encode_doubledelta_serial(src, remaining_len, dest, ndims,
                              use_prev_vals, use_prev_deltas);

    free(prev_vals_ar);
    return len + metadata_len;
}
uint32_t encode_doubledelta_rowmajor_8b(const uint8_t* src, uint32_t len,
    int8_t* dest, uint16_t ndims, bool write_size)
{
    return encode_doubledelta_rowmajor(src, len, dest, ndims, write_size);
}
uint32_t encode_doubledelta_rowmajor_16b(const uint16_t* src, uint32_t len,
    int16_t* dest, uint16_t ndims, bool write_size)
{
    return encode_doubledelta_rowmajor(src, len, dest, ndims, write_size);
}

template<typename int_t, typename uint_t>
uint32_t decode_doubledelta_rowmajor(const int_t* src, uint32_t len,
    uint_t* dest, uint16_t ndims)
{
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(uint_t);
    static const uint8_t vector_sz = 32 / elem_sz;
    static const uint8_t block_sz = 8;
    uint_t* orig_dest = dest;

    if (ndims == 0) { return 0; }

    uint32_t block_sz_elems = block_sz * ndims;
    uint32_t nrows = len / ndims;
    uint32_t nblocks = nrows / block_sz;
    uint32_t padded_ndims = round_up_to_multiple(ndims, vector_sz);
    uint16_t nvectors = padded_ndims / vector_sz + ((padded_ndims % vector_sz) > 0);

    uint_t* prev_vals_ar = (uint_t*)calloc(2 * vector_sz * elem_sz, nvectors);
    int_t* prev_deltas_ar = (int_t*)prev_vals_ar + vector_sz * nvectors;

    // ensure we don't write past the end of the output
    uint16_t overrun_ndims = vector_sz - (ndims % vector_sz);
    uint32_t trailing_nelements = len % block_sz_elems;
    if (nblocks > 1 && overrun_ndims > trailing_nelements) { nblocks -= 1; }

    for (uint32_t b = 0; b < nblocks; b++) { // for each block
        for (int32_t v = nvectors - 1; v >= 0; v--) { // for each stripe
            __m256i* prev_vals_ptr = (__m256i*)(prev_vals_ar + v * vector_sz);
            __m256i* prev_deltas_ptr = (__m256i*)(prev_deltas_ar + v * vector_sz);
            __m256i prev_vals = _mm256_loadu_si256(prev_vals_ptr);
            __m256i prev_deltas = _mm256_loadu_si256(prev_deltas_ptr);
            for (uint8_t i = 0; i < block_sz; i++) {
                const int_t* in_ptr = src + i * ndims + v * vector_sz;
                uint_t* out_ptr = dest + i * ndims + v * vector_sz;
                __m256i verrs = _mm256_loadu_si256((__m256i*)in_ptr);

                __m256i vdeltas = elem_sz == 1 ?
                    _mm256_add_epi8(verrs, prev_deltas) :
                    _mm256_add_epi16(verrs, prev_deltas);
                __m256i vals = elem_sz == 1 ?
                    _mm256_add_epi8(vdeltas, prev_vals) :
                    _mm256_add_epi16(vdeltas, prev_vals);

                _mm256_storeu_si256((__m256i*)out_ptr, vals);
                prev_deltas = vdeltas;
                prev_vals = vals;
            }
            // TODO could replicate this loop for 0th iter and avoid storing
            // prev_vals here (just read from output)
            _mm256_storeu_si256((__m256i*)prev_vals_ptr, prev_vals);
            _mm256_storeu_si256((__m256i*)prev_deltas_ptr, prev_deltas);
        } // for each vector
        src += block_sz_elems;
        dest += block_sz_elems;
    } // for each block

    // undo delta coding for trailing elements serially
    // decode_delta_serial(src, dest, orig_dest + len, ndims, nblocks == 0);
    uint32_t remaining_len = len - (uint32_t)(dest - orig_dest);
    int_t* use_prev_deltas = nullptr;
    uint_t* use_prev_vals = nullptr;
    if (nblocks > 0) {
        use_prev_vals = dest - ndims;
        use_prev_deltas = prev_deltas_ar;
    }
    decode_doubledelta_serial(src, remaining_len, dest, ndims,
                              use_prev_vals, use_prev_deltas);

    free(prev_vals_ar);
    return len;
}
uint32_t decode_doubledelta_rowmajor_8b(const int8_t* src, uint32_t len,
    uint8_t* dest, uint16_t ndims)
{
    return decode_doubledelta_rowmajor(src, len, dest, ndims);
}
uint32_t decode_doubledelta_rowmajor_16b(const int16_t* src, uint32_t len,
    uint16_t* dest, uint16_t ndims)
{
    return decode_doubledelta_rowmajor(src, len, dest, ndims);
}

template<typename int_t, typename uint_t>
uint32_t decode_doubledelta_rowmajor_inplace(uint_t* buff, uint32_t len,
                                       uint16_t ndims)
{
    // return decode_delta_rowmajor_inplace<int_t>(buff, len, ndims);
    uint_t* tmp = (uint_t*)malloc(len * sizeof(int_t) + 1024);
    uint32_t sz = decode_doubledelta_rowmajor((int_t*)buff, len, tmp, ndims);
    // uint32_t sz = decode_delta_rowmajor((int_t*)buff, len, tmp, ndims); // XXX rm after debug
    memcpy(buff, tmp, sz * sizeof(int_t));
    free(tmp);
    return sz;
}
uint32_t decode_doubledelta_rowmajor_inplace_8b(uint8_t* buff, uint32_t len,
                                             uint16_t ndims)
{
    return decode_doubledelta_rowmajor_inplace<int8_t>(buff, len, ndims);
    // return decode_delta_rowmajor_inplace<int8_t>(buff, len, ndims); // XXX rm
}
uint32_t decode_doubledelta_rowmajor_inplace_16b(uint16_t* buff, uint32_t len,
                                             uint16_t ndims)
{
    return decode_doubledelta_rowmajor_inplace<int16_t>(buff, len, ndims);
}

template<typename int_t, typename uint_t>
uint32_t decode_doubledelta_rowmajor(const int_t* src, uint_t* dest) {
    uint16_t ndims;
    uint32_t len;
    src += read_metadata_simple(src, &ndims, &len);
    return decode_doubledelta_rowmajor(src, len, dest, ndims);
}
uint32_t decode_doubledelta_rowmajor_8b(const int8_t* src, uint8_t* dest) {
    return decode_doubledelta_rowmajor(src, dest);
}
uint32_t decode_doubledelta_rowmajor_16b(const int16_t* src, uint16_t* dest) {
   return decode_doubledelta_rowmajor(src, dest);
}

#undef MIN
