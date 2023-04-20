//
//  predict.cpp
//  Compress
//
//  Created by DB on 11/4/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#include "Runtime/MemoryLayout/Compression/Sprintz/predict.h"

#include <stdio.h>
#include <assert.h>
#include <string.h>

#include "immintrin.h"

#include "Runtime/MemoryLayout/Compression/Sprintz/debug_utils.hpp"// TODO rm
#include "Runtime/MemoryLayout/Compression/Sprintz/format.h"
#include "Runtime/MemoryLayout/Compression/Sprintz/util.h"

static const int debug = 0;

#define CHECK_INT_UINT_TYPES_VALID(int_t, uint_t)               \
    static_assert(sizeof(uint_t) == sizeof(int_t),              \
        "uint type and int type sizes must be the same!");      \
    static_assert(sizeof(uint_t) == 1 || sizeof(uint_t) == 2,   \
        "Only element sizes of 1 and 2 bytes are supported!");  \


// TODO serial xff encoding instead
template<typename int_t, typename uint_t>
inline int64_t decode_delta_serial(const int_t* src, uint_t* dest,
    const uint_t* dest_end, uint16_t lag, bool needs_initial_cpy)
{
    CHECK_INT_UINT_TYPES_VALID(uint_t, int_t);

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


// see https://godbolt.org/g/1Q31mK for assembly
template<typename uint_t, typename int_t>
uint32_t encode_xff_rowmajor(const uint_t* src, uint32_t len, int_t* dest,
                             uint16_t ndims, bool write_size)
{
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(uint_t);
    static const uint8_t learning_shift = elem_sz == 1 ? 1 : 3;
    static const uint8_t log2_block_sz = 3;
    static const uint8_t log2_learning_downsample = 1;
    static const uint8_t vector_nbytes = 32;
    // derived constants
    static const uint8_t block_sz = 1 << log2_block_sz;
    static const uint16_t elem_sz_nbits = elem_sz * 8;
    static const uint8_t learning_downsample = 1 << log2_learning_downsample;
    static const uint8_t vector_sz = vector_nbytes / elem_sz;

    int_t* orig_dest = dest;

    uint32_t block_sz_elems = block_sz * ndims;
    uint32_t nrows = len / ndims;
    int32_t nblocks = nrows / block_sz;
    uint32_t padded_ndims = round_up_to_multiple(ndims, vector_sz);
    uint16_t nvectors = padded_ndims / vector_sz + ((padded_ndims % vector_sz) > 0);

    // allocate these three arrays contiguously
    uint32_t row_sz = nvectors * vector_sz;
    uint_t* prev_vals_ar   = (uint_t*)calloc(2 * row_sz * elem_sz, 1);
    int_t*  prev_deltas_ar = (int_t* )prev_vals_ar + row_sz;
    uint_t* coeffs_ar_even = (uint_t*)calloc(2 * row_sz * elem_sz, 1);
    uint_t* coeffs_ar_odd = coeffs_ar_even + row_sz;

    uint16_t metadata_len = 0;
    if (write_size) {
        metadata_len = write_metadata_simple(dest, ndims, len);
        dest += metadata_len;
        orig_dest = dest; // NOTE: we don't do this in any other function
    }

    if (debug > 2) {
        printf("-------- compression (len = %lld, ndims = %d)\n", (int64_t)len, ndims);
        printf("saw original data:\n"); dump_bytes(src, len, ndims);
        // printf("saw original data:\n"); dump_bytes(src, ndims * 8, ndims);
    }

    // ensure we don't write past the end of the output
    uint16_t overrun_ndims = vector_sz - (ndims % vector_sz);
    uint32_t trailing_nelements = len % block_sz_elems;
    // if (nblocks > 1 && overrun_ndims > trailing_nelements) { nblocks -= 1; }
    if (overrun_ndims > trailing_nelements) {
        nblocks -= div_round_up(overrun_ndims, block_sz_elems);
        nblocks = MAX(0, nblocks);
    }

    // const __m256i high_bits_one = _mm256_set1_epi8(-128);
    // const __m256i zeros = _mm256_setzero_si256();
    // const __m256i ones = _mm256_cmpeq_epi8(zeros, zeros);

    // const __m256i filter_coeffs_even = _mm256_setr_epi16(
    //     16, 32, 64, 128, 256, 255, 128, 64,
    //     31, 32, 33, 200, 256, 200, 100, 50);
    // const __m256i filter_coeffs_odd = _mm256_setr_epi16(
    //     256, 128, 64, 32, 16, 8, 4, 2,
    //     2, 4, 8, 16, 32, 64, 128, 256);
    const __m256i low_mask = _mm256_set1_epi16(0xff);

    for (int32_t b = 0; b < nblocks; b++) { // for each block
        // printf("==== %d\n", b);
        for (int32_t v = nvectors - 1; v >= 0; v--) { // for each stripe
            uint32_t v_offset = v * vector_sz;
            __m256i* prev_vals_ptr = (__m256i*)(prev_vals_ar + v_offset);
            __m256i* prev_deltas_ptr = (__m256i*)(prev_deltas_ar + v_offset);
            __m256i prev_vals = _mm256_loadu_si256(prev_vals_ptr);
            __m256i prev_deltas = _mm256_loadu_si256(prev_deltas_ptr);

            const __m256i* even_counters_ptr =
                (const __m256i*)(coeffs_ar_even + v_offset);
            const __m256i* odd_counters_ptr =
                (const __m256i*)(coeffs_ar_odd + v_offset);
            __m256i coef_counters_even = _mm256_loadu_si256(even_counters_ptr);
            __m256i coef_counters_odd = _mm256_loadu_si256(odd_counters_ptr);

            __m256i gradients_sum = _mm256_setzero_si256();

            if (elem_sz == 1) {
                // set coef[i] to ((counter[i] >> learn_shift) >> 4) << 4)
                __m256i filter_coeffs_even  = _mm256_srai_epi16(
                    coef_counters_even, learning_shift + 4);
                __m256i filter_coeffs_odd  = _mm256_srai_epi16(
                    coef_counters_odd, learning_shift + 4);
                filter_coeffs_even = _mm256_slli_epi16(filter_coeffs_even, 4);
                filter_coeffs_odd  = _mm256_slli_epi16(filter_coeffs_odd, 4);

                for (uint8_t i = 0; i < block_sz; i++) {
                    const uint_t* in_ptr = src + i * ndims + v_offset;
                    int_t* out_ptr = dest + i * ndims + v_offset;
                    __m256i vals = _mm256_loadu_si256((__m256i*)in_ptr);
                    __m256i vdeltas = _mm256_sub_epi8(vals, prev_vals);

                    // want: ([a8 b8 c8 d8] .* [e16 f16 g16 h16]) >> 8,
                    //  where rhs vals <= 256; key here is that products end
                    //  up in byte 1 for each multiplication
                    //
                    // 1) [a8; 0] [c8; 0] .* e16 f16 = [...;ae] [...;cg] //and, mul
                    // 2) [b8; 0] [d8; 0] .* e16 f16 = [...;bf] [...;dh] //shft,mul
                    // 3) [ae; 0] [cg; 0] // left shift
                    //  | [...;bf][...;dh]
                    //  = [ae; bf][cg; dh]  // vpblendv
                    __m256i even_predictions = _mm256_mullo_epi16(
                        _mm256_and_si256(prev_deltas, low_mask), filter_coeffs_even);
                    __m256i odd_predictions = _mm256_mullo_epi16(
                        _mm256_srai_epi16(prev_deltas, 8), filter_coeffs_odd);
                    __m256i vpredictions = _mm256_blendv_epi8(odd_predictions,
                        _mm256_srli_epi16(even_predictions, 8), low_mask);

                    __m256i verrs = _mm256_sub_epi8(vdeltas, vpredictions);

                    if (i % learning_downsample == learning_downsample - 1) {
                        __m256i gradients = _mm256_sign_epi8(prev_deltas, verrs);

                        // // upcast to epi16 before accumulating to prevent overflow
                        // __m256i grads_even = _mm256_srai_epi16(
                        //     _mm256_slli_epi16(gradients, 8), 8);
                        // __m256i grads_odd = _mm256_srai_epi16(gradients, 8);
                        // grad_sums_even = _mm256_add_epi16(grad_sums_even, grads_even);
                        // grad_sums_odd = _mm256_add_epi16(grad_sums_odd, grads_odd);

                        // this way is faster, but can overflow
                        gradients_sum = _mm256_add_epi8(gradients_sum, gradients);
                    }

                    _mm256_storeu_si256((__m256i*)out_ptr, verrs);
                    prev_deltas = vdeltas;
                    prev_vals = vals;
                }

                // mean of gradients in block, for even and odd indices
                const uint8_t rshift = 8 + (log2_block_sz - log2_learning_downsample);
                __m256i even_grads = _mm256_srai_epi16(
                    _mm256_slli_epi16(gradients_sum, 8), rshift);
                __m256i odd_grads = _mm256_srai_epi16(gradients_sum, rshift);

                // if (b == 0) { printf("mean gradients: "); dump_m256i(gradients); }
                // if (b == 0) { printf("even gradients: "); dump_m256i(even_grads); }
                // if (b == 0) { printf("odd gradients:  "); dump_m256i(odd_grads); }

                // store updated coefficients (or, technically, the counters)
                coef_counters_even = _mm256_add_epi16(coef_counters_even, even_grads);
                coef_counters_odd = _mm256_add_epi16(coef_counters_odd, odd_grads);
            } else if (elem_sz == 2) {
                static const __m256i low_mask_epi32 = _mm256_set1_epi32(0xffff);

                // set coef[i] to ((counter[i] >> learn_shift) >> 4) << 4)
                // const uint8_t shft = elem_sz_nbits - 4;
                // __m256i filter_coeffs_even  = _mm256_srai_epi32(
                //     coef_counters_even, learning_shift + shft);
                // __m256i filter_coeffs_odd  = _mm256_srai_epi32(
                //     coef_counters_odd, learning_shift + shft);
                // __m256i filter_coeffs = _mm256_blendv_epi8(
                //     filter_coeffs_odd, filter_coeffs_even, low_mask_epi32);
                // filter_coeffs = _mm256_slli_epi16(
                //     filter_coeffs, elem_sz_nbits - shft);
                const uint8_t shft = elem_sz_nbits - 4;
                __m256i filter_coeffs_even  = _mm256_srai_epi32(
                    coef_counters_even, learning_shift + shft);
                __m256i filter_coeffs_odd  = _mm256_srai_epi32(
                    coef_counters_odd, learning_shift + shft);
                filter_coeffs_odd = _mm256_slli_epi32(filter_coeffs_odd, elem_sz_nbits);
                __m256i filter_coeffs = _mm256_blendv_epi8(
                    filter_coeffs_odd, filter_coeffs_even, low_mask_epi32);
                filter_coeffs = _mm256_slli_epi16(
                    filter_coeffs, shft);

                for (uint8_t i = 0; i < block_sz; i++) {
                    const uint_t* in_ptr = src + i * ndims + v_offset;
                    int_t* out_ptr = dest + i * ndims + v_offset;
                    __m256i vals = _mm256_loadu_si256((__m256i*)in_ptr);
                    __m256i vdeltas = _mm256_sub_epi16(vals, prev_vals);

                    __m256i vpredictions = _mm256_mulhi_epi16(
                        prev_deltas, filter_coeffs);
                    vpredictions = _mm256_slli_epi16(vpredictions, 2);

                    __m256i verrs = _mm256_sub_epi16(vdeltas, vpredictions);

                    if (i % learning_downsample == learning_downsample - 1) {
                        __m256i gradients = _mm256_sign_epi16(prev_deltas, verrs);
                        gradients_sum = _mm256_add_epi16(gradients_sum, gradients);
                    }

                    _mm256_storeu_si256((__m256i*)out_ptr, verrs);
                    prev_deltas = vdeltas;
                    prev_vals = vals;
                }
                // mean of gradients in block, for even and odd indices
                const uint8_t rshift = 16 + (log2_block_sz - log2_learning_downsample);
                __m256i even_grads = _mm256_srai_epi32(
                    _mm256_slli_epi32(gradients_sum, 16), rshift);
                __m256i odd_grads = _mm256_srai_epi32(gradients_sum, rshift);

                // store updated coefficients (or, technically, the counters)
                coef_counters_even = _mm256_add_epi32(coef_counters_even, even_grads);
                coef_counters_odd = _mm256_add_epi32(coef_counters_odd, odd_grads);
            }
            _mm256_storeu_si256((__m256i*)prev_vals_ptr, prev_vals);
            _mm256_storeu_si256((__m256i*)prev_deltas_ptr, prev_deltas);
            _mm256_storeu_si256((__m256i*)even_counters_ptr, coef_counters_even);
            _mm256_storeu_si256((__m256i*)odd_counters_ptr, coef_counters_odd);

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
    for (; dest < (orig_dest + len); ++dest) {
        *dest = *(src) - *(src - ndims);
        src++;
    }

    // printf("got thru encoding without dying\n");

    free(prev_vals_ar);
    free(coeffs_ar_even);
    return len + metadata_len;
    // if (write_size) { return len + 6; }
    // return len;
}
uint32_t encode_xff_rowmajor_8b(const uint8_t* src, uint32_t len, int8_t* dest,
    uint16_t ndims, bool write_size)
{
    return encode_xff_rowmajor(src, len, dest, ndims, write_size);
}
uint32_t encode_xff_rowmajor_16b(const uint16_t* src, uint32_t len,
    int16_t* dest, uint16_t ndims, bool write_size)
{
    return encode_xff_rowmajor(src, len, dest, ndims, write_size);
}

template<typename int_t, typename uint_t>
uint32_t decode_xff_rowmajor(const int_t* src, uint32_t len, uint_t* dest,
                             uint16_t ndims)
{
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(uint_t);
    static const uint8_t learning_shift = elem_sz == 1 ? 1 : 3;
    static const uint8_t log2_block_sz = 3;
    static const uint8_t log2_learning_downsample = 1;
    static const uint8_t vector_nbytes = 32;
    // derived constants
    static const uint8_t block_sz = 1 << log2_block_sz;
    static const uint16_t elem_sz_nbits = elem_sz * 8;
    static const uint8_t learning_downsample = 1 << log2_learning_downsample;
    static const uint8_t vector_sz = vector_nbytes / elem_sz;

    uint_t* orig_dest = dest;

    if (ndims == 0) { return 0; }

    uint32_t block_sz_elems = block_sz * ndims;
    uint32_t nrows = len / ndims;
    int32_t nblocks = nrows / block_sz;
    uint32_t padded_ndims = round_up_to_multiple(ndims, vector_sz);
    uint16_t nvectors = padded_ndims / vector_sz + ((padded_ndims % vector_sz) > 0);

    // allocate these three arrays contiguously
    // uint8_t* prev_vals_ar   = (uint8_t*)calloc(4 * nvectors, vector_sz);
    // int8_t*  prev_deltas_ar = (int8_t* )prev_vals_ar + vector_sz * 1 * nvectors;
    // uint8_t* coeffs_ar_even = (uint8_t*)prev_vals_ar + vector_sz * 2 * nvectors;
    // uint8_t* coeffs_ar_odd  = (uint8_t*)prev_vals_ar + vector_sz * 3 * nvectors;

    // uint_t* prev_vals_ar   = (uint_t*)calloc(4 * nvectors * elem_sz, vector_sz);
    // // uint8_t* prev_vals_ar   = (uint8_t*)calloc(4 * nvectors * elem_sz, vector_sz);
    // int_t*  prev_deltas_ar = (int_t* )prev_vals_ar + vector_sz * 1 * nvectors;
    // // int8_t*  prev_deltas_ar = (int8_t* )prev_vals_ar + vector_sz * 1 * nvectors;
    // uint8_t* coeffs_ar_even = (uint8_t*)prev_vals_ar + vector_sz * 2 * nvectors;
    // uint8_t* coeffs_ar_odd  = (uint8_t*)prev_vals_ar + vector_sz * 3 * nvectors;

    uint32_t row_sz = nvectors * vector_sz;
    uint_t* prev_vals_ar   = (uint_t*)calloc(2 * row_sz, elem_sz);
    int_t*  prev_deltas_ar = (int_t* )prev_vals_ar + row_sz;
    uint_t* coeffs_ar_even = (uint_t*)calloc(2 * row_sz, elem_sz);
    uint_t* coeffs_ar_odd = coeffs_ar_even + row_sz;

    // printf("-------- decompression (len = %lld, ndims = %d)\n", (int64_t)len, ndims);
    // printf("saw compressed data:\n");
    // dump_bytes(src, len, ndims);
    // dump_bytes(src, ndims * 8, ndims);

    // ensure we don't write past the end of the output
    uint16_t overrun_ndims = vector_sz - (ndims % vector_sz);
    uint32_t trailing_nelements = len % block_sz_elems;
    // if (nblocks > 1 && overrun_ndims > trailing_nelements) { nblocks -= 1; }
    if (overrun_ndims > trailing_nelements) {
        nblocks -= div_round_up(overrun_ndims, block_sz_elems);
        nblocks = MAX(0, nblocks);
    }

    // const __m256i high_bits_one = _mm256_set1_epi8(-128);
    // const __m256i zeros = _mm256_setzero_si256();
    // const __m256i ones = _mm256_cmpeq_epi8(zeros, zeros);
    // const __m256i filter_coeffs_even = _mm256_setr_epi16(
    //     16, 32, 64, 128, 256, 255, 128, 64,
    //     31, 32, 33, 200, 256, 200, 100, 50);
    // const __m256i filter_coeffs_odd = _mm256_setr_epi16(
    //     256, 128, 64, 32, 16, 8, 4, 2,
    //     2, 4, 8, 16, 32, 64, 128, 256);
    const __m256i low_mask = _mm256_set1_epi16(0xff);

    for (uint32_t b = 0; b < nblocks; b++) { // for each block
        // printf("==== %d\n", b);
        for (int32_t v = nvectors - 1; v >= 0; v--) { // for each stripe
            uint32_t v_offset = v * vector_sz;
            __m256i* prev_vals_ptr = (__m256i*)(prev_vals_ar + v_offset);
            __m256i* prev_deltas_ptr = (__m256i*)(prev_deltas_ar + v_offset);
            __m256i prev_vals = _mm256_loadu_si256(prev_vals_ptr);
            __m256i prev_deltas = _mm256_loadu_si256(prev_deltas_ptr);


            const __m256i* even_counters_ptr =
                (const __m256i*)(coeffs_ar_even + v_offset);
            const __m256i* odd_counters_ptr =
                (const __m256i*)(coeffs_ar_odd + v_offset);
            __m256i coef_counters_even = _mm256_loadu_si256(even_counters_ptr);
            __m256i coef_counters_odd = _mm256_loadu_si256(odd_counters_ptr);

            // __m256i grad_sums_even = _mm256_setzero_si256();
            // __m256i grad_sums_odd = _mm256_setzero_si256();
            __m256i gradients_sum = _mm256_setzero_si256();

            if (elem_sz == 1) {

                // set coef[i] to ((counter[i] >> learn_shift) >> 4) << 4)
                const uint_t shft = elem_sz_nbits - 4;
                __m256i filter_coeffs_even  = _mm256_srai_epi16(
                    coef_counters_even, learning_shift + shft);
                __m256i filter_coeffs_odd  = _mm256_srai_epi16(
                    coef_counters_odd, learning_shift + shft);
                filter_coeffs_even = _mm256_slli_epi16(
                    filter_coeffs_even, elem_sz_nbits - shft);
                filter_coeffs_odd  = _mm256_slli_epi16(
                    filter_coeffs_odd, elem_sz_nbits - shft);

                for (uint8_t i = 0; i < block_sz; i++) {
                    const int_t* in_ptr = src + i * ndims + v_offset;
                    uint_t* out_ptr = dest + i * ndims + v_offset;
                    __m256i verrs = _mm256_loadu_si256((__m256i*)in_ptr);

                    // see encode func for explanation
                    __m256i even_predictions = _mm256_mullo_epi16(
                        _mm256_and_si256(prev_deltas, low_mask), filter_coeffs_even);
                    __m256i odd_predictions = _mm256_mullo_epi16(
                        _mm256_srai_epi16(prev_deltas, 8), filter_coeffs_odd);
                    __m256i vpredictions = _mm256_blendv_epi8(odd_predictions,
                        _mm256_srli_epi16(even_predictions, 8), low_mask);

                    // compute gradients, but downsample for speed
                    if (i % learning_downsample == learning_downsample - 1) {
                        __m256i gradients = _mm256_sign_epi8(prev_deltas, verrs);
                        gradients_sum = _mm256_add_epi8(gradients_sum, gradients);
                    }

                    __m256i vdeltas = _mm256_add_epi8(verrs, vpredictions);
                    __m256i vals = _mm256_add_epi8(vdeltas, prev_vals);

                    _mm256_storeu_si256((__m256i*)out_ptr, vals);
                    prev_deltas = vdeltas;
                    prev_vals = vals;
                }
                // mean of gradients in block, for even and odd indices
                const uint8_t rshift = 8 + log2_block_sz - log2_learning_downsample;
                __m256i even_grads = _mm256_srai_epi16(
                    _mm256_slli_epi16(gradients_sum, 8), rshift);
                __m256i odd_grads = _mm256_srai_epi16(gradients_sum, rshift);

                // store updated coefficients (or, technically, the counters)
                coef_counters_even = _mm256_add_epi16(coef_counters_even, even_grads);
                coef_counters_odd = _mm256_add_epi16(coef_counters_odd, odd_grads);

            } else if (elem_sz == 2) {
                static const __m256i low_mask_epi32 = _mm256_set1_epi32(0xffff);

                // set coef[i] to ((counter[i] >> learn_shift) >> 12) << 12)
                // const uint8_t shft = elem_sz_nbits - 4;
                // __m256i filter_coeffs_even  = _mm256_srai_epi32(
                //     coef_counters_even, learning_shift + shft);
                // __m256i filter_coeffs_odd  = _mm256_srai_epi32(
                //     coef_counters_odd, learning_shift + shft);
                // __m256i filter_coeffs = _mm256_blendv_epi8(
                //     filter_coeffs_odd, filter_coeffs_even, low_mask_epi32);
                // filter_coeffs = _mm256_slli_epi16(
                //     filter_coeffs, elem_sz_nbits - shft);

                const uint8_t shft = elem_sz_nbits - 4;
                __m256i filter_coeffs_even  = _mm256_srai_epi32(
                    coef_counters_even, learning_shift + shft);
                __m256i filter_coeffs_odd  = _mm256_srai_epi32(
                    coef_counters_odd, learning_shift + shft);
                filter_coeffs_odd = _mm256_slli_epi32(filter_coeffs_odd, elem_sz_nbits);
                __m256i filter_coeffs = _mm256_blendv_epi8(
                    filter_coeffs_odd, filter_coeffs_even, low_mask_epi32);
                filter_coeffs = _mm256_slli_epi16(
                    filter_coeffs, shft);

                for (uint8_t i = 0; i < block_sz; i++) {
                    const int_t* in_ptr = src + i * ndims + v_offset;
                    uint_t* out_ptr = dest + i * ndims + v_offset;
                    __m256i verrs = _mm256_loadu_si256((__m256i*)in_ptr);

                    __m256i vpredictions = _mm256_mulhi_epi16(
                        prev_deltas, filter_coeffs);
                    vpredictions = _mm256_slli_epi16(vpredictions, 2);

                    if (i % learning_downsample == learning_downsample - 1) {
                        __m256i gradients = _mm256_sign_epi16(prev_deltas, verrs);
                        gradients_sum = _mm256_add_epi16(gradients_sum, gradients);
                    }

                    __m256i vdeltas = _mm256_add_epi16(verrs, vpredictions);
                    __m256i vals = _mm256_add_epi16(vdeltas, prev_vals);

                    _mm256_storeu_si256((__m256i*)out_ptr, vals);
                    prev_deltas = vdeltas;
                    prev_vals = vals;
                }
                // mean of gradients in block, for even and odd indices
                const uint8_t rshift = 16 + log2_block_sz - log2_learning_downsample;
                __m256i even_grads = _mm256_srai_epi32(
                    _mm256_slli_epi32(gradients_sum, 16), rshift);
                __m256i odd_grads = _mm256_srai_epi32(gradients_sum, rshift);

                // store updated coefficients (or, technically, the counters)
                coef_counters_even = _mm256_add_epi32(coef_counters_even, even_grads);
                coef_counters_odd = _mm256_add_epi32(coef_counters_odd, odd_grads);
            }
            _mm256_storeu_si256((__m256i*)prev_vals_ptr, prev_vals);
            _mm256_storeu_si256((__m256i*)prev_deltas_ptr, prev_deltas);
            _mm256_storeu_si256((__m256i*)even_counters_ptr, coef_counters_even);
            _mm256_storeu_si256((__m256i*)odd_counters_ptr, coef_counters_odd);
        } // for each vector
        src += block_sz_elems;
        dest += block_sz_elems;
    } // for each block

    // undo delta coding for trailing elements serially
    decode_delta_serial(src, dest, orig_dest + len, ndims, nblocks == 0);

    // printf("decoded data:\n"); dump_bytes(orig_dest, len, ndims);

    if (debug > 2) {
        printf("decompressed data:\n"); dump_bytes(orig_dest, len, ndims * 4);
    }

    free(prev_vals_ar);
    free(coeffs_ar_even);
    return len;
}
uint32_t decode_xff_rowmajor_8b(const int8_t* src, uint32_t len, uint8_t* dest,
                             uint16_t ndims)
{
    return decode_xff_rowmajor(src, len, dest, ndims);
}
uint32_t decode_xff_rowmajor_16b(const int16_t* src, uint32_t len,
                                 uint16_t* dest, uint16_t ndims)
{
    return decode_xff_rowmajor(src, len, dest, ndims);
}

// TODO actually operate in-place
template<typename int_t, typename uint_t>
uint32_t decode_xff_rowmajor_inplace(uint_t* buff, uint32_t len,
                                     uint16_t ndims)
{
    static const uint8_t elem_sz = sizeof(int_t);
    // XXX padding is necessary for for 8bit with ndims=1, but shouldn't be
    // EDIT: maybe fixed by better handling of reducing nblocks in low-dim case
    uint_t* tmp = (uint_t*)malloc(len * elem_sz + 1024);
    uint32_t sz = decode_xff_rowmajor((int_t*)buff, len, tmp, ndims);
    memcpy(buff, tmp, sz * elem_sz);
    free(tmp);
    return sz;
}
uint32_t decode_xff_rowmajor_inplace_8b(uint8_t* buff, uint32_t len,
                                     uint16_t ndims)
{
    return decode_xff_rowmajor_inplace<int8_t>(buff, len, ndims);
}
uint32_t decode_xff_rowmajor_inplace_16b(uint16_t* buff, uint32_t len,
                                     uint16_t ndims)
{
    return decode_xff_rowmajor_inplace<int16_t>(buff, len, ndims);
}

template<typename int_t, typename uint_t>
uint32_t decode_xff_rowmajor(const int_t* src, uint_t* dest) {
    uint16_t ndims;
    uint32_t len;
    src += read_metadata_simple(src, &ndims, &len);
    return decode_xff_rowmajor(src, len, dest, ndims);
}
uint32_t decode_xff_rowmajor_8b(const int8_t* src, uint8_t* dest) {
    return decode_xff_rowmajor(src, dest);
}
uint32_t decode_xff_rowmajor_16b(const int16_t* src, uint16_t* dest) {
    return decode_xff_rowmajor(src, dest);
}
