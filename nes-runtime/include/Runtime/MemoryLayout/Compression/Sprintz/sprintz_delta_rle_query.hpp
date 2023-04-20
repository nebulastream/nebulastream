//
//  sprintz_delta_rle.cpp
//  Compress
//
//  Created by DB on 12/11/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

// #include "sprintz_delta.h"

#include <stdio.h>

#include <assert.h>
#include <stdint.h>
#include <string.h>

#include "bitpack.h"
#include "format.h"
#include "util.h" // for memrep

#include "debug_utils.hpp" // TODO rm

#include  "query.hpp"

// static const int debug = 0;
// static const int debug = 3;
// static const int debug = 4;

static const int kDefaultGroupSzBlocks = 2;

// ========================================================== rowmajor delta rle

template<bool DoWrite=false, class int_t, class uint_t, class Func>
SPRINTZ_FORCE_INLINE int64_t query_rowmajor_delta_rle(const int_t* src,
    uint_t* dest, uint16_t ndims, uint32_t ngroups, uint16_t remaining_len,
    Func& func)
{
    // static const int gDebug = 4;
    static const int gDebug = 0;

    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(uint_t);
    typedef typename ElemSzTraits<elem_sz>::bitwidth_t bitwidth_t;
    static const uint8_t elem_sz_nbits = 8 * elem_sz;
    static const uint8_t nbits_sz_bits = elem_sz == 1 ? 3 : 4; // XXX {8,16}b
    // constants that could, in principle, be changed (but not in this impl)
    static const uint8_t block_sz = 8;
    static const uint8_t stripe_nbytes = 8;
    static const uint8_t vector_sz_nbytes = 32;
    // constants that could actually be changed in this impl
    static const uint8_t group_sz_blocks = kDefaultGroupSzBlocks;
    // derived constants
    static const int group_sz_per_dim = block_sz * group_sz_blocks;
    static const uint8_t stripe_header_sz = nbits_sz_bits * stripe_nbytes / 8;
    static const uint8_t stripe_sz = stripe_nbytes / elem_sz;
    static const uint8_t nbits_sz_mask = (1 << nbits_sz_bits) - 1;
    static const uint64_t kHeaderUnpackMask = TILE_BYTE(nbits_sz_mask);
    static const uint32_t min_data_size = 8 * block_sz * group_sz_blocks;
    static const uint8_t vector_sz = vector_sz_nbytes / elem_sz;
    // assert(stripe_sz % 8 == 0);
    assert(vector_sz % stripe_sz == 0);
    assert(vector_sz >= stripe_sz);


    // printf("running delta rowmajor rle!  ");
    // printf("DoWrite = %d\n", (int)DoWrite);
//    func(); // TODO rm

    // uint8_t* orig_dest = dest;
    // const int8_t* orig_src = src;

    uint_t* orig_dest = dest;
    const int_t* orig_src = src;

    // int gDebug = debug;
    // int debug = (elem_sz == 2) ? gDebug : 0;
    int debug = gDebug;
    // int debug = false;

    // exit(1); // TODO rm

    if (debug > 0) {
        int32_t min_orig_len = ngroups * group_sz_blocks * block_sz * ndims;
        printf("--------- decomp: saw original ngroups, ndims, min_len = %u, %d, %d\n", ngroups, ndims, min_orig_len);
        if (debug > 3) {
            printf("saw compressed data (with possible extra at end):\n");
            // dump_bytes(src, orig_len + 8, ndims * elem_sz);
            // dump_bytes(((uint8_t*)src) + ndims, min_orig_len, ndims * elem_sz);
            dump_elements(src, min_orig_len + 4, ndims);
        }
    }

    // if (debug > 2) { exit(1); } // TODO rm

    // ------------------------ handle edge cases

    // bool just_cpy = orig_len < min_data_size;
    bool just_cpy = (ngroups == 0) && remaining_len < min_data_size;
    // just_cpy = just_cpy || orig_len & (((uint64_t)1) << 47);
    if (just_cpy) { // if data was too small or failed to compress
        // printf("decomp: data less than min data size: %lu\n", min_data_size);
        memcpy(dest, src, remaining_len * elem_sz);
        return remaining_len;
    }
    if (ndims == 0) {
        perror("ERROR: Received ndims of 0!");
        return 0;
    }

    // ================================ one-time initialization

    // ------------------------ stats derived from ndims
    // header stats
    uint32_t nheader_vals = ndims * group_sz_blocks;
    uint32_t nheader_stripes = DIV_ROUND_UP(nheader_vals, stripe_sz);
    uint32_t total_header_bits = ndims * nbits_sz_bits * group_sz_blocks;
    uint32_t total_header_bytes = DIV_ROUND_UP(total_header_bits, 8);

    // final header can be shorter than others; construct mask so we don't
    // read in start of packed data as header bytes
    uint8_t remaining_header_sz = total_header_bytes % stripe_header_sz;
    uint8_t final_header_sz = remaining_header_sz ? remaining_header_sz : stripe_header_sz;
    uint32_t shift_bits = 8 * (4 - final_header_sz);
    uint32_t final_header_mask = ((uint32_t)0xffffffff) >> shift_bits;

    // stats for main decompression loop
    // uint32_t group_sz = ndims * group_sz_per_dim;
    uint16_t nstripes = DIV_ROUND_UP(ndims, stripe_sz);
    uint32_t padded_ndims = round_up_to_multiple(ndims, vector_sz);
    uint16_t nvectors = DIV_ROUND_UP(padded_ndims, vector_sz);

    // stats for sizing temp storage
    uint32_t nstripes_in_group = nstripes * group_sz_blocks;
    uint32_t group_header_sz = round_up_to_multiple(
        nstripes_in_group * stripe_sz, vector_sz);
    uint32_t nstripes_in_vectors = group_header_sz / stripe_sz;
    uint16_t nvectors_in_group = group_header_sz / vector_sz;

    // ------------------------ temp storage
    // allocate temp vars of minimal possible size such that we can
    // do vector loads and stores (except bitwidths, which are u64s so
    // that we can store directly after sad_epu8)
    uint64_t* headers_tmp       = (uint64_t*)calloc(nheader_stripes, 8);
    uint8_t*  headers           = (uint8_t*) calloc(1, group_header_sz);
    uint64_t* data_masks        = (uint64_t*)calloc(nstripes_in_vectors * elem_sz, 8);
    bitwidth_t* stripe_bitwidths= (bitwidth_t*)calloc(nstripes_in_vectors, 8);
    uint32_t* stripe_bitoffsets = (uint32_t*)calloc(nstripes, 4);

    // extra row in deltas is to store last decoded values
    // TODO just special case very first row
    int_t* deltas = (int_t*)calloc(block_sz * padded_ndims, elem_sz);
    uint_t* prev_vals_ar = (uint_t*)calloc(padded_ndims, elem_sz);

    // ================================ main loop

    // printf("got to start of main loop without segfaulting!\n");

    for (uint64_t g = 0; g < ngroups; g++) {
        const uint8_t* header_src = (const uint8_t*)src;
        src = (int_t*)(((int8_t*)src) + total_header_bytes);

        // printf("==== group %d\n", (int)g);

        // ------------------------ create unpacked headers array
        // unpack headers for all the blocks; note that this is tricky
        // because we need to zero-pad final stripe's headers in each block
        // so that stripe widths don't get messed up (from initial data in
        // next block's header)
        uint64_t* header_write_ptr = (uint64_t*)headers_tmp;
        for (uint32_t stripe = 0; stripe < nheader_stripes - 1; stripe++) {
            uint64_t packed_header = *(uint32_t*)header_src;
            header_src += stripe_header_sz;
            uint64_t header = _pdep_u64(packed_header, kHeaderUnpackMask);
            *header_write_ptr = header;
            header_write_ptr++;
        }
        // unpack header for the last stripe in the last block
        uint64_t packed_header = (*(uint32_t*)header_src) & final_header_mask;
        uint64_t header = _pdep_u64(packed_header, kHeaderUnpackMask);
        *header_write_ptr = header;

        // insert zeros between the unpacked headers so that the stripe
        // bitwidths, etc, are easy to compute
        uint8_t* header_in_ptr = (uint8_t*)headers_tmp;
        uint32_t header_pad_nstripes = DIV_ROUND_UP(ndims, stripe_sz);
        uint32_t header_padded_ndims = header_pad_nstripes * stripe_sz;
        for (uint32_t b = 0; b < group_sz_blocks; b++) {
            uint32_t src_offset = b * ndims;
            uint32_t dest_offset = b * header_padded_ndims;
            memcpy(headers + dest_offset, header_in_ptr + src_offset, ndims);
        }

        if (debug) { printf("padded headers:"); dump_bytes(headers, nstripes_in_vectors * 8); }

        // ------------------------ masks and bitwidths for all stripes
        for (uint32_t v = 0; v < nvectors_in_group; v++) {
            uint32_t v_offset = v * vector_sz_nbytes;
            __m256i raw_header = _mm256_loadu_si256(
                (const __m256i*)(headers + v_offset));

            if (elem_sz == 1) {
                // map nbits of 7 to 8
                static const __m256i sevens = _mm256_set1_epi8(0x07);
                __m256i header = _mm256_sub_epi8(
                    raw_header, _mm256_cmpeq_epi8(raw_header, sevens));

                // compute and store bit widths
                __m256i bitwidths = _mm256_sad_epu8(
                    header, _mm256_setzero_si256());
                uint8_t* store_addr = ((uint8_t*)stripe_bitwidths) + v_offset;
                _mm256_storeu_si256((__m256i*)store_addr, bitwidths);

                // compute and store masks
                __m256i masks = _mm256_shuffle_epi8(nbits_to_mask_8b, raw_header);
                uint8_t* store_addr2 = ((uint8_t*)data_masks) + v_offset;
                _mm256_storeu_si256((__m256i*)store_addr2, masks);
            } else if (elem_sz == 2) {
                // map nbits of 15 to 16
                static const __m256i fifteens = _mm256_set1_epi8(15);
                __m256i header = _mm256_sub_epi8(
                    raw_header, _mm256_cmpeq_epi8(raw_header, fifteens));

                // compute and store bitwidths
                __m256i u32_masks = _mm256_set1_epi64x(0xffffffff);
                __m256i even_u32s = _mm256_and_si256(u32_masks, header);
                __m256i odd_u32s = _mm256_andnot_si256(u32_masks, header);

                __m256i even_bitwidths = _mm256_sad_epu8(
                    even_u32s, _mm256_setzero_si256());
                __m256i odd_bitwidths = _mm256_sad_epu8(
                    odd_u32s, _mm256_setzero_si256());

                __m256i bitwidths = _mm256_or_si256(even_bitwidths,
                    _mm256_slli_epi64(odd_bitwidths, 32));

                uint8_t* store_addr = ((uint8_t*)stripe_bitwidths) + v_offset;
                _mm256_storeu_si256((__m256i*)store_addr, bitwidths);

                // compute and store masks
                __m256i masks0 = _mm256_undefined_si256();
                __m256i masks1 = _mm256_undefined_si256();
                mm256_shuffle_epi8_to_epi16(
                    nbits_to_mask_16b_low, nbits_to_mask_16b_high,
                    raw_header, masks0, masks1);
                uint8_t* store_addr2 = ((uint8_t*)data_masks) + 2*v_offset;
                _mm256_storeu_si256((__m256i*)store_addr2, masks0);
                _mm256_storeu_si256((__m256i*)(store_addr2 + vector_sz_nbytes), masks1);
            }
        }

        if (debug) {
            printf("padded masks:     "); dump_elements((uint16_t*)data_masks, group_header_sz);
            // printf("padded masks:     "); dump_bytes((uint8_t*)data_masks, group_header_sz * elem_sz);
            printf("padded bitwidths: "); dump_bytes(stripe_bitwidths, nstripes_in_vectors);
            printf("\n");
        }
        // ------------------------ inner loop; decompress each block
        uint64_t* masks = data_masks;
        bitwidth_t* bitwidths = stripe_bitwidths;
        for (int b = 0; b < group_sz_blocks; b++) { // for each block in group
            // compute where each stripe begins, as well as width of a row
            stripe_bitoffsets[0] = 0;
            for (uint32_t stripe = 1; stripe < nstripes; stripe++) {
                stripe_bitoffsets[stripe] = (uint32_t)(stripe_bitoffsets[stripe - 1]
                    + bitwidths[stripe - 1]);
            }
            uint32_t in_row_nbits = (uint32_t)(stripe_bitoffsets[nstripes - 1] +
                bitwidths[nstripes - 1]);
            uint32_t in_row_nbytes = DIV_ROUND_UP(in_row_nbits, 8);
            uint32_t out_row_nbytes = padded_ndims * elem_sz;

            if (in_row_nbits == 0) {
                int8_t* src8 = (int8_t*)src;
                int8_t low_byte = *src8;
                uint8_t high_byte = (uint8_t)*(src8 + 1);
                high_byte = high_byte & (low_byte >> 7); // 0 if low msb == 0
                uint16_t length = (low_byte & 0x7f) | (((uint16_t)high_byte) << 7);

                // write out the run
                uint32_t ncopies = length * block_sz;
                if (g > 0 || b > 0) { // if not at very beginning of data
                    const uint_t* inptr = dest - ndims;
                    for (int32_t v = nvectors - 1; v >= 0; v--) {
                        uint32_t vstripe_start = v * vector_sz;
                        __m256i prev_vals = _mm256_loadu_si256((const __m256i*)
                            (prev_vals_ar + vstripe_start));
                        func(v, prev_vals, prev_vals, ncopies);
                    }
                    if (DoWrite) {
                        memrep(dest, inptr, ndims * elem_sz, ncopies);
                    }
                    dest += ndims * ncopies;
                } else { // deltas of 0 at very start -> all zeros
                    uint32_t num_zeros = ncopies * ndims;
                    __m256i prev_vals = _mm256_setzero_si256();
                    for (int32_t v = nvectors - 1; v >= 0; v--) {
                        func(v, prev_vals, prev_vals, ncopies);
                    }
                    memset(dest, 0, num_zeros * elem_sz); // TODO mul by elem_sz is right ?
                    dest += num_zeros;
                }
                // printf("decompressed rle block of length %d at offset %d\n", length, (int)(dest - orig_dest));

                src8++;
                src8 += (high_byte > 0); // if 0, wasn't used for run length
                src = (int_t*)src8;

                masks += nstripes;
                bitwidths += nstripes;
                continue;
            }


            // ------------------------ unpack data for each stripe
            // for (uint32_t stripe = 0; stripe < nstripes; stripe++) {
            for (int stripe = nstripes - 1; stripe >= 0; stripe--) {
                uint32_t offset_bits = stripe_bitoffsets[stripe] & 0x07;
                uint32_t offset_bytes = stripe_bitoffsets[stripe] >> 3;

                uint64_t mask = masks[stripe];
                uint8_t nbits = bitwidths[stripe];
                uint8_t total_bits = nbits + offset_bits;

                const int8_t* inptr = ((const int8_t*)src) + offset_bytes;
                uint8_t* outptr = ((uint8_t*)deltas) + (stripe * stripe_nbytes);

                // this is the hot loop
                if (total_bits <= 64) { // input guaranteed to fit in 8B
                    for (int i = 0; i < block_sz; i++) {
                        uint64_t packed_data = (*(uint64_t*)inptr) >> offset_bits;
                        *(uint64_t*)outptr = _pdep_u64(packed_data, mask);
                        inptr += in_row_nbytes;
                        outptr += out_row_nbytes;
                    }
                } else { // input spans 9 bytes
                    uint8_t nbits_lost = total_bits - 64;
                    for (int i = 0; i < block_sz; i++) {
                        uint64_t packed_data = (*(uint64_t*)inptr) >> offset_bits;
                        packed_data |= (*(uint64_t*)(inptr + 8)) << (nbits - nbits_lost);
                        *(uint64_t*)outptr = _pdep_u64(packed_data, mask);
                        inptr += in_row_nbytes;
                        outptr += out_row_nbytes;
                    }
                }
            } // for each stripe

            // zigzag + delta decode
            for (int32_t v = nvectors - 1; v >= 0; v--) {
                uint32_t vstripe_start = v * vector_sz;
                __m256i prev_vals = _mm256_loadu_si256((const __m256i*)
                    (prev_vals_ar + vstripe_start));
                // if (debug) { printf("========\ninitial prev vals: "); dump_m256i(prev_vals); }
                __m256i vals = _mm256_undefined_si256();
                for (uint8_t i = 0; i < block_sz; i++) {
                    uint32_t in_offset = i * padded_ndims + vstripe_start;
                    uint32_t out_offset = i * ndims + vstripe_start;

                    __m256i raw_vdeltas = _mm256_loadu_si256(
                        (const __m256i*)(deltas + in_offset));

                    __m256i vdeltas = _mm256_undefined_si256();
                    if (elem_sz == 1) {
                        vdeltas = mm256_zigzag_decode_epi8(raw_vdeltas);
                        vals = _mm256_add_epi8(prev_vals, vdeltas);
                    } else if (elem_sz == 2) {
                        vdeltas = mm256_zigzag_decode_epi16(raw_vdeltas);
                        vals = _mm256_add_epi16(prev_vals, vdeltas);
                    }

                    func(v, prev_vals, vals);

                    if (DoWrite) {
                        _mm256_storeu_si256((__m256i*)(dest + out_offset), vals);
                    }
                    // if (debug) {
                    //     printf("---- row %d\n", i);
                    //     printf("deltas: "); dump_m256i<int16_t>(vdeltas);
                    //     // printf("prev vals:   "); dump_m256i<uint16_t>(prev_vals);
                    //     // printf("vals:   "); dump_m256i<uint16_t>(vals);
                    // }
                    prev_vals = vals;
                }
                _mm256_storeu_si256((__m256i*)(prev_vals_ar+vstripe_start), vals);
            }

            src += block_sz * in_row_nbytes / elem_sz;
            dest += block_sz * ndims;
            masks += nstripes;
            bitwidths += nstripes;
        } // for each block
    } // for each group

    // printf("got to end of main loop without segfaulting! about to free stuff...\n");

    // printf("headers_tmp = %p, headers = %p\n", headers_tmp, headers);

    free(headers_tmp);
    // printf("freed headers_tmp\n");
    free(headers);
    // printf("freed headers\n");
    free(data_masks);
    // printf("freed data masks\n");
    free(stripe_bitwidths);
    // printf("freed stripe_bitwidths\n");
    free(stripe_bitoffsets);
    // printf("freed most stuff; about to free deltas and prev_vals\n");
    free(deltas);
    free(prev_vals_ar);

    // printf("bytes read: %lld\n", (long long)(src - orig_src));

    // uint32_t remaining_len = orig_len - (dest - orig_dest);

    // copy over trailing data
    // uint32_t remaining_len = orig_len % group_sz;
    // uint32_t remaining_len = orig_len - (src - orig_src);
    // printf("remaining len: %d\n", (int)remaining_len);
    // printf("read bytes: %lu\n", remaining_len);
    // printf("remaining data: "); ar::print(src, remaining_len);
    if (debug) { printf("remaining len: %d\n", remaining_len); }
    if (DoWrite) {
        memcpy(dest, src, remaining_len * elem_sz);
    }

    if (debug > 2) {
        // printf("decompressed data:\n"); dump_bytes(orig_dest, orig_len * elem_sz, ndims * 4);
        printf("decompressed data:\n"); dump_elements(orig_dest, (int)(dest - orig_dest), ndims);
    }
    return dest + remaining_len - orig_dest;
}
