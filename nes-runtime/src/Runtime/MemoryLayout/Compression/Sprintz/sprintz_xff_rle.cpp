//
//  sprintz_xff_rle.cpp
//  Compress
//
//  Created by DB on 12/15/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#include "Runtime/MemoryLayout/Compression/Sprintz/sprintz_xff.h"

#include <stdio.h>

#include <assert.h>
#include <stdint.h>
#include <string.h>

#include "Runtime/MemoryLayout/Compression/Sprintz/bitpack.h"
#include "Runtime/MemoryLayout/Compression/Sprintz/format.h"
#include "Runtime/MemoryLayout/Compression/Sprintz/util.h"// for copysign

// byte shuffle values to construct data masks; note that nbits == 7 yields
// a byte of all ones (0xff); also note that rows 1 and 3 below are unused
// static const __m256i nbits_to_mask = _mm256_setr_epi8(
//                                                       0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0xff,
//                                                       0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // unused
//                                                       0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0xff,
//                                                       0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00); // unused


// TODO shuffle LUTs below should be in a shared header (prolly bitpack.h)

// byte shuffle values to construct data masks; note that nbits == 7 yields
// a byte of all ones (0xff); also note that rows 1 and 3 below are unused
// static const __m256i nbits_to_mask_8b = _mm256_setr_epi8(
//     0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0xff,
//     0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // unused
//     0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0xff,
//     0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00); // unused

// static const __m256i nbits_to_mask_16b_low = _mm256_setr_epi8(
//     0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff,   // 0-8
//     0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,               // 9-15
//     0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0x7f, 0xff,   // 0-8
//     0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff);              // 9-15
// static const __m256i nbits_to_mask_16b_high = _mm256_setr_epi8(
//     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,         // 0-7
//     0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0xff,         // 8-15
//     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,         // 0-7
//     0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0xff);        // 8-15


static const int kDefaultGroupSzBlocks = 2;

static const int debug = 0;
// static const int debug = 3;
// static const int debug = 4;

// ================================================================ xff + rle

template<typename int_t, typename uint_t>
int64_t compress_rowmajor_xff_rle(const uint_t* src, uint32_t len,
    int_t* dest, uint16_t ndims, bool write_size)
{
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(uint_t);
    static const uint8_t elem_sz_nbits = 8 * elem_sz;
    static const uint8_t nbits_sz_bits = elem_sz == 1 ? 3 : 4; // XXX only {8,16}b
    typedef typename ElemSzTraits<elem_sz>::counter_t counter_t;
    // constants that could actually be changed in this impl
    static const uint8_t group_sz_blocks = kDefaultGroupSzBlocks;
    static const uint16_t max_run_nblocks = 0x7fff; // 15 bit counter
    // xff constants
    // static const uint8_t learning_shift = elem_sz == 1 ? 1 : 3;
    static const uint8_t learning_shift = 1;
    static const uint8_t log2_learning_downsample = 1;
    static const uint8_t learning_downsample = 1 << log2_learning_downsample;
    // misc constants
    static const uint8_t log2_block_sz = 3;
    static const uint8_t stripe_sz_nbytes = 8;
    static const uint8_t length_header_nbytes = 8; // XXX indirect to format.h
    // misc derived constants
    static const uint8_t block_sz = 1 << log2_block_sz;
    static const uint32_t min_data_size = 8 * block_sz * group_sz_blocks;
    static const uint8_t stripe_sz = stripe_sz_nbytes / elem_sz;

    // const uint8_t* orig_src = src;
    // int8_t* orig_dest = dest;
    // const uint8_t* src_end = src + len;
    const uint_t* orig_src = src;
    const uint_t* src_end = src + len;
    int_t* orig_dest = dest;

    int gDebug = debug;
    int debug = (elem_sz == 2) ? gDebug : 0;

    // ================================ one-time initialization

    // ------------------------ stats derived from ndims
    uint16_t nstripes = DIV_ROUND_UP(ndims, stripe_sz);
    uint32_t group_sz = ndims * block_sz * group_sz_blocks;
    uint32_t total_header_bits = ndims * nbits_sz_bits * group_sz_blocks;
    uint32_t total_header_bytes = DIV_ROUND_UP(total_header_bits, 8);

    // ------------------------ store data size and number of dimensions

    if (debug > 0) {
        printf("-------- compression (len = %lld)\n", (long long)len);
        if (debug > 2) {
            printf("saw original data:\n"); dump_elements(src, len, ndims);
        }
    }
    if (debug > 1) { printf("total header bits, bytes: %d, %d\n", total_header_bits, total_header_bytes); }

    // handle low dims and low length; we'd read way past the end of the
    // input in this case
    if (len < min_data_size) {
        assert(min_data_size < ((uint32_t)1) << 16);
        if (debug) { printf("data less than min data size: %u\n", min_data_size); }
        if (write_size) {
            dest += write_metadata_rle(dest, ndims, 0, (uint16_t)len);
        }
        memcpy(dest, src, len * elem_sz);
        return (dest - orig_dest) + len;
    }
    if (write_size) {
        dest += length_header_nbytes / elem_sz;  // XXX needs to div evenly
    }

    // printf("------------ comp (len = %lld)\n", (int64_t)len);
    // // printf("saw original data:\n"); dump_bytes(src, len, ndims);
    // printf("saw original data:\n"); dump_bytes(src, len, ndims * 4);
    // // printf("saw original data:\n"); dump_bytes(src, len, 16);

    // ------------------------ temp storage
    uint8_t*  stripe_bitwidths  = (uint8_t*) malloc(nstripes*sizeof(uint8_t));
    uint32_t* stripe_bitoffsets = (uint32_t*)malloc(nstripes*sizeof(uint32_t));
    uint64_t* stripe_masks      = (uint64_t*)malloc(nstripes*sizeof(uint64_t));
    uint32_t* stripe_headers    = (uint32_t*)malloc(nstripes*sizeof(uint32_t));

    uint32_t total_header_bytes_padded = total_header_bytes + 4;
    uint8_t* header_bytes = (uint8_t*)calloc(total_header_bytes_padded, 1);

    // contiguous allocation of xff stats
    // int8_t* errs = (int8_t*)calloc(1, (block_sz + 4) * ndims);
    // uint8_t* prev_vals_ar   = (uint8_t*)(errs + block_sz * ndims);
    // int8_t* prev_deltas_ar  = (int8_t* )(errs + (block_sz + 1) * ndims);
    // int16_t* coef_counters_ar=(int16_t*)(errs + (block_sz + 2) * ndims);

    int_t*  errs             = (int_t* )calloc((block_sz + 2) * ndims, elem_sz);
    uint_t* prev_vals_ar     = (uint_t*)(errs + (block_sz + 0) * ndims);
    int_t*  prev_deltas_ar   = (int_t* )(errs + (block_sz + 1) * ndims);
    counter_t* coef_counters_ar = (counter_t*)calloc(ndims, sizeof(counter_t));

    // ================================ main loop

    uint16_t run_length_nblocks = 0;

    const uint_t* last_full_group_start = src_end - group_sz;
    uint32_t ngroups = 0;
    while (src <= last_full_group_start) {
        ngroups++;  // invariant: groups we start are always finished

        // int8_t* header_dest = dest;
        // dest += total_header_bytes;
        int8_t* header_dest = (int8_t*)dest;
        dest = (int_t*)(((int8_t*)dest) + total_header_bytes);

        // printf("==== group %3d\t(%d -> %d)\n",
        //     (int)(ngroups - 1), (int)(src - orig_src), (int)(dest - orig_dest - length_header_nbytes));

        // debug = debug && (ngroups <= 20);
        // debug = gDebug && (elem_sz == 2) && (ngroups > 16);

        memset(header_bytes, 0, total_header_bytes_padded);
        memset(header_dest, 0, total_header_bytes);

        uint32_t header_bit_offset = 0;
        int b = 0;
        while (b < group_sz_blocks) { // for each block

            if (debug) { printf("---- %d.%d\n", (int)ngroups - 1, b); }

            // ------------------------ zero stripe info from previous iter
            memset(stripe_bitwidths, 0, nstripes * sizeof(stripe_bitwidths[0]));
            memset(stripe_masks,     0, nstripes * sizeof(stripe_masks[0]));
            memset(stripe_headers,   0, nstripes * sizeof(stripe_headers[0]));

            // ------------------------ compute info for each stripe

            // auto deltas = (int8_t*)calloc(ndims * block_sz, 1); // TODO rm
            // auto predictions = (int8_t*)calloc(ndims * block_sz, 1); // TODO rm
            // auto grads = (int8_t*)calloc(ndims, 1); // TODO rm
            // // auto grad_sums = (int8_t*)calloc(ndims * block_sz / learning_downsample, 1); // TODO rm
            // auto grad_sums = (int8_t*)calloc(ndims * block_sz, 1); // TODO rm
            // auto all_grads = (int8_t*)calloc(ndims * block_sz, 1); // TODO rm

            for (uint16_t dim = 0; dim < ndims; dim++) {
                // compute maximum number of bits used by any value of this dim,
                // while simultaneously computing deltas
                // uint8_t mask = 0;
                // uint8_t prev_val = prev_vals_ar[dim];
                // int8_t prev_delta = prev_deltas_ar[dim];
                uint_t mask = 0;
                uint_t prev_val = prev_vals_ar[dim];
                int_t prev_delta = prev_deltas_ar[dim];

                // int16_t coef = (coef_counters_ar[dim] >> (learning_shift + 4)) << 4;
                // int8_t grad_sum = 0;
                const uint8_t shft = elem_sz_nbits - 4;
                counter_t counter = coef_counters_ar[dim];

                // XXX 16b coef matches simd impl, but means that it will
                // be very hard for 16b data to get double delta coded; i.e.:
                //  floor(32767 / (2^12)) * 2^(12) = 28,672
                // which is only 14/15 of actual double delta coding
                //
                int16_t coef = (coef_counters_ar[dim] >> (learning_shift + shft)) << shft;
                // counter_t coef = (coef_counters_ar[dim] >> (learning_shift + shft)) << shft;
                int_t grad_sum = 0;

                for (uint8_t i = 0; i < block_sz; i++) {
                    uint32_t offset = (i * ndims) + dim;
                    uint_t val = src[offset];
                    int_t delta = (int_t)(val - prev_val);
                    int_t prediction = (((counter_t)prev_delta) * coef) >> elem_sz_nbits;

                    // TODO I think we actually want this, but for now rm
                    // it since it complicates the paper and we don't have
                    // it implemented in the lowdim version of xff
                    //
                    // if (elem_sz == 2) {
                    //     // can't just rshift by one less since that wouldn't
                    //     // zero the LSB
                    //     prediction = prediction << 2;
                    // }

                    int_t err = delta - prediction;
                    uint_t bits = ZIGZAG_ENCODE_SCALAR(err);

                    if (i % learning_downsample == learning_downsample - 1) {
                        grad_sum += icopysign(err, prev_delta);
                        if (debug) {
                            printf("prev delta, err:\t%d\t\t%d\n", prev_delta, err);
                        }
                    }
                    // int8_t grad = copysign_i8(err, prev_delta);
                    // grad_sums[(i * ndims) + dim] = grad_sum;
                    // all_grads[(i * ndims) + dim] = grad;
                    // deltas[offset] = delta; // TODO rm
                    // predictions[offset] = prediction; // TODO rm

                    mask |= bits;
                    errs[offset] = bits;
                    prev_val = val;
                    prev_delta = delta;
                }
                // write out value for delta encoding of next block
                // mask = NBITS_MASKS_U8[mask];
                if (elem_sz == 1) {
                    mask = NBITS_MASKS_U8[mask];
                } else if (elem_sz == 2) {
                    uint8_t upper_mask = NBITS_MASKS_U8[mask >> 8];
                    mask = upper_mask > 0 ? (upper_mask << 8) + 255 : NBITS_MASKS_U8[mask];
                    // mask = 0xffff; // TODO rm
                }
                // mask = mask ? NBITS_MASKS_U8[255] : 0; // TODO rm
                prev_vals_ar[dim] = prev_val;
                prev_deltas_ar[dim] = prev_delta;

                // update prediction coefficient
                // int8_t grad = grad_sum >> (log2_block_sz - log2_learning_downsample);
                // coef_counters_ar[dim] += grad;
                static const uint8_t shift_to_get_mean =
                    log2_block_sz - log2_learning_downsample;
                coef_counters_ar[dim] += grad_sum >> shift_to_get_mean;

                if (debug) {
                    printf("coef: %d\n", coef);
                    printf("grad_sum: %d\n", grad_sum);
                    printf("counter: %d\n", coef_counters_ar[dim]);
                }

                // if (mask > 0) { mask = NBITS_MASKS_U8[255]; } // TODO rm
                uint8_t max_nbits = (32 - _lzcnt_u32((uint32_t)mask));

                // printf("\tmax nbits: %d\n", max_nbits);

                uint16_t stripe = dim / stripe_sz;
                uint8_t idx_in_stripe = dim % stripe_sz;

                // accumulate stats about this stripe
                stripe_bitwidths[stripe] += max_nbits;
                stripe_masks[stripe] |= ((uint64_t)mask) << (idx_in_stripe * elem_sz_nbits);

                // accumulate header info for this stripe
                uint32_t write_nbits = max_nbits - (max_nbits == elem_sz_nbits); // map 8 to 7
                stripe_headers[stripe] |= write_nbits << (idx_in_stripe * nbits_sz_bits);
            }

            // TODO rm all this
            // printf("zigzagged errs:\n"); dump_elements(errs, ndims*block_sz, ndims);
            // printf("predictions:\n"); dump_elements(predictions, ndims*block_sz, ndims);
            // if (b == 0) {
            // if (ngroups > 9) {
            //     printf("--- b=%d\n", b);
            //     printf("stripe nbits: %d\n", stripe_bitwidths[0]);
            //     printf("src block:\n"); dump_elements(src, ndims*block_sz, ndims);
            //     // printf("deltas:\n"); dump_elements(deltas, ndims*block_sz, ndims);
            //     // printf("all grads:\n"); dump_elements(all_grads, ndims * block_sz, ndims);
            //     // printf("grad_sums:\n"); dump_elements(grad_sums, ndims * block_sz, ndims);
            //     // printf("mean grads:\n"); dump_elements(grads, ndims, ndims);
            //     printf("coef counters:\n"); dump_elements(coef_counters_ar, ndims, ndims);
            //     int8_t coeffs[3];
            //     for (int dim = 0; dim < 3; dim++) {
            //         coeffs[dim] = (coef_counters_ar[dim] >> (learning_shift + 4)) << 4;
            //     }
            //     printf("coeffs:\n"); dump_elements(coeffs, 3, 3);
            // }
            // free(predictions);
            // free(deltas);
            // free(grads);
            // free(grad_sums);
            // free(all_grads);
            // free(coef_counters_ar);

            // compute start offsets of each stripe (in bits)
            stripe_bitoffsets[0] = 0;
            for (uint32_t stripe = 1; stripe < nstripes; stripe++) {
                stripe_bitoffsets[stripe] = stripe_bitoffsets[stripe - 1] +
                    stripe_bitwidths[stripe - 1];
            }
            // compute width of each row (in bytes); note that we byte align
            uint32_t row_width_bits = stripe_bitoffsets[nstripes - 1] +
                stripe_bitwidths[nstripes-1];
            uint32_t out_row_nbytes = DIV_ROUND_UP(row_width_bits, 8);

            // printf("mask for zigzag val of 2: %d\n", NBITS_MASKS_U8[2]);
            // printf("stripe bitwidth 0: %d\n", stripe_bitwidths[0]);
            // printf("--- block %d, in offset = %d, row_width_bits = %u\n", b, (int)(src - orig_src), row_width_bits);

just_read_block:

            // if (ngroups > 10) {
            //     printf("block %3d\t(%d -> %d)\n", b, (int)(src - orig_src),
            //         (int)(dest - orig_dest - length_header_nbytes));
            //     printf("row_width_bits = %d\n", row_width_bits);
            // }

            // ------------------------ handle runs of zeros
            bool do_rle = row_width_bits == 0 && run_length_nblocks < max_run_nblocks;

            // printf("row width bits: %d\n", row_width_bits);
            // do_rle = false;

            if (do_rle) {
do_rle:
                run_length_nblocks++; // TODO uncomment
                src += block_sz * ndims;

                // printf("************************ trying to encode a run!\n");

                if (src <= last_full_group_start) {
                    continue; // still enough data to finish group
                } else { // not enough data to finish group
                    // write out headers for this block; they're all zero, so
                    // just increment addr at which to write next time
                    header_bit_offset += ndims * nbits_sz_bits;

                    if (debug) {
                        printf("aborting and compressing rle block of length %d ending at src offset %d\n", run_length_nblocks, (int)(src - orig_src));
                        printf("coefs: "); dump_elements(coef_counters_ar, ndims, ndims);
                    }

                    b++; // we're finishing off this block

                    /// write out length of the current constant section
                    uint8_t* dest_u8 = (uint8_t*)dest;
                    *dest_u8 = run_length_nblocks & 0x7f; // bottom 7 bits
                    dest_u8++;
                    if (run_length_nblocks > 0x7f) { // need another byte
                        *(dest_u8-1) |= 0x80; // set MSB of previous byte
                        *dest_u8 = (uint8_t)(run_length_nblocks >> 7);
                        dest_u8++;
                    }

                    // write out this const section, and use empty const
                    // sections to fill up rest of block
                    for (; b < group_sz_blocks; b++) {
                        *dest_u8 = 0;
                        dest_u8++;
                    };

                    // we shouldn't need this write, but makes our invariants
                    // consistent
                    run_length_nblocks = 0;

                    dest = (int_t*)dest_u8;
                    goto main_loop_end; // just memcpy remaining data
                }
            }

            if (run_length_nblocks > 0) { // just finished a run
                if (debug) {
                    printf("%d.%d: compressing rle block of length %d ending at offset %d\n",
                        (int)ngroups - 1, b, run_length_nblocks, (int)(src - orig_src));
                }
                b++;

                uint8_t* dest_u8 = (uint8_t*)dest;
                *dest_u8 = run_length_nblocks & 0x7f; // bottom 7 bits
                dest_u8++;
                if (run_length_nblocks > 0x7f) { // need another byte
                    *(dest_u8-1) |= 0x80; // set MSB of previous byte
                    *dest_u8 = (uint8_t)(run_length_nblocks >> 7);
                    dest_u8++;
                }
                dest = (int_t*)dest_u8;

                run_length_nblocks = 0;

                // write out header (equivalent since already zeroed)
                header_bit_offset += ndims * nbits_sz_bits;

                // if closing the run finished this block, life is hard; we
                // can't just execute the below code because it would try to
                // write past the end of the header bytes and write a data
                // block the decoder isn't expecting; handle this case by
                // just feeding the current block through again as part of
                // the next block; to do this, we need to reset `prev_vals`
                if (b == group_sz_blocks) {
                    // printf("reseting prev vals...\n");
                    // memcpy(prev_vals_ar, src - ndims, ndims);
                    // continue;

                    // printf("**** weird case: starting new group!\n");
                    // start new group, and pretend the block we read
                    // was the first block in that group (which it is now)
                    ngroups++;  // invariant: groups we start are always finished
                    header_bit_offset = 0;
                    b = 0;
                    header_dest = (int8_t*)dest;
                    dest = (int_t*)(((int8_t*)dest) + total_header_bytes);
                    memset(header_bytes, 0, total_header_bytes_padded);
                    memset(header_dest, 0, total_header_bytes);

                    // printf("==== group %d\t\t(%d -> %d)\n",
                    //     (int)(ngroups - 1), (int)(src - orig_src),
                    //     (int)(dest - orig_dest - length_header_nbytes));
                    goto just_read_block;
                }

                // have to enforce invariant that bitwidth of 0 always gets
                // run-length encoded; this case can happen when the current
                // block was zeros, but we hit the limit `max_run_nblocks`
                if (row_width_bits == 0) { goto do_rle; }
            }

            // ------------------------ write out header bits for this block
            for (uint32_t stripe = 0; stripe < nstripes; stripe++) {
                uint16_t byte_offset = header_bit_offset >> 3;
                uint16_t bit_offset = header_bit_offset & 0x07;

                if (elem_sz == 1) {
                    *(uint32_t*)(header_dest + byte_offset) |= \
                        stripe_headers[stripe] << bit_offset;
                } else {
                    *(uint64_t*)(header_dest + byte_offset) |= \
                        ((uint64_t)(stripe_headers[stripe])) << bit_offset;
                }

                uint8_t is_final_stripe = stripe == (nstripes - 1);
                uint8_t has_trailing_dims = (ndims % stripe_sz) != 0;
                uint8_t add_ndims = is_final_stripe && has_trailing_dims ?
                    ndims % stripe_sz : stripe_sz;
                header_bit_offset += nbits_sz_bits * add_ndims;
            }

            // ------------------------ write out block data

            // zero output so that we can just OR in bits (and also touch each
            // cache line in order to ensure that we prefetch, despite our
            // weird pattern of writes)
            memset(dest, 0, out_row_nbytes * block_sz); // above line can overrun dest buff

            // write out packed data; we iterate thru stripes in reverse order
            // since (nbits % stripe_sz) != 0 will make the last stripe in each
            // row write into the start of the first stripe in the next row
            for (int16_t stripe = 0; stripe < nstripes; stripe++) {
                // load info for this stripe
                uint8_t offset_bits = (uint8_t)(stripe_bitoffsets[stripe] & 0x07);
                uint32_t offset_bytes = stripe_bitoffsets[stripe] >> 3;
                uint64_t mask = stripe_masks[stripe];
                uint16_t nbits = stripe_bitwidths[stripe];
                uint16_t total_bits = nbits + offset_bits;

                int8_t* outptr = ((int8_t*)dest) + offset_bytes;
                const uint8_t* inptr = ((const uint8_t*)errs) + (stripe * stripe_sz_nbytes);

                if (total_bits <= 64) { // always fits in one u64
                    for (int i = 0; i < block_sz; i++) { // for each sample in block
                        uint64_t data = *(uint64_t*)inptr;
                        uint64_t packed_data = _pext_u64(data, mask);
                        uint64_t write_data = packed_data << offset_bits;
                        *(uint64_t*)outptr = write_data | (*(uint64_t*)outptr);

                        outptr += out_row_nbytes;
                        inptr += ndims * elem_sz;
                    }
                } else { // data spans 9 bytes
                    uint8_t nbits_lost = total_bits - 64;
                    for (int i = 0; i < block_sz; i++) { // for each sample in block
                        uint64_t data = *(uint64_t*)inptr;
                        uint64_t packed_data = _pext_u64(data, mask);
                        uint8_t extra_byte = (uint8_t)(packed_data >> (nbits - nbits_lost));
                        uint64_t write_data = packed_data << offset_bits;
                        *(uint64_t*)outptr = write_data | (*(uint64_t*)outptr);
                        *(outptr + 8) = extra_byte;

                        outptr += out_row_nbytes;
                        inptr += ndims * elem_sz;
                    }
                }
                // printf("read back header: "); dumpEndianBits(*(uint32_t*)(header_dest - stripe_header_sz));
            } // for each stripe
            src += block_sz * ndims;
            dest += block_sz * out_row_nbytes / elem_sz;

            // run_length_nblocks = 0;
            b++;
        } // for each block
    } // for each group

main_loop_end:

    free(stripe_bitwidths);
    free(stripe_bitoffsets);
    free(stripe_masks);
    free(stripe_headers);
    free(errs);

    // printf("wrote ngroups: %d\n", (int)ngroups);
    size_t remaining_len = src_end - src;
    if (write_size) {
        write_metadata_rle(orig_dest, ndims, ngroups, remaining_len);
        // *(uint32_t*)orig_dest = ngroups;
        // *(uint16_t*)(orig_dest + 4) = (uint16_t)remaining_len;
        // *(uint16_t*)(orig_dest + 6) = ndims;
    }
    if (debug)  printf("remaining_len len: %d\n", (int)remaining_len);
    // printf("remaining_len len: %d (%d -> %d)\n",
    //     (int)remaining_len, (int)(src - orig_src), (int)(dest - orig_dest));

    memcpy(dest, src, remaining_len * elem_sz);
    return dest + remaining_len - orig_dest;
}

int64_t compress_rowmajor_xff_rle_8b(const uint8_t* src, uint32_t len,
    int8_t* dest, uint16_t ndims, bool write_size)
{
    return compress_rowmajor_xff_rle(src, len, dest, ndims, write_size);
}
int64_t compress_rowmajor_xff_rle_16b(const uint16_t* src, uint32_t len,
    int16_t* dest, uint16_t ndims, bool write_size)
{
    return compress_rowmajor_xff_rle(src, len, dest, ndims, write_size);
}

template<typename int_t, typename uint_t>
SPRINTZ_FORCE_INLINE int64_t decompress_rowmajor_xff_rle(const int_t* src,
    uint_t* dest, uint16_t ndims, uint32_t ngroups, uint16_t remaining_len)
{
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(uint_t);
    static const uint8_t elem_sz_nbits = 8 * elem_sz;
    static const uint8_t nbits_sz_bits = elem_sz == 1 ? 3 : 4; // XXX {8,16}b
    typedef typename ElemSzTraits<elem_sz>::bitwidth_t bitwidth_t;
    [[maybe_unused]] typedef typename ElemSzTraits<elem_sz>::counter_t counter_t;
    // xff constants
    // static const uint8_t learning_shift = elem_sz == 1 ? 1 : 3;
    static const uint8_t learning_shift = 1;
    static const uint8_t log2_learning_downsample = 1;
    static const uint8_t learning_downsample = 1 << log2_learning_downsample;
    // constants that could actually be changed in this impl
    static const uint8_t group_sz_blocks = kDefaultGroupSzBlocks;
    // misc constants
    static const uint8_t log2_block_sz = 3;
    static const __m256i low_mask = _mm256_set1_epi16(0xff);
    static const __m256i low_mask_epi32 = _mm256_set1_epi32(0xffff);
    static const uint8_t stripe_nbytes = 8;
    static const uint8_t vector_sz_nbytes = 32;
    // misc derived constants
    static const uint8_t block_sz = 1 << log2_block_sz;
    static const size_t min_data_size = 8 * block_sz * group_sz_blocks;
    static const uint8_t nbits_sz_mask = (1 << nbits_sz_bits) - 1;
    static const uint64_t kHeaderUnpackMask = TILE_BYTE(nbits_sz_mask);
    static const uint8_t stripe_header_sz = nbits_sz_bits * stripe_nbytes / 8;
    static const uint8_t stripe_sz = stripe_nbytes / elem_sz;
    static const uint8_t vector_sz = vector_sz_nbytes / elem_sz;

    assert(vector_sz % stripe_sz == 0);
    assert(vector_sz >= stripe_sz);

    uint_t* orig_dest = dest;
    const int_t* orig_src = src;

    int gDebug = debug;
    int debug = (elem_sz == 2) ? gDebug : 0;

    if (debug > 0) {
        int32_t min_orig_len = ngroups * group_sz_blocks * block_sz * ndims;
        printf("--------- decomp: saw original ngroups, ndims, min_len = %u, %d, %d\n", ngroups, ndims, min_orig_len);
        if (debug > 3) {
            printf("saw compressed data (with possible extra at end):\n");
            dump_bytes(src, 32, ndims * elem_sz);
            // dump_bytes(((uint8_t*)src) + ndims, min_orig_len, ndims * elem_sz);
            // dump_elements(src, min_orig_len + 4, ndims);
        }
    }

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
    // uint8_t* deltas = (uint8_t*)calloc((block_sz + 1) * padded_ndims, 1);

    // int8_t* errs_ar = (int8_t*)calloc((block_sz + 4) * padded_ndims, 1);
    // uint8_t* prev_vals_ar = (uint8_t*)(errs_ar + (block_sz + 0) * padded_ndims);
    // int8_t* prev_deltas_ar = (int8_t*)(errs_ar + (block_sz + 1) * padded_ndims);
    // int8_t* coeffs_ar_even = (int8_t*)(errs_ar + (block_sz + 2) * padded_ndims);
    // int8_t* coeffs_ar_odd =  (int8_t*)(errs_ar + (block_sz + 3) * padded_ndims);

    int_t*  errs_ar         = (int_t* )calloc((block_sz + 2) * padded_ndims, elem_sz);
    uint_t* prev_vals_ar    = (uint_t*)(errs_ar + (block_sz + 0) * padded_ndims);
    int_t*  prev_deltas_ar  = (int_t* )(errs_ar + (block_sz + 1) * padded_ndims);
    uint_t* coeffs_ar_even  = (uint_t*)calloc(2 * padded_ndims, elem_sz);
    uint_t* coeffs_ar_odd   = coeffs_ar_even + padded_ndims;

    if (debug) printf("padded ndims: %d\n", padded_ndims);

    // ================================ main loop

    // uint64_t ngroups = orig_len / group_sz; // if we get an fp error, it's this
    for (uint64_t g = 0; g < ngroups; g++) {
        // const uint8_t* header_src = (uint8_t*)src;
        // src += total_header_bytes;
        const uint8_t* header_src = (const uint8_t*)src;
        src = (int_t*)(((int8_t*)src) + total_header_bytes);

        // printf("==== group %3d\t(%d -> %d)\n",
        //     (int)g, (int)(src - orig_src), (int)(dest - orig_dest));


        // debug = gDebug && (elem_sz == 2) && (g >= 16);


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

        // if (debug) { printf("padded headers:"); dump_bytes(headers, nstripes_in_vectors * 8); }

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

        // if (debug) {
        //     printf("padded masks:     "); dump_elements((uint16_t*)data_masks, group_header_sz);
        //     // printf("padded masks:     "); dump_bytes((uint8_t*)data_masks, group_header_sz * elem_sz);
        //     printf("padded bitwidths: "); dump_bytes(stripe_bitwidths, nstripes_in_vectors);
        //     printf("\n");
        // }
        // ------------------------ inner loop; decompress each block
        uint64_t* masks = data_masks;
        bitwidth_t* bitwidths = stripe_bitwidths;
        for (int b = 0; b < group_sz_blocks; b++) { // for each block in group

            // if (debug) { printf("---- %d.%d nbits: ", (int)g, b); dump_bytes(header_ptr, ndims); }
            if (debug) { printf("---- %d.%d\n", (int)g, b); }

            // if (g >= 2) {  printf("--- b = %d\n", b); }

            // if (g >= 10) {
            //     printf("block %3d\t(%d -> %d)\n", b, (int)(src - orig_src),
            //         (int)(dest - orig_dest));
            // }

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

            // if (in_row_nbits > 99999) { // TODO rm
            if (in_row_nbits == 0) {
                int8_t* src8 = (int8_t*)src;
                int8_t low_byte = *src8;
                uint8_t high_byte = (uint8_t)*(src8 + 1);
                high_byte = high_byte & (low_byte >> 7); // 0 if low msb == 0
                uint16_t length = (low_byte & 0x7f) | (((uint16_t)high_byte) << 7);

                // write out the run
                if (g > 0 || b > 0) { // if not at very beginning of data
                    const uint_t* inptr = dest - ndims;

                    for (int32_t bb = 0; bb < length; bb++) {
                        for (int32_t v = nvectors - 1; v >= 0; v--) {
                            uint32_t v_offset = v * vector_sz;
                            __m256i* prev_vals_ptr = (__m256i*)(prev_vals_ar + v_offset);
                            __m256i* prev_deltas_ptr = (__m256i*)(prev_deltas_ar + v_offset);
                            __m256i prev_vals = _mm256_loadu_si256(prev_vals_ptr);
                            __m256i prev_deltas = _mm256_loadu_si256(prev_deltas_ptr);
                            __m256i vals = _mm256_undefined_si256();

                            __m256i coef_counters_even = _mm256_loadu_si256(
                                (const __m256i*)(coeffs_ar_even + v_offset));
                            __m256i coef_counters_odd = _mm256_loadu_si256(
                                (const __m256i*)(coeffs_ar_odd + v_offset));

                            if (elem_sz == 1) {
                                // compute filter coeffs
                                // TODO store in an array to avoid recomputation
                                __m256i filter_coeffs_even  = _mm256_srai_epi16(
                                    coef_counters_even, learning_shift + 4);
                                __m256i filter_coeffs_odd  = _mm256_srai_epi16(
                                    coef_counters_odd, learning_shift + 4);
                                filter_coeffs_even = _mm256_slli_epi16(filter_coeffs_even, 4);
                                filter_coeffs_odd  = _mm256_slli_epi16(filter_coeffs_odd, 4);

                                __m256i even_prev_deltas = _mm256_srai_epi16(
                                    _mm256_slli_epi16(prev_deltas, 8), 8);
                                __m256i odd_prev_deltas = _mm256_srai_epi16(
                                    prev_deltas, 8);

                                for (uint8_t i = 0; i < block_sz; i++) {
                                    uint32_t out_offset = i * ndims + v_offset;

                                    __m256i even_predictions = _mm256_mullo_epi16(
                                        even_prev_deltas, filter_coeffs_even);
                                    __m256i odd_predictions = _mm256_mullo_epi16(
                                        odd_prev_deltas, filter_coeffs_odd);
                                    __m256i vpredictions = _mm256_blendv_epi8(odd_predictions,
                                        _mm256_srli_epi16(even_predictions, 8), low_mask);

                                    // if 0 err, predictions equal true deltas
                                    __m256i vdeltas = vpredictions;
                                    vals = _mm256_add_epi8(prev_vals, vdeltas);

                                    _mm256_storeu_si256((__m256i*)(dest + out_offset), vals);

                                    even_prev_deltas = _mm256_srai_epi16(
                                        even_predictions, 8);
                                    odd_prev_deltas = _mm256_srai_epi16(
                                        odd_predictions, 8);
                                    prev_deltas = vdeltas;
                                    prev_vals = vals;
                                } // for each row

                            } else if (elem_sz == 2) {
                                const uint8_t shft = elem_sz_nbits - 4;
                                __m256i filter_coeffs_even  = _mm256_srai_epi32(
                                    coef_counters_even, learning_shift + shft);
                                __m256i filter_coeffs_odd  = _mm256_srai_epi32(
                                    coef_counters_odd, learning_shift + shft);
                                __m256i filter_coeffs = _mm256_blendv_epi8(
                                    filter_coeffs_odd, filter_coeffs_even, low_mask_epi32);
                                filter_coeffs = _mm256_slli_epi16(
                                    filter_coeffs, elem_sz_nbits - shft);

                                for (uint8_t i = 0; i < block_sz; i++) {
                                    uint_t* out_ptr = dest + i * ndims + v_offset;

                                    __m256i vpredictions = _mm256_mulhi_epi16(
                                        prev_deltas, filter_coeffs);
                                    // vpredictions = _mm256_slli_epi16(vpredictions, 2);

                                    __m256i vdeltas = vpredictions; // since err of 0
                                    __m256i vals = _mm256_add_epi16(vdeltas, prev_vals);

                                    if (debug) { printf("prev vals: "); dump_m256i<int16_t>(prev_vals); }

                                    _mm256_storeu_si256((__m256i*)out_ptr, vals);
                                    prev_deltas = vdeltas;
                                    prev_vals = vals;
                                }
                                if (debug) {
                                    printf("\tbb=%d\n", bb);
                                    printf("prev_vals ptr; %p\n", (void*)prev_vals_ptr);
                                    printf("coeffs[0]: %d\n", (int16_t)_mm256_extract_epi16(filter_coeffs, 0));
                                    printf("prev_vals[0]: %d\n", (int16_t)_mm256_extract_epi16(prev_vals, 0));
                                    printf("prev_deltas[0]: %d\n", (int16_t)_mm256_extract_epi16(prev_deltas, 0));
                                    printf("vals[0]: %d\n", (int16_t)_mm256_extract_epi16(vals, 0));
                                    // printf("coeffs even: "); dump_m256i<int16_t>(coef_counters_even);
                                    printf("counter[0]: %d\n", (int32_t)_mm256_extract_epi32(coef_counters_even, 0));
                                }
                            }
                            _mm256_storeu_si256((__m256i*)prev_vals_ptr, prev_vals);
                            _mm256_storeu_si256((__m256i*)prev_deltas_ptr, prev_deltas);
                            // printf("filter coeffs even at end of block: "); dump_m256i(filter_coeffs_even);
                        } // for each vector
                        dest += ndims * block_sz;

                        // printf("dest block:\n");
                        // dump_elements(dest - ndims*block_sz*length, ndims*block_sz, ndims);

                    } // for each block in run
                } else { // deltas of 0 at very start -> all zeros
                    size_t num_zeros = length * block_sz * ndims;
                    memset(dest, 0, num_zeros * elem_sz);
                    dest += num_zeros;
                }

                if (debug) {
                    printf("%d.%d: decompressed rle block of length %d at offset %d\n",
                        (int)g, b, length, (int)(dest - orig_dest));
                }

                src8++;
                src8 += (high_byte > 0); // if 0, wasn't used for run length
                src = (int_t*)src8;

                masks += nstripes;
                bitwidths += nstripes;
                continue;
            }

            // ------------------------ unpack data for each stripe
            for (int stripe = nstripes - 1; stripe >= 0; stripe--) {
                uint32_t offset_bits = stripe_bitoffsets[stripe] & 0x07;
                uint32_t offset_bytes = stripe_bitoffsets[stripe] >> 3;

                uint64_t mask = masks[stripe];
                uint8_t nbits = bitwidths[stripe];
                uint8_t total_bits = nbits + offset_bits;

                const int8_t* inptr = ((const int8_t*)src) + offset_bytes;
                uint8_t* outptr = ((uint8_t*)errs_ar) + (stripe * stripe_nbytes);

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

            // ------------------------ zigzag + xff decode
            for (int32_t v = nvectors - 1; v >= 0; v--) {
                uint32_t v_offset = v * vector_sz;
                __m256i* prev_vals_ptr = (__m256i*)(prev_vals_ar + v_offset);
                __m256i* prev_deltas_ptr = (__m256i*)(prev_deltas_ar + v_offset);
                __m256i prev_vals = _mm256_loadu_si256(prev_vals_ptr);
                __m256i prev_deltas = _mm256_loadu_si256(prev_deltas_ptr);
                __m256i vals = _mm256_undefined_si256();

                __m256i* even_counters_ptr =
                    (__m256i*)(coeffs_ar_even + v_offset);
                __m256i* odd_counters_ptr =
                    (__m256i*)(coeffs_ar_odd + v_offset);
                __m256i coef_counters_even = _mm256_loadu_si256(
                    (const __m256i*)even_counters_ptr);
                __m256i coef_counters_odd = _mm256_loadu_si256(
                    (const __m256i*)odd_counters_ptr);

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
                        uint32_t in_offset = i * padded_ndims + v_offset;
                        uint32_t out_offset = i * ndims + v_offset;

                        __m256i raw_verrs = _mm256_loadu_si256(
                            (const __m256i*)(errs_ar + in_offset));

                        __m256i even_prev_deltas = _mm256_srai_epi16(
                            _mm256_slli_epi16(prev_deltas, 8), 8);
                        __m256i odd_prev_deltas = _mm256_srai_epi16(prev_deltas, 8);

                        __m256i even_predictions = _mm256_mullo_epi16(
                            even_prev_deltas, filter_coeffs_even);
                        __m256i odd_predictions = _mm256_mullo_epi16(
                            odd_prev_deltas, filter_coeffs_odd);
                        __m256i vpredictions = _mm256_blendv_epi8(odd_predictions,
                            _mm256_srli_epi16(even_predictions, 8), low_mask);

                        // zigzag decode
                        __m256i verrs = mm256_zigzag_decode_epi8(raw_verrs);

                        // compute gradients, but downsample for speed
                        // __m256i gradients = _mm256_setzero_si256(); // TODO rm
                        if (i % learning_downsample == learning_downsample - 1) {
                            __m256i gradients = _mm256_sign_epi8(prev_deltas, verrs);
                            gradients_sum = _mm256_add_epi8(gradients_sum, gradients);
                        }

                        // xff, then delta decode
                        __m256i vdeltas = _mm256_add_epi8(verrs, vpredictions);
                        vals = _mm256_add_epi8(prev_vals, vdeltas);

                        _mm256_storeu_si256((__m256i*)(dest + out_offset), vals);
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
                    // set coef[i] to ((counter[i] >> learn_shift) >> 12) << 12)
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
                        uint32_t in_offset = i * padded_ndims + v_offset;
                        uint32_t out_offset = i * ndims + v_offset;

                        __m256i raw_verrs = _mm256_loadu_si256(
                            (const __m256i*)(errs_ar + in_offset));

                        __m256i vpredictions = _mm256_mulhi_epi16(
                            prev_deltas, filter_coeffs);
                        // vpredictions = _mm256_slli_epi16(vpredictions, 2);

                        // zigzag decode
                        __m256i verrs = mm256_zigzag_decode_epi16(raw_verrs);

                        if (i % learning_downsample == learning_downsample - 1) {
                            __m256i gradients = _mm256_sign_epi16(prev_deltas, verrs);
                            gradients_sum = _mm256_add_epi16(gradients_sum, gradients);

                            // TODO uncomment above
                            // dump_m256i<int16_t>(prev_deltas);
                            if (debug) {
                                printf("prev_delta, err:\t%d\t\t%d\n",
                                    (int16_t)_mm256_extract_epi16(prev_deltas, 0),
                                    (int16_t)_mm256_extract_epi16(verrs, 0));
                            }
                        }

                        __m256i vdeltas = _mm256_add_epi16(verrs, vpredictions);
                        __m256i vals = _mm256_add_epi16(vdeltas, prev_vals);

                        if (debug && i == 7) { printf("final delta:\t%d\n",
                                (int16_t)_mm256_extract_epi16(vdeltas, 0)); }

                        _mm256_storeu_si256((__m256i*)(dest + out_offset), vals);
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

                    if (debug) {
                        // printf("counte[0]: %d\n", (int16_t)_mm256_extract_epi16(filter_coeffs, 0));
                        printf("coeffs[0]: %d\n", (int16_t)_mm256_extract_epi16(filter_coeffs, 0));
                        // printf("grad sum: "); dump_m256i<int16_t>(gradients_sum);
                        printf("grad sum[0]: %d\n", (int16_t)_mm256_extract_epi16(gradients_sum, 0));
                        // printf("coeffs even: "); dump_m256i<int16_t>(coef_counters_even);
                        printf("counter[0]: %d\n", (int32_t)_mm256_extract_epi32(coef_counters_even, 0));
                    }
                }
                _mm256_storeu_si256((__m256i*)prev_vals_ptr, prev_vals);
                _mm256_storeu_si256((__m256i*)prev_deltas_ptr, prev_deltas);
                _mm256_storeu_si256(even_counters_ptr, coef_counters_even);
                _mm256_storeu_si256(odd_counters_ptr, coef_counters_odd);
                // if (g >= 9) {
                //     printf("--- b = %d\n", b);
                //     // printf("grad sums: "); dump_m256i<int8_t>(gradients_sum);
                //     printf("counters even: "); dump_m256i<int16_t>(coef_counters_even);
                //     printf("filter coeffs even: "); dump_m256i<int8_t>(filter_coeffs_even);
                //     printf("dest block:\n"); dump_elements(dest, ndims*block_sz, ndims);
                // }
            }
            // if (debug) { printf("wrote data in block:\n"); dump_bytes(dest, block_sz*ndims, ndims*elem_sz); }
            if (debug) { printf("wrote data in block:\t\t"); dump_elements(dest, block_sz*ndims, ndims); }
            src += block_sz * in_row_nbytes / elem_sz;
            dest += block_sz * ndims;
            masks += nstripes;
            bitwidths += nstripes;
        } // for each block
        // printf("will now write to dest at offset %lld\n", (uint64_t)(dest - orig_dest));
    } // for each group

    free(headers_tmp);
    free(headers);
    free(data_masks);
    free(stripe_bitwidths);
    free(stripe_bitoffsets);
    free(errs_ar);
    free(coeffs_ar_even);

    // copy over trailing data
    if (debug) { printf("remaining len: %d\n", remaining_len); }
    memcpy(dest, src, remaining_len * elem_sz);

    if (debug > 2) {
        // printf("decompressed data:\n"); dump_bytes(orig_dest, orig_len * elem_sz, ndims * 4);
        printf("decompressed data:\n"); dump_elements(orig_dest, (int)(dest - orig_dest), ndims);
    }

    return dest + remaining_len - orig_dest;
}

SPRINTZ_FORCE_INLINE int64_t decompress_rowmajor_xff_rle_8b(const int8_t* src,
    uint8_t* dest, uint16_t ndims, uint32_t ngroups, uint16_t remaining_len)
{
    return decompress_rowmajor_xff_rle(src, dest, ndims, ngroups, remaining_len);
}
SPRINTZ_FORCE_INLINE int64_t decompress_rowmajor_xff_rle_16b(const int16_t* src,
    uint16_t* dest, uint16_t ndims, uint32_t ngroups, uint16_t remaining_len)
{
    return decompress_rowmajor_xff_rle(src, dest, ndims, ngroups, remaining_len);
}

int64_t decompress_rowmajor_xff_rle_8b(const int8_t* src, uint8_t* dest) {
    uint16_t ndims;
    uint32_t ngroups;
    uint16_t remaining_len;
    src += read_metadata_rle(src, &ndims, &ngroups, &remaining_len);
    return decompress_rowmajor_xff_rle(src, dest, ndims, ngroups, remaining_len);
}

int64_t decompress_rowmajor_xff_rle_16b(const int16_t* src, uint16_t* dest) {
    uint16_t ndims;
    uint32_t ngroups;
    uint16_t remaining_len;
    src += read_metadata_rle(src, &ndims, &ngroups, &remaining_len);
    return decompress_rowmajor_xff_rle(src, dest, ndims, ngroups, remaining_len);
}
