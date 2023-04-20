//
//  sprintz_xff.c
//  Compress
//
//  Created by DB on 9/16/17.
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
static const __m256i nbits_to_mask = _mm256_setr_epi8(
    0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0xff,
    0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // unused
    0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0xff,
    0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00); // unused


static const int kDefaultGroupSzBlocks = 2;

// ========================================================== rowmajor xff


int64_t compress8b_rowmajor_xff(const uint8_t* src, uint64_t len, int8_t* dest,
                            uint16_t ndims, bool write_size)
{
    // constants that could, in principle, be changed (but not in this impl)
    static const uint8_t log2_block_sz = 3;
    static const uint8_t stripe_sz = 8;
    static const uint8_t nbits_sz_bits = 3;
    // constants that could actually be changed in this impl
    static const uint8_t log2_learning_downsample = 1;
    // static const uint8_t log2_learning_downsample = 0;
    static const uint8_t group_sz_blocks = kDefaultGroupSzBlocks;
    static const uint8_t learning_shift = 1;
    // derived constants
    static const uint8_t block_sz = 1 << log2_block_sz;
    static const uint8_t learning_downsample = 1 << log2_learning_downsample;

    const uint8_t* orig_src = src;
    int8_t* orig_dest = dest;
    const uint8_t* src_end = src + len;

    // ================================ one-time initialization

    // ------------------------ stats derived from ndims
    uint16_t nstripes = ndims / stripe_sz + ((ndims % stripe_sz) > 0);
    uint32_t group_sz = ndims * block_sz * group_sz_blocks;
    uint32_t total_header_bits = ndims * nbits_sz_bits * group_sz_blocks;
    uint32_t total_header_bytes = (total_header_bits / 8) + ((total_header_bits % 8) > 0);

    // ------------------------ store data size and number of dimensions
    if (write_size) {
        assert(len < ((uint64_t)1) << 48);
        *(uint64_t*)dest = len;
        *(uint16_t*)(dest + 6) = ndims;
        dest += 8;
    }
    // handle low dims and low length; we'd read way past the end of the
    // input in this case
    if (len < 8 * block_sz * group_sz_blocks) {
        uint32_t remaining_len = (uint32_t)(len - (src - orig_src));
        memcpy(dest, src, remaining_len);
        return dest + remaining_len - orig_dest;
    }

    // printf("-------- compression (len = %lld)\n", (int64_t)len);
    // printf("saw original data:\n"); dump_bytes(src, len, ndims);

    // ------------------------ temp storage
    uint8_t*  stripe_bitwidths  = (uint8_t*) malloc(nstripes*sizeof(uint8_t));
    uint32_t* stripe_bitoffsets = (uint32_t*)malloc(nstripes*sizeof(uint32_t));
    uint64_t* stripe_masks      = (uint64_t*)malloc(nstripes*sizeof(uint64_t));
    uint32_t* stripe_headers    = (uint32_t*)malloc(nstripes*sizeof(uint32_t));

    uint32_t total_header_bytes_padded = total_header_bytes + 4;
    uint8_t* header_bytes = (uint8_t*)calloc(total_header_bytes_padded, 1);

    // extra row is for storing previous values
    // TODO just look at src and special case first row
    // int8_t* deltas = (int8_t*)calloc(1, (block_sz + 2) * ndims);
    // uint8_t* prev_vals_ar = (uint8_t*)(deltas + block_sz * ndims);
    int8_t* errs = (int8_t*)calloc(1, (block_sz + 4) * ndims);
    uint8_t* prev_vals_ar   = (uint8_t*)(errs + block_sz * ndims);
    int8_t* prev_deltas_ar  = (int8_t* )(errs + (block_sz + 1) * ndims);
    int16_t* coef_counters_ar=(int16_t*)(errs + (block_sz + 2) * ndims);

    // ================================ main loop

    uint16_t run_length_nblocks = 0;

    // uint64_t ngroups = len / group_sz;
    // for (uint64_t g = 0; g < ngroups; g++) {
    // const uint8_t* last_full_group_start = src_end - group_sz;
    // uint32_t ngroups = 0;
    // while (src <= last_full_group_start) {
    //     ngroups++;  // invariant: groups we start are always finished

    uint64_t ngroups = len / group_sz;
    for (uint64_t g = 0; g < ngroups; g++) {
        int8_t* header_dest = dest;
        dest += total_header_bytes;

        memset(header_bytes, 0, total_header_bytes_padded);
        memset(header_dest, 0, total_header_bytes);

        uint32_t header_bit_offset = 0;
        for (int b = 0; b < group_sz_blocks; b++) { // for each block in group

            // printf("---- block %d\n", b);

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
                uint8_t mask = 0;
                uint8_t prev_val = prev_vals_ar[dim];
                int8_t prev_delta = prev_deltas_ar[dim];

                int16_t coef = (coef_counters_ar[dim] >> (learning_shift + 4)) << 4;
                int8_t grad_sum = 0;

                for (uint8_t i = 0; i < block_sz; i++) {
                    uint32_t offset = (i * ndims) + dim;
                    uint8_t val = src[offset];
                    int8_t delta = (int8_t)(val - prev_val);
                    int8_t prediction = (((int16_t)prev_delta) * coef) >> 8;
                    int8_t err = delta - prediction;
                    uint8_t bits = zigzag_encode_8b(err);

                    if (i % learning_downsample == learning_downsample - 1) {
                        grad_sum += copysign_i8(err, prev_delta);
                    }
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
                mask = NBITS_MASKS_U8[mask];
                prev_vals_ar[dim] = prev_val;
                prev_deltas_ar[dim] = prev_delta;

                // update prediction coefficient
                int8_t grad = grad_sum >> (log2_block_sz - log2_learning_downsample);
                coef_counters_ar[dim] += grad;
                // grads[dim] = grad;

                // mask = NBITS_MASKS_U8[255]; // TODO rm
                uint8_t max_nbits = (32 - _lzcnt_u32((uint32_t)mask));

                uint16_t stripe = dim / stripe_sz;
                uint8_t idx_in_stripe = dim % stripe_sz;

                // accumulate stats about this stripe
                stripe_bitwidths[stripe] += max_nbits;
                stripe_masks[stripe] |= ((uint64_t)mask) << (idx_in_stripe * 8);

                // accumulate header info for this stripe
                uint32_t write_nbits = max_nbits - (max_nbits == 8); // map 8 to 7
                stripe_headers[stripe] |= write_nbits << (idx_in_stripe * nbits_sz_bits);
                // printf("write_nbits = %d, stripe header = ", write_nbits);
                // dump_bytes(stripe_headers[stripe], false); dump_bits(stripe_headers[stripe]);
            }
            // TODO rm all this
            // printf("zigzagged errs:\n"); dump_elements(errs, ndims*block_sz, ndims);
            // printf("predictions:\n"); dump_elements(predictions, ndims*block_sz, ndims);
            // if (b == 0) {
            //     printf("deltas:\n"); dump_elements(deltas, ndims*block_sz, ndims);
            //     printf("all grads:\n"); dump_elements(all_grads, ndims * block_sz, ndims);
            //     printf("grad_sums:\n"); dump_elements(grad_sums, ndims * block_sz, ndims);
            //     printf("mean grads:\n"); dump_elements(grads, ndims, ndims);
            //     printf("coef counters:\n"); dump_elements(coef_counters_ar, ndims, ndims);
            // }
            // free(predictions);
            // free(deltas);
            // free(grads);
            // free(grad_sums);
            // free(all_grads);

            // compute start offsets of each stripe (in bits)
            stripe_bitoffsets[0] = 0;
            for (uint32_t stripe = 1; stripe < nstripes; stripe++) {
                stripe_bitoffsets[stripe] = stripe_bitoffsets[stripe - 1] +
                    stripe_bitwidths[stripe - 1];
            }
            // compute width of each row (in bytes); note that we byte align
            uint32_t row_width_bits = stripe_bitoffsets[nstripes - 1] +
                stripe_bitwidths[nstripes-1];
            uint32_t in_row_nbytes =
                (row_width_bits >> 3) + ((row_width_bits % 8) > 0);

            // ------------------------ write out header bits for this block
            for (uint32_t stripe = 0; stripe < nstripes; stripe++) {
                uint16_t byte_offset = header_bit_offset >> 3;
                uint16_t bit_offset = header_bit_offset & 0x07;
                // Note: this write will get more complicated when upper byte
                // of each stripe_header isn't guaranteed to be 0
                *(uint32_t*)(header_dest + byte_offset) |= \
                    stripe_headers[stripe] << bit_offset;

                uint8_t is_final_stripe = stripe == (nstripes - 1);
                uint8_t has_trailing_dims = (ndims % stripe_sz) > 0;
                uint8_t add_ndims = is_final_stripe && has_trailing_dims ?
                    ndims % stripe_sz : stripe_sz;
                header_bit_offset += nbits_sz_bits * add_ndims;
            }

            // zero output so that we can just OR in bits (and also touch each
            // cache line in order to ensure that we prefetch, despite our
            // weird pattern of writes)
            // memset(dest, 0, nstripes * stripe_sz * block_sz);
            memset(dest, 0, in_row_nbytes * block_sz); // above line can overrun dest buff

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

                int8_t* outptr = dest + offset_bytes;
                // const uint8_t* inptr = (const uint8_t*)(deltas +
                const uint8_t* inptr =
                    (const uint8_t*)(errs + (stripe * stripe_sz));

                // XXX Note that this impl assumes that output buff is zeroed
                if (total_bits <= 64) { // always fits in one u64
                    for (int i = 0; i < block_sz; i++) { // for each sample in block
                        // 8B write to store (at least most of) the data
                        uint64_t data = *(uint64_t*)inptr;
                        uint64_t packed_data = _pext_u64(data, mask);
                        uint64_t write_data = packed_data << offset_bits;
                        *(uint64_t*)outptr = write_data | (*(uint64_t*)outptr);

                        outptr += in_row_nbytes;
                        inptr += ndims;
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

                        outptr += in_row_nbytes;
                        inptr += ndims;
                    }
                }
                // printf("read back header: "); dumpEndianBits(*(uint32_t*)(header_dest - stripe_header_sz));
            } // for each stripe
            src += block_sz * ndims;
            dest += block_sz * in_row_nbytes;
        } // for each block
    } // for each group

main_loop_end:

    free(stripe_bitwidths);
    free(stripe_bitoffsets);
    free(stripe_masks);
    free(stripe_headers);
    free(errs);

    uint32_t remaining_len = (uint32_t)(len - (src - orig_src));
    memcpy(dest, src, remaining_len);
    return dest + remaining_len - orig_dest;
}

int64_t decompress8b_rowmajor_xff(const int8_t* src, uint8_t* dest) {
    // constants that could, in principle, be changed (but not in this impl)
    static const uint8_t log2_block_sz = 3;
    static const uint8_t vector_sz = 32;
    static const uint8_t stripe_sz = 8;
    static const uint8_t nbits_sz_bits = 3;
    static const __m256i low_mask = _mm256_set1_epi16(0xff);
    // constants that could actually be changed in this impl
    static const uint8_t log2_learning_downsample = 1;
    static const uint8_t group_sz_blocks = kDefaultGroupSzBlocks;
    static const uint8_t learning_shift = 1;
    // derived constants
    static const uint8_t block_sz = 1 << log2_block_sz;
    static const int group_sz_per_dim = block_sz * group_sz_blocks;
    static const uint8_t learning_downsample = 1 << log2_learning_downsample;
    static const uint8_t stripe_header_sz = nbits_sz_bits * stripe_sz / 8;
    static const uint8_t nbits_sz_mask = (1 << nbits_sz_bits) - 1;
    static const uint64_t kHeaderUnpackMask = TILE_BYTE(nbits_sz_mask);
    assert(stripe_sz % 8 == 0);
    assert(vector_sz % stripe_sz == 0);
    assert(vector_sz >= stripe_sz);

    uint8_t* orig_dest = dest;
    // const int8_t* orig_src = src;

    // ================================ one-time initialization

    // ------------------------ read original data size, ndims
    static const uint8_t len_nbytes = 6;
    uint64_t one = 1; // make next line legible
    uint64_t len_mask = (one << (8 * len_nbytes)) - 1;
    uint64_t orig_len = (*(uint64_t*)src) & len_mask;
    uint16_t ndims = (*(uint16_t*)(src + len_nbytes));
    src += 8;

    bool just_cpy = orig_len < 8 * block_sz * group_sz_blocks;
    // just_cpy = just_cpy || orig_len & (((uint64_t)1) << 47);
    if (just_cpy) { // if data was too small or failed to compress
        memcpy(dest, src, (uint32_t)orig_len);
        return orig_len;
    }
    if (ndims == 0) {
        perror("ERROR: Received ndims of 0!");
        return 0;
    }

    // printf("-------- decompression (orig_len = %lld)\n", (int64_t)orig_len);
    // printf("saw compressed data (with possible extra at end):\n");
    // dump_bytes(src, orig_len + 24);

    // ------------------------ stats derived from ndims
    // header stats
    uint32_t nheader_vals = ndims * group_sz_blocks;
    uint32_t nheader_stripes = nheader_vals / stripe_sz + \
        ((nheader_vals % stripe_sz) > 0);
    uint32_t total_header_bits = ndims * nbits_sz_bits * group_sz_blocks;
    uint32_t total_header_bytes = (total_header_bits / 8) + ((total_header_bits % 8) > 0);

    // final header can be shorter than others; construct mask so we don't
    // read in start of packed data as header bytes
    uint8_t remaining_header_sz = total_header_bytes % stripe_header_sz;
    uint8_t final_header_sz = remaining_header_sz ? remaining_header_sz : stripe_header_sz;
    uint32_t shift_bits = 8 * (4 - final_header_sz);
    uint32_t final_header_mask = ((uint32_t)0xffffffff) >> shift_bits;

    // stats for main decompression loop
    uint32_t group_sz = ndims * group_sz_per_dim;
    uint16_t nstripes = ndims / stripe_sz + ((ndims % stripe_sz) > 0);
    uint32_t padded_ndims = round_up_to_multiple(ndims, vector_sz);
    uint16_t nvectors = padded_ndims / vector_sz + ((padded_ndims % vector_sz) > 0);

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
    uint64_t* data_masks        = (uint64_t*)calloc(nstripes_in_vectors, 8);
    uint64_t* stripe_bitwidths  = (uint64_t*)calloc(nstripes_in_vectors, 8);
    uint32_t* stripe_bitoffsets = (uint32_t*)calloc(nstripes, 4);

    // extra row in deltas is to store last decoded values
    // TODO just special case very first row
    int8_t* errs_ar = (int8_t*)calloc((block_sz + 4) * padded_ndims, 1);
    uint8_t* prev_vals_ar = (uint8_t*)(errs_ar + (block_sz + 0) * padded_ndims);
    int8_t* prev_deltas_ar = (int8_t*)(errs_ar + (block_sz + 1) * padded_ndims);
    int8_t* coeffs_ar_even = (int8_t*)(errs_ar + (block_sz + 2) * padded_ndims);
    int8_t* coeffs_ar_odd =  (int8_t*)(errs_ar + (block_sz + 3) * padded_ndims);

    // ================================ main loop

    uint64_t ngroups = orig_len / group_sz; // if we get an fp error, it's this
    for (uint64_t g = 0; g < ngroups; g++) {
        const uint8_t* header_src = (uint8_t*)src;
        src += total_header_bytes;

        // ------------------------ create unpacked headers array
        // unpack headers for all the blocks; note that this is tricky
        // because we need to zero-pad final stripe's headers in each block
        // so that stripe widths don't get messed up (from initial data in
        // next block's header)
        uint64_t* header_write_ptr = (uint64_t*)headers_tmp;
        for (size_t stripe = 0; stripe < nheader_stripes - 1; stripe++) {
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
        for (size_t b = 0; b < group_sz_blocks; b++) {
            size_t src_offset = b * ndims;
            size_t dest_offset = b * nstripes * stripe_sz;
            memcpy(headers + dest_offset, header_in_ptr + src_offset, ndims);
        }

        // // printf("header_tmp:    "); dump_bytes(headers_tmp, nheader_stripes * 8);
        // if (dump_group) { printf("padded headers:"); dump_bytes(headers, nstripes_in_vectors * 8); }

        // ------------------------ masks and bitwidths for all stripes
        for (size_t v = 0; v < nvectors_in_group; v++) {
            __m256i raw_header = _mm256_loadu_si256(
                (const __m256i*)(headers + v * vector_sz));
            // map nbits of 7 to 8
            static const __m256i sevens = _mm256_set1_epi8(0x07);
            __m256i header = _mm256_sub_epi8(
                raw_header, _mm256_cmpeq_epi8(raw_header, sevens));

            // compute and store bit widths
            __m256i bitwidths = _mm256_sad_epu8(
                header, _mm256_setzero_si256());
            uint8_t* store_addr = ((uint8_t*)stripe_bitwidths) + v * vector_sz;
            _mm256_storeu_si256((__m256i*)store_addr, bitwidths);

            // compute and store masks
            __m256i masks = _mm256_shuffle_epi8(nbits_to_mask, raw_header);
            uint8_t* store_addr2 = ((uint8_t*)data_masks) + v * vector_sz;
            _mm256_storeu_si256((__m256i*)store_addr2, masks);
        }

        // printf("padded masks:     "); dump_bytes(data_masks, group_header_sz);
        // printf("padded bitwidths: "); dump_elements(stripe_bitwidths, nstripes_in_vectors);

        // ------------------------ inner loop; decompress each block
        uint64_t* masks = data_masks;
        uint64_t* bitwidths = stripe_bitwidths;
        for (int b = 0; b < group_sz_blocks; b++) { // for each block in group
            // compute where each stripe begins, as well as width of a row
            stripe_bitoffsets[0] = 0;
            for (size_t stripe = 1; stripe < nstripes; stripe++) {
                stripe_bitoffsets[stripe] = (uint32_t)(stripe_bitoffsets[stripe - 1]
                    + bitwidths[stripe - 1]);
            }
            uint32_t in_row_nbits = (uint32_t)(stripe_bitoffsets[nstripes - 1] +
                bitwidths[nstripes - 1]);
            uint32_t in_row_nbytes = (in_row_nbits >> 3) + ((in_row_nbits % 8) > 0);

            // ------------------------ unpack data for each stripe
            for (int stripe = nstripes - 1; stripe >= 0; stripe--) {
                uint32_t offset_bits = stripe_bitoffsets[stripe] & 0x07;
                uint32_t offset_bytes = stripe_bitoffsets[stripe] >> 3;

                uint64_t mask = masks[stripe];
                uint8_t nbits = bitwidths[stripe];
                uint8_t total_bits = nbits + offset_bits;

                const int8_t* inptr = src + offset_bytes;
                // uint8_t* outptr = deltas + (stripe * stripe_sz);
                int8_t* outptr = errs_ar + (stripe * stripe_sz);
                uint32_t out_row_nbytes = padded_ndims;

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
                        // printf("packed_data "); dump_bytes(packed_data);
                        packed_data |= (*(uint64_t*)(inptr + 8)) << (nbits - nbits_lost);
                        // printf("packed_data after OR "); dump_bytes(packed_data);
                        *(uint64_t*)outptr = _pdep_u64(packed_data, mask);
                        inptr += in_row_nbytes;
                        outptr += out_row_nbytes;
                    }
                }
            } // for each stripe

            for (int32_t v = nvectors - 1; v >= 0; v--) {
                uint32_t v_offset = v * vector_sz;
                __m256i* prev_vals_ptr = (__m256i*)(prev_vals_ar + v_offset);
                __m256i* prev_deltas_ptr = (__m256i*)(prev_deltas_ar + v_offset);
                __m256i prev_vals = _mm256_loadu_si256(prev_vals_ptr);
                __m256i prev_deltas = _mm256_loadu_si256(prev_deltas_ptr);
                __m256i vals = _mm256_setzero_si256();

                const __m256i* even_counters_ptr =
                    (const __m256i*)(coeffs_ar_even + v_offset);
                const __m256i* odd_counters_ptr =
                    (const __m256i*)(coeffs_ar_odd + v_offset);
                __m256i coef_counters_even = _mm256_loadu_si256(even_counters_ptr);
                __m256i coef_counters_odd = _mm256_loadu_si256(odd_counters_ptr);

                // set coef[i] to ((counter[i] >> learn_shift) >> 4) << 4)
                __m256i filter_coeffs_even  = _mm256_srai_epi16(
                    coef_counters_even, learning_shift + 4);
                __m256i filter_coeffs_odd  = _mm256_srai_epi16(
                    coef_counters_odd, learning_shift + 4);
                filter_coeffs_even = _mm256_slli_epi16(filter_coeffs_even, 4);
                filter_coeffs_odd  = _mm256_slli_epi16(filter_coeffs_odd, 4);

                __m256i gradients_sum = _mm256_setzero_si256();

                for (uint8_t i = 0; i < block_sz; i++) {
                    uint32_t in_offset = i * padded_ndims + v_offset;
                    uint32_t out_offset = i * ndims + v_offset;

                    __m256i raw_verrs = _mm256_loadu_si256(
                        (const __m256i*)(errs_ar + in_offset));

                    // straightforward sign extension of even bytes
                    __m256i even_prev_deltas = _mm256_srai_epi16(
                        _mm256_slli_epi16(prev_deltas, 8), 8);

                    // evil sign extenstion of even bytes; more instructions
                    // but lower total reciprocal thruput
                    // TODO test whether this is faster
                    // __m256i low_bytes = _mm256_and_si256(prev_deltas, low_mask);
                    // __m256i negative = _mm256_cmpgt_epi16(low_bytes, max_pos_val);
                    // __m256i neg_mask = _mm256_andnot_si256(low_mask, negative);
                    // __m256i even_prev_deltas = _mm256_or_si256(neg_mask, low_bytes);

                    // sign extension of odd bytes
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
                    if (i % learning_downsample == learning_downsample - 1) {
                        __m256i gradients = _mm256_sign_epi8(prev_deltas, verrs);
                        gradients_sum = _mm256_add_epi8(gradients_sum, gradients);
                    }
                    // if (b == 0) { printf("grads%d: ", i); dump_m256i<int8_t>(gradients); }
                    // if (b == 0) { printf("gradsums%d: ", i); dump_m256i<int8_t>(gradients_sum); }
                    // if (b == 0) { printf("\n"); }
                    // printf("verrs: "); dump_m256i(verrs);
                    // printf("vpredictions: "); dump_m256i(vpredictions);
                    // printf("vdeltas: "); dump_m256i(vpredictions);

                    // xff, then delta decode
                    __m256i vdeltas = _mm256_add_epi8(verrs, vpredictions);
                    vals = _mm256_add_epi8(prev_vals, vdeltas);

                    _mm256_storeu_si256((__m256i*)(dest + out_offset), vals);
                    prev_deltas = vdeltas;
                    prev_vals = vals;
                }
                _mm256_storeu_si256((__m256i*)prev_vals_ptr, prev_vals);
                _mm256_storeu_si256((__m256i*)prev_deltas_ptr, prev_deltas);

                // mean of gradients in block, for even and odd indices
                const uint8_t rshift = 8 + log2_block_sz - log2_learning_downsample;
                __m256i even_grads = _mm256_srai_epi16(
                    _mm256_slli_epi16(gradients_sum, 8), rshift);
                __m256i odd_grads = _mm256_srai_epi16(gradients_sum, rshift);

                // store updated coefficients (or, technically, the counters)
                coef_counters_even = _mm256_add_epi16(coef_counters_even, even_grads);
                coef_counters_odd = _mm256_add_epi16(coef_counters_odd, odd_grads);

                _mm256_storeu_si256((__m256i*)even_counters_ptr, coef_counters_even);
                _mm256_storeu_si256((__m256i*)odd_counters_ptr, coef_counters_odd);
            }
            src += block_sz * in_row_nbytes;
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

    // copy over trailing data
    size_t remaining_len = orig_len - (dest - orig_dest);
    memcpy(dest, src, remaining_len);

    // size_t dest_sz = dest + remaining_len - orig_dest;
    // printf("decompressed data:\n"); dump_bytes(orig_dest, dest_sz, ndims);

    return dest + remaining_len - orig_dest;
}
