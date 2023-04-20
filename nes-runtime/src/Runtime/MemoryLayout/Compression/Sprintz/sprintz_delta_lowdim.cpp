//
//  sprintz3.cpp
//  Compress
//
//  Created by DB on 2017-12-2.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#include "Runtime/MemoryLayout/Compression/Sprintz/sprintz_delta.h"

#include <stdio.h>

#include <assert.h>
#include <stdint.h>
#include <string.h>

#include "Runtime/MemoryLayout/Compression/Sprintz/bitpack.h"
#include "Runtime/MemoryLayout/Compression/Sprintz/format.h"
#include "Runtime/MemoryLayout/Compression/Sprintz/transpose.h"

static constexpr uint64_t kHeaderMask8b = TILE_BYTE(0x07); // 3 ones

static const __m256i nbits_to_mask = _mm256_setr_epi8(
    0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0xff,
    0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // unused
    0x00, 0x01, 0x03, 0x07, 0x0f, 0x1f, 0x3f, 0xff,
    0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00); // unused


static const int kDefaultGroupSzBlocks = 2;
// static const int kDefaultGroupSzBlocks = 8;  // slight pareto improvement

static const int debug = 0;
// static const int debug = 4;

// ------------------------------------------------ delta + rle low ndims

template<typename int_t, typename uint_t>
int64_t compress_rowmajor_delta_rle_lowdim(const uint_t* src, uint32_t len,
    int_t* dest, uint16_t ndims, bool write_size)
{
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(uint_t);
    static const uint8_t elem_sz_nbits = 8 * elem_sz;
    static const uint8_t nbits_sz_bits = elem_sz == 1 ? 3 : 4; // XXX only {8,16}b
    // constants that could, in principle, be changed (but not in this impl)
    static const int block_sz = 8;
    static const uint8_t stripe_sz_nbytes = 8;
    // constants that could actually be changed in this impl
    static const int group_sz_blocks = kDefaultGroupSzBlocks;
    static const int length_header_nbytes = 8;  // TODO indirect to format.h
    static const uint16_t max_run_nblocks = 0x7fff; // 15 bit counter
    // derived consts
    static const uint32_t min_data_size = 8 * block_sz * group_sz_blocks;
    static const uint8_t stripe_sz = stripe_sz_nbytes / elem_sz;

    // const uint8_t* orig_src = src;
    // int8_t* orig_dest = dest;
    // const uint8_t* src_end = src + len;
    const uint_t* orig_src = src;
    const uint_t* src_end = src + len;
    int_t* orig_dest = dest;

    bool invalid_ndims = ndims == 0;
    invalid_ndims |= (elem_sz == 1 && ndims > 4);
    invalid_ndims |= (elem_sz == 2 && ndims > 2);
    if (invalid_ndims) {
        printf("ERROR: compress_rowmajor_delta_rle_lowdim: invalid ndims: %d\n", ndims);
        return -1;
    }

    int gDebug = debug;
    int debug = (elem_sz == 2) ? gDebug : 0;

    // ================================ one-time initialization

    // ------------------------ stats derived from ndims
    uint32_t group_sz = ndims * block_sz * group_sz_blocks;
    uint32_t total_header_bits = ndims * nbits_sz_bits * group_sz_blocks;
    uint32_t total_header_bytes = DIV_ROUND_UP(total_header_bits, 8);

    // ------------------------ handle edge cases

    if (debug > 0) {
        printf("-------- compression (len = %lld)\n", (int64_t)len);
        if (debug > 2) {
            // printf("saw original data:\n"); dump_elements(src, len, ndims);
            // printf("saw original data:\n"); dump_elements(src, 16, ndims);
            // printf("saw original data:\n"); dump_bytes(src, 32, ndims * elem_sz);
            printf("saw original data:\n"); dump_bytes(src, 96, ndims * elem_sz);
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
        dest += length_header_nbytes / elem_sz;
    }

    // ------------------------ temp storage
    uint8_t* dims_nbits = (uint8_t*)(malloc(ndims*sizeof(uint8_t)));

    uint32_t total_header_bytes_padded = total_header_bytes + 4;
    uint8_t* header_bytes = (uint8_t*)calloc(total_header_bytes_padded, 1);

    // extra row is for storing previous values
    int_t* deltas = (int_t*)calloc(elem_sz, (block_sz + 1) * ndims);
    uint_t* prev_vals_ar = (uint_t*)(deltas + block_sz * ndims);

    // ================================ main loop

    uint16_t run_length_nblocks = 0;

    const uint_t* last_full_group_start = src_end - group_sz;
    uint32_t ngroups = 0;
    while (src <= last_full_group_start) {
        ngroups++;  // invariant: groups we start are always finished

        // if (debug) printf("==== group %d\n", (int)ngroups - 1);


        // debug = debug && (ngroups <= 4);

        // if (debug) printf("made it to here... group_sz_blocks = %d\n", group_sz_blocks);

        // int8_t* header_dest = dest;
        // dest += total_header_bytes;
        int8_t* header_dest = (int8_t*)dest;
        dest = (int_t*)(((int8_t*)dest) + total_header_bytes);

        memset(header_bytes, 0, total_header_bytes_padded);
        memset(header_dest, 0, total_header_bytes);

        uint32_t header_bit_offset = 0;
        int b = 0;
        while (b < group_sz_blocks) { // for each block

            // ------------------------ compute info for each stripe
            uint32_t total_dims_nbits = 0;
            for (uint16_t dim = 0; dim < ndims; dim++) {
                // compute maximum number of bits used by any value of this dim,
                // while simultaneously computing deltas
                uint_t mask = 0;
                uint_t prev_val = prev_vals_ar[dim];
                for (uint8_t i = 0; i < block_sz; i++) {
                    uint32_t in_offset = (i * ndims) + dim; // rowmajor
                    uint32_t out_offset = (dim * block_sz) + i; // colmajor
                    uint_t val = src[in_offset];
                    int_t delta = (int_t)(val - prev_val);
                    uint_t bits = ZIGZAG_ENCODE_SCALAR(delta);
                    // uint8_t bits = val;
                    mask |= bits;
                    deltas[out_offset] = bits;
                    prev_val = val;
                }
                // write out value for delta encoding of next block
                prev_vals_ar[dim] = prev_val;

                // mask = 255;  // TODO rm
                // if (elem_sz == 2) { mask = 0xffff; }  // TODO rm
                // if (mask > 0) { mask = 255; } // TODO rm
                uint8_t max_nbits = (32 - _lzcnt_u32((uint32_t)mask));
                max_nbits += max_nbits == (elem_sz_nbits - 1); // 7->8 or 15->16

                dims_nbits[dim] = max_nbits;
                total_dims_nbits += max_nbits;
            }

            // if (debug) { printf("--- (in the first place) %d.%d nbits: ", (int)ngroups - 1, b); dump_bytes(dims_nbits, ndims); }

just_read_block:

            // ------------------------ handle runs of zeros
            bool do_rle = total_dims_nbits == 0 && run_length_nblocks < max_run_nblocks;
            if (do_rle) {
do_rle:
                run_length_nblocks++; // TODO uncomment
                src += block_sz * ndims;

                if (src < last_full_group_start) {
                    continue; // still enough data to finish group
                } else { // not enough data to finish group
                    // write out headers for this block; they're all zero, so
                    // just increment addr at which to write next time
                    header_bit_offset += ndims * nbits_sz_bits;

                    if (debug) { printf("---- %d.%d nbits: ", (int)ngroups - 1, b); dump_bytes(dims_nbits, ndims); }
                    if (debug) { printf("aborting and compressing rle block of length %d ending at src offset %d\n", run_length_nblocks, (int)(src - orig_src)); }

                    b++; // we're finishing off this block

                    // write out length of the current constant section
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
                if (debug) { printf("---- %d.%d nbits: ", (int)ngroups - 1, b); dump_bytes(dims_nbits, ndims); }
                if (debug) { printf("compressing rle block of length %d ending at offset %d\n", run_length_nblocks, (int)(src - orig_src)); }
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
                // block the decoder isn't expecting
                if (b == group_sz_blocks) {
                    // printf("forcing group end...\n");
                    // memcpy(prev_vals_ar, src - ndims, ndims);
                    // continue;

                    // start new group, and pretend the block we read
                    // was the first block in that group (which it is now)
                    ngroups++;  // invariant: groups we start are always finished
                    header_bit_offset = 0;
                    b = 0;
                    header_dest = (int8_t*)dest;
                    dest = (int_t*)(((int8_t*)dest) + total_header_bytes);
                    memset(header_bytes, 0, total_header_bytes_padded);
                    memset(header_dest, 0, total_header_bytes);

                    goto just_read_block;
                }

                // have to enforce invariant that bitwidth of 0 always gets
                // run-length encoded; this case can happen when the current
                // block was zeros, but we hit the limit `max_run_nblocks`
                // printf("total_header_bits: %d\n", )
                if (total_dims_nbits == 0) { goto do_rle; }

                // printf("row width bits for this nonzero block: %d\n", row_width_bits);
                // printf("modified header bit offset: %d\n", header_bit_offset);
            }

            // ------------------------ write out header bits for this block

            // if (debug) { printf("---- %d.%d\n", ngroups - 1, b); }
            if (debug) { printf("--- %d.%d nbits: ", (int)ngroups - 1, b); dump_bytes(dims_nbits, ndims); }

            for (uint16_t dim = 0; dim < ndims; dim++) {
                uint16_t byte_offset = header_bit_offset >> 3;
                uint8_t bit_offset = header_bit_offset & 0x07;
                uint8_t write_nbits = dims_nbits[dim] - (dims_nbits[dim] == elem_sz_nbits);
                *(uint32_t*)(header_dest + byte_offset) |= \
                        write_nbits << bit_offset;
                header_bit_offset += nbits_sz_bits;
            }

            // ------------------------ write out block data

            // zero output so that we can just OR in bits (and also touch each
            // cache line in order to ensure that we prefetch, despite our
            // weird pattern of writes)
            uint32_t data_sz = total_dims_nbits * block_sz / 8;
            memset(dest, 0, data_sz * elem_sz);

            // if (debug) {
            //     printf("deltas: \n"); dump_elements(deltas, ndims * block_sz, block_sz);
            // }

            uint64_t* delta_buff_u64 = (uint64_t*)deltas;
            for (uint16_t dim = 0; dim < ndims; dim++) {
                uint8_t nbits = dims_nbits[dim];
                if (elem_sz == 1) {
                    uint64_t mask = kBitpackMasks8[nbits];
                    *((uint64_t*)dest) = _pext_u64(delta_buff_u64[dim], mask);
                } else if (elem_sz == 2) {
                    uint64_t mask = kBitpackMasks16[nbits];
                    static const uint8_t stripe_sz = stripe_sz_nbytes / elem_sz;
                    static const uint8_t nstripes =
                        elem_sz * block_sz / stripe_sz_nbytes;

                    // if (debug) { printf("using mask: "); dump_bits(mask); }

                    // TODO try switch to unroll each nbits case here for 16b;
                    // cases where nbits is a multiple of 2 will fall on byte
                    // boundaries and be nicer
                    //
                    // NOTE: only reason we can always do an 8B write is that,
                    // for 16b, even numbers fall exactly on byte boundaries
                    // and the largest odd number is 13, which yields 52b
                    // and therefore fits in 8B for any bit offset
                    // 2 cases to think about:
                    //  -first 4-elem chunk in block
                    //      -prev block ends byte-aligned since 8 elems, so
                    //      always a byte-aligned 8B write
                    //  -2nd 4-elem chunk in block
                    //      -prev block might end with trailing 4b in last
                    //      byte, but worst case is 13b, which is a 52b write,
                    //      so this write will be 52 + 4 = 56b
                    uint16_t total_bit_offset = 0;
                    int8_t* dest8 = (int8_t*)dest;
                    for (uint8_t s = 0; s < nstripes; s++) {
                        uint32_t in_idx = dim * nstripes + s;
                        uint8_t byte_offset = total_bit_offset >> 3;
                        uint8_t bit_offset = total_bit_offset & 0x07;
                        // if (debug) { printf("packing deltas: "); dump_bytes(delta_buff_u64[in_idx]); }
                        uint64_t packed_data = _pext_u64(
                            delta_buff_u64[in_idx], mask);
                        *(uint64_t*)(dest8 + byte_offset) |= packed_data << bit_offset;
                        total_bit_offset += nbits * stripe_sz;
                        // if (debug) printf("total_bit_offset: %d\n", total_bit_offset);
                    }
                }
                // if (debug) {
                //     // printf("packed deltas: \n"); dump_elements(dest, block_sz, block_sz);
                //     printf("packed deltas: \n"); dump_bytes(dest, block_sz * elem_sz, block_sz * elem_sz);
                // }
                int8_t* dest8 = ((int8_t*)dest) + nbits * block_sz / 8;
                dest = (int_t*)dest8;
                // dest += nbits * block_sz / 8;
            }
            src += block_sz * ndims;
            b++;
        } // for each block
    } // for each group

main_loop_end:

    free(deltas);
    free(dims_nbits);
    free(header_bytes);

    uint16_t remaining_len = (uint16_t)(src_end - src);
    if (write_size) {
        write_metadata_rle(orig_dest, ndims, ngroups, remaining_len);
    }
    memcpy(dest, src, remaining_len * elem_sz);

    // printf("=== sprintz comp: memcpying remaining_len elements = %d (%d B)\n", (int)remaining_len, (int)remaining_len * 2);
    // auto ret = dest + remaining_len - orig_dest;
    // printf("final bytes:\n"); dump_bytes((uint8_t*)dest, remaining_len * elem_sz);
    // printf("=== sprintz comp: returning length %d (%d B)\n", (int)ret, (int)ret * 2);

    // XXX: if dest and orig_dest not both aligned or not aligned to 2B
    // (which there's no reason they have to be), this can produce an
    // off-by-one error
    return dest + remaining_len - orig_dest;
}

int64_t compress_rowmajor_delta_rle_lowdim_8b(const uint8_t* src, uint32_t len,
    int8_t* dest, uint16_t ndims, bool write_size)
{
    return compress_rowmajor_delta_rle_lowdim(src, len, dest, ndims, write_size);
}
int64_t compress_rowmajor_delta_rle_lowdim_16b(const uint16_t* src, uint32_t len,
    int16_t* dest, uint16_t ndims, bool write_size)
{
    return compress_rowmajor_delta_rle_lowdim(src, len, dest, ndims, write_size);
}

template<typename int_t, typename uint_t>
SPRINTZ_FORCE_INLINE int64_t decompress_rowmajor_delta_rle_lowdim(
    const int_t* src, uint_t* dest, uint16_t ndims, uint64_t ngroups,
    uint16_t remaining_len)
{
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
    static const uint8_t nbits_sz_mask = (1 << nbits_sz_bits) - 1;
    static const uint64_t kHeaderUnpackMask = TILE_BYTE(nbits_sz_mask);
    static const size_t min_data_size = 8 * block_sz * group_sz_blocks;
    static const uint8_t vector_sz = vector_sz_nbytes / elem_sz;

    // uint8_t* orig_dest = dest;
    // const int8_t* orig_src = src;
    uint_t* orig_dest = dest;

    bool invalid_ndims = ndims == 0;
    invalid_ndims |= (elem_sz == 1 && ndims > 4);
    invalid_ndims |= (elem_sz == 2 && ndims > 2);
    if (invalid_ndims) {
        printf("ERROR: decompress_rowmajor_delta_rle_lowdim: invalid ndims: %d\n", ndims);
        return -1;
    }

    int gDebug = debug;
    int debug = (elem_sz == 2) ? gDebug : 0;

    // ================================ one-time initialization

    // ------------------------ read original data size, ndims

    bool just_cpy = (ngroups == 0) && remaining_len < min_data_size;
    if (just_cpy) { // if data was too small or failed to compress
        memcpy(dest, src, (size_t)remaining_len * elem_sz);
        return remaining_len;
    }

    if (debug) {
        int64_t min_orig_len = ngroups * group_sz_blocks * block_sz * ndims;
        printf("-------- decompression (orig_len = %lld)\n", (int64_t)min_orig_len);
        if (debug > 2) {
            printf("saw compressed data (with possible missing data if runs):\n");
            // dump_bytes(src, min_orig_len + 8, ndims * elem_sz);
            // dump_bytes(src, 192, ndims * elem_sz);
            dump_bytes(src, 32, ndims * elem_sz);
            // dump_elements(src, min_orig_len + 4, ndims);
            // dump_elements(src, 17, ndims);
        }
    }
    // debug = false; // TODO rm

    // ------------------------ stats derived from ndims
    // header stats
    uint32_t nheader_vals = ndims * group_sz_blocks;
    uint32_t nheader_stripes = DIV_ROUND_UP(nheader_vals, stripe_nbytes);
    uint32_t total_header_bits = ndims * nbits_sz_bits * group_sz_blocks;
    uint32_t total_header_bytes = DIV_ROUND_UP(total_header_bits, 8);

    // stats for main decompression loop
    uint32_t group_sz = ndims * group_sz_per_dim;
    uint32_t padded_ndims = round_up_to_multiple(ndims, vector_sz);
    // uint16_t nvectors = padded_ndims / vector_sz + ((padded_ndims % vector_sz) > 0);
    uint16_t nvectors = DIV_ROUND_UP(padded_ndims, vector_sz);

    // ------------------------ temp storage
    // allocate temp vars of minimal possible size such that we can
    // do vector loads and stores (except bitwidths, which are u64s so
    // that we can store directly after sad_epu8)
    uint8_t*  headers = (uint8_t*) calloc(1, total_header_bytes);
    uint_t* deltas = (uint_t*)calloc(block_sz * padded_ndims, elem_sz);
    uint_t* prev_vals_ar = (uint_t*)calloc(padded_ndims, elem_sz);

    // ================================ main loop

    for (uint64_t g = 0; g < ngroups; g++) {
        // const uint8_t* header_src = (uint8_t*)src;
        // src += total_header_bytes;
        const uint8_t* header_src = (const uint8_t*)src;
        src = (int_t*)(((int8_t*)src) + total_header_bytes);

        uint32_t header_bit_offset = 0;

        // debug = debug && (g <= 2);

        // ------------------------ create unpacked headers array
        // unpack headers for all the blocks; note that this is tricky
        // because we need to zero-pad final stripe's headers in each block
        // so that stripe widths don't get messed up (from initial data in
        // next block's header)
        uint64_t* header_write_ptr = (uint64_t*)headers;
        for (size_t stripe = 0; stripe < nheader_stripes - 1; stripe++) {
            uint64_t packed_header = *(uint32_t*)header_src;
            uint64_t header = _pdep_u64(packed_header, kHeaderUnpackMask);
            *header_write_ptr = header;
            header_src += stripe_header_sz;
            header_write_ptr++;
        }
        // unpack header for the last stripe in the last block
        // uint64_t packed_header = (*(uint32_t*)header_src) & final_header_mask;
        uint64_t packed_header = (*(uint32_t*)header_src);
        uint64_t header = _pdep_u64(packed_header, kHeaderUnpackMask);
        *header_write_ptr = header;

        // if (debug) { printf("unpacked header: "); dump_bits(headers, ndims * group_sz_blocks); }
        // if (debug) { printf("unpacked header: "); dump_bytes(headers, ndims * group_sz_blocks); }

        // ------------------------ inner loop; decompress each block
        // uint8_t* header_ptr = headers;
        for (int b = 0; b < group_sz_blocks; b++) { // for each block in group
            uint8_t* header_ptr = headers + (b * ndims);
            if (debug) { printf("---- %d.%d nbits: ", (int)g, b); dump_bytes(header_ptr, ndims); }

            // if (debug) { printf("contents of src at block start: \n"); dump_elements(src, block_sz*ndims, block_sz); }

            // run-length decode if necessary
            bool all_zeros = true;
            for (uint16_t dim = 0; dim < ndims; dim++) {
                all_zeros = all_zeros && (header_ptr[dim] == 0);
            }
            if (all_zeros) {
                int8_t* src8 = (int8_t*)src;
                int8_t low_byte = *src8;
                uint8_t high_byte = (uint8_t)*(src8 + 1);
                high_byte = high_byte & (low_byte >> 7); // 0 if low msb == 0
                uint16_t length = (low_byte & 0x7f) | (((uint16_t)high_byte) << 7);

                // write out the run
                if (g > 0 || b > 0) { // if not at very beginning of data
                    const uint_t* inptr = dest - ndims;
                    uint32_t ncopies = length * block_sz;
                    memrep(dest, inptr, ndims * elem_sz, ncopies);
                    dest += ndims * ncopies;
                } else { // deltas of 0 at very start -> all zeros
                    size_t num_zeros = length * block_sz * ndims;
                    memset(dest, 0, num_zeros * elem_sz);
                    dest += num_zeros;
                }
                if (debug) { printf("decompressed rle block of length %d at offset %d\n", length, (int)(dest - orig_dest)); }

                src8++;
                src8 += (high_byte > 0); // if 0, wasn't used for run length
                src = (int_t*)src8;

                continue;
            }

            // ------------------------ unpack data for each dim

            // if (debug) { printf("contents of src before unpack: \n"); dump_elements(src, block_sz*ndims, block_sz); }
            // if (debug) { printf("contents of src before unpack: \n"); dump_bytes(src, block_sz*ndims*elem_sz, block_sz*elem_sz); }

            // uint8_t* delta_buff = deltas + (dim * block_sz);
            for (uint16_t dim = 0; dim < ndims; dim++) {
                uint8_t nbits = header_ptr[dim];
                uint8_t true_nbits = nbits + (nbits == (elem_sz_nbits - 1));
                if (elem_sz == 1) {
                    uint64_t mask = kBitpackMasks8[nbits];
                    int8_t* outptr = ((int8_t*)deltas) + (dim * block_sz);
                    *((uint64_t*)outptr) = _pdep_u64(*(uint64_t*)src, mask);
                } else if (elem_sz == 2) {
                    uint64_t mask = kBitpackMasks16[nbits];
                    // if (debug) { printf("true nbits: %d;  ", true_nbits); }
                    // if (debug) { printf("mask: "); dump_bits(mask); }
                    static const uint8_t stripe_sz = stripe_nbytes / elem_sz;
                    static const uint8_t nstripes =
                        elem_sz * block_sz / stripe_nbytes;

                    uint64_t* delta_buff_u64 = (uint64_t*)deltas;
                    int8_t* src8 = (int8_t*)src;
                    uint16_t total_bit_offset = 0;
                    for (uint8_t s = 0; s < nstripes; s++) {
                        uint32_t out_idx = dim * nstripes + s;
                        uint8_t byte_offset = total_bit_offset >> 3;
                        uint8_t bit_offset = total_bit_offset & 0x07;
                        uint64_t packed_data = *(uint64_t*)(src8 + byte_offset);
                        uint64_t unpacked_data = _pdep_u64(
                            packed_data >> bit_offset, mask);
                        delta_buff_u64[out_idx] = unpacked_data;
                        // delta_buff_u64[out_idx] = _pext_u64(
                            // packed_data >> bit_offset, mask);
                        // if (debug) { printf("packed data:   "); dump_bytes(*(uint64_t*)(src8 + byte_offset)); }
                        if (debug) { printf("unpacked data: "); dump_bytes(unpacked_data); }
                        // if (debug) { printf("unpacked data in buff: "); dump_bytes(delta_buff_u64[out_idx]); }
                        total_bit_offset += true_nbits * stripe_sz;
                        // if (debug) printf("new total bit offset: %d\n", total_bit_offset);
                    }
                }
                // if (debug) { printf("contents of src after unpack: \n"); dump_elements(src, block_sz*ndims, block_sz); }
                int8_t* src8 = ((int8_t*)src) + true_nbits * block_sz / 8;
                src = (int_t*)src8;
                // src += true_nbits * block_sz / 8;
                // printf("%d.%d-%d: nbits=%d\t", (int)g, b, dim, nbits);
                // // printf("src:   "); dump_bytes(src, 8, false);
                // // printf(" -> dest:   "); dump_bytes(outptr, 8);
            }

            // ------------------------ transpose

            if (elem_sz == 1) {
                uint8_t* deltas8 = (uint8_t*)deltas; // still compile if uint_t != uint8_t
                switch (ndims) {
                    // no zigzag or delta coding
                    // case 1: memcpy(dest, deltas, ndims*block_sz*elem_sz); break;
                    // case 2: transpose_2x8_8b(deltas, dest); break;
                    // case 3: transpose_3x8_8b(deltas, dest); break;
                    // case 4: transpose_4x8_8b(deltas, dest); break;

                    case 1: break;
                    case 2: transpose_2x8_8b(deltas8, deltas8); break;
                    case 3: transpose_3x8_8b(deltas8, deltas8); break;
                    case 4: transpose_4x8_8b(deltas8, deltas8); break;
                    default:
                        printf("ERROR: decompress8b_rowmajor_delta_rle_lowdim: "
                            "received invalid ndims: %d\n", ndims);
                        return -1;
                }

                __m256i raw_vdeltas = _mm256_loadu_si256((const __m256i*)deltas);
                __m256i vdeltas = mm256_zigzag_decode_epi8(raw_vdeltas);

                // vars that would have been initialized in various cases
                __m256i swapped128_vdeltas = _mm256_permute2x128_si256(
                    vdeltas, vdeltas, 0x01);
                __m256i shifted15_vdeltas = _mm256_alignr_epi8(
                    swapped128_vdeltas, vdeltas, 15);
                __m256i vals = _mm256_setzero_si256();
                __m256i prev_vals = _mm256_loadu_si256((__m256i*)prev_vals_ar);
                uint8_t prev_val = prev_vals_ar[0];
                switch (ndims) {

                // can't just use a loop because _mm256_srli_si256 demands that
                // the shift amount be a compile-time constant (which it is
                // if the loop is unrolled, but apparently this isn't good enough)
            #define LOOP_BODY(I, SHIFT, DELTA_ARRAY)                              \
                { __m256i shifted_deltas = _mm256_srli_si256(DELTA_ARRAY, SHIFT); \
                vals = _mm256_add_epi8(prev_vals, shifted_deltas);                \
                _mm256_storeu_si256((__m256i*)(dest + I * ndims), vals);          \
                prev_vals = vals; }

                case 1:
                    // this is just some ugliness to deal with the fact that
                    // extract_epi8 technically requires a constant
                    int_t deltas[block_sz];
                    #define EXTRACT_DELTA(IDX) do { deltas[IDX] = _mm256_extract_epi8(vdeltas, IDX); } while(0);
                    EXTRACT_DELTA(0); EXTRACT_DELTA(1);
                    EXTRACT_DELTA(2); EXTRACT_DELTA(3);
                    EXTRACT_DELTA(4); EXTRACT_DELTA(5);
                    EXTRACT_DELTA(6); EXTRACT_DELTA(7);
                    #undef EXTRACT_DELTA
                    #pragma unroll
                    for (uint8_t i = 0; i < block_sz; i++) {
                        // int8_t delta = _mm256_extract_epi8(vdeltas, i);
                        int8_t delta = deltas[i];
                        uint8_t val = prev_val + delta;
                        dest[i] = val;
                        prev_val = val;
                    }
                    prev_vals_ar[0] = prev_val;
                    break;
                case 2: // everything fits in lower 128b
                    LOOP_BODY(0, 0, vdeltas); LOOP_BODY(1, 2, vdeltas);
                    LOOP_BODY(2, 4, vdeltas); LOOP_BODY(3, 6, vdeltas);
                    LOOP_BODY(4, 8, vdeltas); LOOP_BODY(5, 10, vdeltas);
                    LOOP_BODY(6, 12, vdeltas); LOOP_BODY(7, 14, vdeltas);
                    _mm256_storeu_si256((__m256i*)prev_vals_ar, vals);
                    break;
                case 3:
                    LOOP_BODY(0, 0, vdeltas);
                    LOOP_BODY(1, 3, vdeltas);
                    LOOP_BODY(2, 6, vdeltas);
                    LOOP_BODY(3, 9, vdeltas);
                    LOOP_BODY(4, 12, vdeltas);
                    LOOP_BODY(5, 0, shifted15_vdeltas);
                    LOOP_BODY(6, 3, shifted15_vdeltas);
                    LOOP_BODY(7, 6, shifted15_vdeltas);
                    _mm256_storeu_si256((__m256i*)prev_vals_ar, vals);
                    break;
                case 4:
                    LOOP_BODY(0, 0, vdeltas);
                    LOOP_BODY(1, 4, vdeltas);
                    LOOP_BODY(2, 8, vdeltas);
                    LOOP_BODY(3, 12, vdeltas);
                    LOOP_BODY(4, 0, swapped128_vdeltas);
                    LOOP_BODY(5, 4, swapped128_vdeltas);
                    LOOP_BODY(6, 8, swapped128_vdeltas);
                    LOOP_BODY(7, 12, swapped128_vdeltas);
                    _mm256_storeu_si256((__m256i*)prev_vals_ar, vals);
                    break;

            #undef LOOP_BODY
                }
            } else if (elem_sz == 2) {

                // if (debug && g <= 2) {
                //     printf("deltas before transpose: \n"); dump_elements(deltas, ndims * block_sz, block_sz);
                // }

                if (ndims == 2) {
                    // deltas were already u16s if this gets called, but
                    // compiler doesn't know that
                    transpose_2x8_16b((uint16_t*)deltas, (uint16_t*)deltas);
                }

                // if (debug && g <= 2) {
                //     printf("deltas after transpose: \n"); dump_elements(deltas, ndims * block_sz, ndims);
                // }

                __m256i raw_vdeltas = _mm256_loadu_si256((const __m256i*)deltas);
                __m256i vdeltas = mm256_zigzag_decode_epi16(raw_vdeltas);
                __m256i swapped128_vdeltas = _mm256_permute2x128_si256(
                    vdeltas, vdeltas, 0x01);
                __m256i vals = _mm256_setzero_si256();
                __m256i prev_vals = _mm256_loadu_si256((__m256i*)prev_vals_ar);
                uint16_t prev_val = (uint16_t)prev_vals_ar[0];
                switch (ndims) {
                case 1:
                    // this is just some ugliness to deal with the fact that
                    // extract_epi16 technically requires a constant
                    int_t deltas[block_sz];
                    #define EXTRACT_DELTA(IDX) do { deltas[IDX] = _mm256_extract_epi16(vdeltas, IDX); } while(0);
                    EXTRACT_DELTA(0); EXTRACT_DELTA(1);
                    EXTRACT_DELTA(2); EXTRACT_DELTA(3);
                    EXTRACT_DELTA(4); EXTRACT_DELTA(5);
                    EXTRACT_DELTA(6); EXTRACT_DELTA(7);
                    #undef EXTRACT_DELTA

                    #pragma unroll
                    for (uint8_t i = 0; i < block_sz; i++) {
                        // int_t delta = _mm256_extract_epi16(vdeltas, i);
                        int_t delta = deltas[i];
                        uint_t val = prev_val + delta;
                        dest[i] = val;
                        prev_val = val;
                    }
                    prev_vals_ar[0] = prev_val;
                    break;
                case 2:

        #define LOOP_BODY(I, SHIFT, DELTA_ARRAY)                              \
            { __m256i shifted_deltas = _mm256_srli_si256(DELTA_ARRAY, SHIFT); \
            vals = _mm256_add_epi16(prev_vals, shifted_deltas);               \
            _mm256_storeu_si256((__m256i*)(dest + I * ndims), vals);          \
            prev_vals = vals; }

                    LOOP_BODY(0, 0, vdeltas);
                    LOOP_BODY(1, 4, vdeltas);
                    LOOP_BODY(2, 8, vdeltas);
                    LOOP_BODY(3, 12, vdeltas);
                    LOOP_BODY(4, 0, swapped128_vdeltas);
                    LOOP_BODY(5, 4, swapped128_vdeltas);
                    LOOP_BODY(6, 8, swapped128_vdeltas);
                    LOOP_BODY(7, 12, swapped128_vdeltas);
                    _mm256_storeu_si256((__m256i*)prev_vals_ar, vals);
                    break;
        #undef LOOP_BODY
                }
            } // elem_sz
            dest += block_sz * ndims;
        } // for each block
    } // for each group

    free(headers);
    free(deltas);
    free(prev_vals_ar);

    memcpy(dest, src, remaining_len * elem_sz);

    // printf("=== sprintz decomp: memcpying remaining_len elements = %d (%d B)\n", (int)remaining_len, (int)remaining_len * 2);
    // auto ret = dest + remaining_len - orig_dest;
    // printf("final bytes:\n"); dump_bytes((uint8_t*)dest, remaining_len * elem_sz);
    // printf("=== sprintz decomp: returning length %d (%d B)\n", (int)ret, (int)ret * 2);


    if (debug > 2) {
        size_t dest_sz = (dest + remaining_len - orig_dest);
        // printf("decompressed data:\n"); dump_bytes(orig_dest, dest_sz * elem_sz, ndims * elem_sz);
        // printf("decompressed data:\n"); dump_bytes((uint8_t*)orig_dest, 192, ndims * elem_sz);
        // printf("decompressed data:\n"); dump_elements(orig_dest, dest_sz, ndims);
        // printf("decompressed data:\n"); dump_elements(orig_dest, 16, ndims);
    }

    // XXX: if dest and orig_dest not both aligned or not aligned to 2B
    // (which there's no reason they have to be), this can produce an
    // off-by-one error
    return dest + remaining_len - orig_dest;
}

SPRINTZ_FORCE_INLINE int64_t decompress_rowmajor_delta_rle_lowdim_8b(
    const int8_t* src, uint8_t* dest, uint16_t ndims, uint64_t ngroups,
    uint16_t remaining_len)
{
    return decompress_rowmajor_delta_rle_lowdim(src, dest, ndims, ngroups, remaining_len);
}

SPRINTZ_FORCE_INLINE int64_t decompress_rowmajor_delta_rle_lowdim_16b(
    const int16_t* src, uint16_t* dest, uint16_t ndims, uint64_t ngroups,
    uint16_t remaining_len)
{
    return decompress_rowmajor_delta_rle_lowdim(src, dest, ndims, ngroups, remaining_len);
}

int64_t decompress_rowmajor_delta_rle_lowdim_8b(
    const int8_t* src, uint8_t* dest)
{
    uint16_t ndims;
    uint32_t ngroups;
    uint16_t remaining_len;
    src += read_metadata_rle(src, &ndims, &ngroups, &remaining_len);
    return decompress_rowmajor_delta_rle_lowdim(
        src, dest, ndims, ngroups, remaining_len);
}
int64_t decompress_rowmajor_delta_rle_lowdim_16b(
    const int16_t* src, uint16_t* dest)
{
    uint16_t ndims;
    uint32_t ngroups;
    uint16_t remaining_len;
    src += read_metadata_rle(src, &ndims, &ngroups, &remaining_len);
    return decompress_rowmajor_delta_rle_lowdim(
        src, dest, ndims, ngroups, remaining_len);
}
