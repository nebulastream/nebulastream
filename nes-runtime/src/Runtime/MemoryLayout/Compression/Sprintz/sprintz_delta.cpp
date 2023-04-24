//
//  sprintz2.c
//  Compress
//
//  Created by DB on 9/16/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#include "Runtime/MemoryLayout/Compression/Sprintz/sprintz_delta.h"

#include <stdio.h>

#include <assert.h>
#include <stdint.h>
#include <string.h>

#include "Runtime/MemoryLayout/Compression/Sprintz/bitpack.h"
#include "Runtime/MemoryLayout/Compression/Sprintz/format.h"
#include "Runtime/MemoryLayout/Compression/Sprintz/util.h"// for memrep

// #include "array_utils.hpp" // TODO rm
#include "Runtime/MemoryLayout/Compression/Sprintz/debug_utils.hpp"// TODO rm

static const int debug = 0;
// static const int debug = 3;
// static const int debug = 4;

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

// // TODO this should just be in one header
// #define CHECK_INT_UINT_TYPES_VALID(int_t, uint_t)               \
//     static_assert(sizeof(uint_t) == sizeof(int_t),              \
//         "uint type and int type sizes must be the same!");      \
//     static_assert(sizeof(uint_t) == 1 || sizeof(uint_t) == 2,   \
//         "Only element sizes of 1 and 2 bytes are supported!");  \

// TODO move to debug utils
#define PTR_DIFF_NBYTES(P1, P2) \
    (int)(((uint8_t*)P1) - ((uint8_t*)P2))


// ------------------------------------------------ row-major, no delta or RLE

template<typename int_t, typename uint_t>
int64_t compress_rowmajor(const uint_t* src, uint32_t len, int_t* dest,
                            uint16_t ndims, bool write_size)
{
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(uint_t);
    static const uint8_t elem_sz_nbits = 8 * elem_sz;
    static const int nbits_sz_bits = elem_sz == 1 ? 3 : 4; // XXX only {8,16}b
    // constants that could, in principle, be changed (but not in this impl)
    static const int block_sz = 8;
    static const int stripe_sz_nbytes = 8;
    // constants that could actually be changed in this impl
    static const int group_sz_blocks = kDefaultGroupSzBlocks;
    // derived constants
    static const int stripe_sz = stripe_sz_nbytes / elem_sz;

    const uint_t* orig_src = src;
    int_t* orig_dest = dest;

    int gDebug = debug;
    int debug = (elem_sz == 2) ? gDebug : 0;
    debug = false;

    // ================================ one-time initialization

    // ------------------------ stats derived from ndims
    uint16_t nstripes = DIV_ROUND_UP(ndims, stripe_sz);
    uint32_t group_sz = ndims * block_sz * group_sz_blocks;
    uint32_t total_header_bits = ndims * nbits_sz_bits * group_sz_blocks;
    uint32_t total_header_bytes = DIV_ROUND_UP(total_header_bits, 8);

    // ------------------------ store data size and number of dimensions
    if (write_size) {
        dest += write_metadata_simple(dest, ndims, len);
    }
    // handle low dims and low length; we'd read way past the end of the
    // input in this case
    if (len < 8 * block_sz * group_sz_blocks) {
        uint32_t remaining_len = (uint32_t)(len - (src - orig_src));
        memcpy(dest, src, remaining_len * elem_sz);
        return dest + remaining_len - orig_dest;
    }

    // {
    //     printf("------ input length, ndims = %lu, %d\n", len, ndims);
    //     uint64_t written = *(uint64_t*)orig_dest;
    //     int64_t written_length = written & (((uint64_t)1 << 48) - 1);
    //     uint16_t written_ndims = (uint16_t)(written >> 48);
    //     printf("just wrote out length %lld, ndims %d\n", written_length, written_ndims);
    // }

    if (debug > 0) {
        printf("-------- compression (len = %lld)\n", (long long)len);
        if (debug > 2) {
            printf("saw original data:\n"); dump_elements(src, len, ndims);
        }
    }

    if (debug > 1) { printf("total header bits, bytes: %d, %d\n", total_header_bits, total_header_bytes); }

    // ------------------------ temp storage
    uint8_t*  stripe_bitwidths  = (uint8_t*) malloc(nstripes*sizeof(uint8_t));
    uint32_t* stripe_bitoffsets = (uint32_t*)malloc(nstripes*sizeof(uint32_t));
    uint64_t* stripe_masks      = (uint64_t*)malloc(nstripes*sizeof(uint64_t));
    uint32_t* stripe_headers    = (uint32_t*)malloc(nstripes*sizeof(uint32_t));
    uint8_t*  header_bytes      = (uint8_t*) calloc(total_header_bytes + 4, 1);

    // ================================ main loop

    if (debug) {
        printf("---- pre-loop (%d -> %d)\n",
            (int)(src - orig_src)*elem_sz, (int)(dest - orig_dest)*elem_sz);
    }

    uint64_t ngroups = len / group_sz;
    for (uint64_t g = 0; g < ngroups; g++) {
        int8_t* header_dest = (int8_t*)dest;
        dest = (int_t*)(((int8_t*)dest) + total_header_bytes);

        // printf("headerdest")

        memset(header_bytes, 0, total_header_bytes + 4);
        memset(header_dest, 0, total_header_bytes);

        uint32_t header_bit_offset = 0;
        for (int b = 0; b < group_sz_blocks; b++) { // for each block in group

            if (debug) {
                printf("---- %d.%d (%d -> %d):\n", (int)g, (int)b,
                    PTR_DIFF_NBYTES(src, orig_src), PTR_DIFF_NBYTES(dest, orig_dest));
                    // (int)(src - orig_src)*elem_sz, (int)(dest - orig_dest)*elem_sz);
            }

            // ------------------------ zero stripe info from previous iter
            memset(stripe_bitwidths, 0, nstripes * sizeof(stripe_bitwidths[0]));
            memset(stripe_masks,     0, nstripes * sizeof(stripe_masks[0]));
            memset(stripe_headers,   0, nstripes * sizeof(stripe_headers[0]));

            // ------------------------ compute info for each stripe
            for (uint16_t dim = 0; dim < ndims; dim++) {
                // compute maximum number of bits used by any value of this dim
                uint_t mask = 0;
                for (int i = 0; i < block_sz; i++) {
                    uint_t val = src[(i * ndims) + dim];
                    mask |= val;
                }
                if (elem_sz == 1) {
                    mask = NBITS_MASKS_U8[mask];
                } else if (elem_sz == 2) {
                    // mask |= mask >> 1;
                    // mask |= mask >> 2;
                    // mask |= mask >> 4;
                    // mask |= mask >> 8;
                    uint8_t upper_mask = NBITS_MASKS_U8[mask >> 8];
                    mask = upper_mask > 0 ? (upper_mask << 8) + 255 : NBITS_MASKS_U8[mask];
                    // mask = 0xffff; // TODO rm
                    // mask = NBITS_MASKS_U8[mask] | (NBITS_MASKS_U8[mask >> 8] << 8);
                }
                uint8_t max_nbits = (32 - _lzcnt_u32((uint32_t)mask));

                uint16_t stripe = dim / stripe_sz;
                uint8_t idx_in_stripe = dim % stripe_sz;

                // accumulate stats about this stripe
                stripe_bitwidths[stripe] += max_nbits;
                stripe_masks[stripe] |= ((uint64_t)mask) << (idx_in_stripe * elem_sz_nbits);

                // accumulate header info for this stripe
                uint32_t write_nbits = max_nbits - (max_nbits == elem_sz_nbits); // map 8 to 7
                stripe_headers[stripe] |= write_nbits << (idx_in_stripe * nbits_sz_bits);
                // printf("write_nbits = %d, stripe header = ", write_nbits);
                // dump_bytes(stripe_headers[stripe], false); dump_bits(stripe_headers[stripe]);
            }
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

            // ------------------------ write out header bits for this block
            for (uint32_t stripe = 0; stripe < nstripes; stripe++) {
                uint16_t byte_offset = header_bit_offset >> 3;
                uint16_t bit_offset = header_bit_offset & 0x07;

                if (elem_sz == 1) {
                    *(uint32_t*)(header_dest + byte_offset) |= \
                        stripe_headers[stripe] << bit_offset;
                } else {
                    // if (debug > 0) {
                    //     printf("prev contents of header dest: ");
                    //     dump_bytes(header_dest, 8, true);
                    //     printf("ORing in header value: %d\n", (int)(stripe_headers[stripe] << bit_offset));
                    // }
                    *(uint64_t*)(header_dest + byte_offset) |= \
                        ((uint64_t)(stripe_headers[stripe])) << bit_offset;
                    // if (debug > 0) {
                    //     printf("new contents of header dest: ");
                    //     dump_bytes(header_dest, 8, false);
                    //     printf(" \t(bit, byte offsets = %d, %d)\n", bit_offset, byte_offset);
                    // }
                }

                uint8_t is_final_stripe = stripe == (nstripes - 1);
                uint8_t has_trailing_dims = (ndims % stripe_sz) != 0;
                uint8_t add_ndims = is_final_stripe && has_trailing_dims ?
                    ndims % stripe_sz : stripe_sz;
                header_bit_offset += nbits_sz_bits * add_ndims;
            }

            // if (debug > 1) {
            //     printf("wrote headers: "); dump_bytes(header_dest, total_header_bytes);
            // //     ar::print(stripe_bitwidths, nstripes, "stripe_bitwidths");
            //     // printf("row width bits: %d\n", row_width_bits);
            // }

            // zero output so that we can just OR in bits (and also touch each
            // cache line in order to ensure that we prefetch, despite our
            // weird pattern of writes)
            // memset(dest, 0, nstripes * stripe_sz * block_sz);
            // if (debug) { printf("header before zeroing: "); dump_bytes(header_dest, 8); }
            memset(dest, 0, out_row_nbytes * block_sz); // above line can overrun dest buff

            // if (debug) { printf("header before block data write: "); dump_bytes(header_dest, 8); }
            if (debug) {
                printf("stripe masks: "); dump_bytes((uint8_t*)stripe_masks, 32);
            }

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
                const uint8_t* inptr = ((const uint8_t*)src) + (stripe * stripe_sz_nbytes);

                // printf("total bits, nbits lost = %d, %d\n", total_bits, nbits_lost);
                if (debug > 1) {
                    printf("%d.%d.%d: ", (int)g, (int)b, (int)stripe);
                    // printf("offset bytes, offset bits, nbits, total_bits = %u, %u, %u, %d\n",
                    //     offset_bytes, offset_bits, nbits, total_bits);
                    printf("src, dest offsets = %d -> %d\n", (int)(src - orig_src), (int)(dest - orig_dest));
                    printf("mask: "); dump_bits(mask);
                }
                // printf("using mask: "); dump_bytes(mask);

                // XXX Note that this impl assumes that output buff is zeroed
                if (total_bits <= 64) { // always fits in one u64
                    for (int i = 0; i < block_sz; i++) { // for each sample in block

                        // uint8_t orig_outptr_byte = *outptr;

                        // 8B write to store (at least most of) the data
                        uint64_t data = *(uint64_t*)inptr;
                        uint64_t packed_data = _pext_u64(data, mask);
                        uint64_t write_data = packed_data << offset_bits;
                        *(uint64_t*)outptr = write_data | (*(uint64_t*)outptr);

                        // printf("packing block: "); dump_bytes(data, false);
                        // printf("\t-> "); dump_bytes(*(uint64_t*)outptr);

                        // printf("packing block: "); dumpEndianBits(data, false);
                        // printf("\t-> "); dumpEndianBits(orig_outptr_byte, false); printf(" | "); dumpEndianBits(packed_data, false); printf(" >> %d", offset_bits);
                        // printf("\t-> "); dumpEndianBits(*(uint64_t*)outptr, false); dumpEndianBits(*(outptr+8));

                        outptr += out_row_nbytes;
                        inptr += ndims * elem_sz;
                    }
                } else { // data spans 9 bytes
                    // printf(">>> executing the slow path!\n");
                    uint8_t nbits_lost = total_bits - 64;
                    for (int i = 0; i < block_sz; i++) { // for each sample in block
                        // uint8_t orig_outptr_byte = *outptr; // TODO rm after debug
                        uint64_t data = *(uint64_t*)inptr;
                        uint64_t packed_data = _pext_u64(data, mask);
                        uint8_t extra_byte = (uint8_t)(packed_data >> (nbits - nbits_lost));
                        uint64_t write_data = packed_data << offset_bits;
                        *(uint64_t*)outptr = write_data | (*(uint64_t*)outptr);
                        *(outptr + 8) = extra_byte;

                        // printf("packing block: "); dump_bytes(data, false);
                        // printf("\t-> "); dump_bytes(*(uint64_t*)outptr);

                        // printf("extra byte = "); dumpEndianBits(extra_byte);
                        // printf("packing block: "); dumpEndianBits(data, false);
                        // printf("\t-> "); dumpEndianBits(orig_outptr_byte, false); printf(" | "); dumpEndianBits(packed_data, false); printf(" >> %d", offset_bits);
                        // printf("\t-> "); dumpEndianBits(*(uint64_t*)outptr, false); dumpEndianBits(*(outptr+8));

                        outptr += out_row_nbytes;
                        inptr += ndims * elem_sz;
                    }
                }
                // printf("read back header: "); dumpEndianBits(*(uint32_t*)(header_dest - stripe_header_sz));
            } // for each stripe

            // if (debug) { printf("header dest at end of block: "); dump_bytes(header_dest, 8); }

            src += block_sz * ndims;
            dest += block_sz * out_row_nbytes / elem_sz;
        } // for each block

        // if (dump_group) {
        //     // printf("stripe headers: "); dump_bytes(stripe_headers, nstripes * 4);
        //     printf("> hit end of group; header start = %lld <\n",
        //         (uint64_t)(header_dest - orig_dest));
        //     printf("header dest bytes: "); dump_bytes(header_dest, total_header_bytes);
        //     printf("header dest bits:  "); dump_bits(header_dest, total_header_bytes);
        // }

    } // for each group

    free(stripe_bitwidths);
    free(stripe_bitoffsets);
    free(stripe_masks);
    free(stripe_headers);

    uint32_t remaining_len = (uint32_t)(len - (src - orig_src));
    // printf("read src bytes: %lu\n", (uint32_t)(src - orig_src));
    // printf("remaining_len: %lu\n", remaining_len);
    // printf("remaining data: "); ar::print(src, remaining_len);
    memcpy(dest, src, remaining_len * elem_sz);

    // {
    //     uint64_t written = *(uint64_t*)orig_dest;
    //     int64_t written_length = written & (((uint64_t)1 << 48) - 1);
    //     uint16_t written_ndims = (uint16_t)(written >> 48);
    //     printf("final written length %lld, ndims %d\n", written_length, written_ndims);
    // }

    // printf("mysterious header values at end of compression:\n");
    // printf("mystery header bytes: "); dump_bytes(orig_dest + 753737, 1);
    // printf("mystery header bits:  "); dump_bits(orig_dest + 753737, 1);

    if (debug > 3) {
        uint32_t dest_sz = (uint32_t)   (dest + remaining_len - orig_dest) * elem_sz;
        printf("wrote compressed data:\n"); dump_bytes(orig_dest + 5, dest_sz, ndims * elem_sz);
        // printf("wrote compressed data:\n"); dump_bytes(orig_dest + 3, dest_sz, ndims * elem_sz);
    }

    // uint32_t dest_sz = dest + remaining_len - orig_dest;
    // if (dest_sz >= len) { // if made things larger, just mempcpy
    //     memcpy(orig_dest + 8, orig_src, len);
    //     *(uint64_t*)orig_dest |= ((uint64_t)1) << 47;
    //     return len + 8;
    // }

    return dest + remaining_len - orig_dest;
}

int64_t compress_rowmajor_8b(const uint8_t* src, uint32_t len, int8_t* dest,
                            uint16_t ndims, bool write_size)
{
    return compress_rowmajor(src, len, dest, ndims, write_size);
}
int64_t compress_rowmajor_16b(const uint16_t* src, uint32_t len, int16_t* dest,
                            uint16_t ndims, bool write_size)
{
    return compress_rowmajor(src, len, dest, ndims, write_size);
}

template<typename int_t, typename uint_t>
int64_t decompress_rowmajor(const int_t* src, uint_t* dest) {

    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(uint_t);
    typedef typename ElemSzTraits<elem_sz>::bitwidth_t bitwidth_t;

    static const uint8_t elem_sz_nbits = 8 * elem_sz;
    static const uint8_t nbits_sz_bits = elem_sz == 1 ? 3 : 4; // XXX {8,16}b
    // constants that could, in principle, be changed (but not in this impl)
    static const uint8_t block_sz = 8;
    static const uint8_t stripe_sz = 8 / elem_sz;
    static const uint8_t stripe_nbytes = 8;
    static const uint8_t vector_sz_nbytes = 32;
    // constants that could actually be changed in this impl
    static const uint8_t group_sz_blocks = kDefaultGroupSzBlocks;
    // derived constants
    static const int group_sz_per_dim = block_sz * group_sz_blocks;
    static const uint8_t stripe_header_sz = nbits_sz_bits * stripe_nbytes / 8;
    static const uint8_t nbits_sz_mask = (1 << nbits_sz_bits) - 1;
    static const uint64_t kHeaderUnpackMask = TILE_BYTE(nbits_sz_mask);
    static const uint8_t vector_sz = vector_sz_nbytes / elem_sz;
    // assert(stripe_sz % 8 == 0);
    assert(vector_sz % stripe_sz == 0);
    assert(vector_sz >= stripe_sz);

    uint_t* orig_dest = dest;
    const int_t* orig_src = src;

    int gDebug = debug;
    // int debug = (elem_sz == 2) ? gDebug : 0;
    int debug = false;

    // ================================ one-time initialization

    // ------------------------ read original data size, ndims
    // static const uint32_t len_nbytes = 6;
    // uint64_t one = 1; // make next line legible
    // uint64_t len_mask = (one << (8 * len_nbytes)) - 1;
    // uint64_t orig_len = (*(uint64_t*)src) & len_mask;
    // uint16_t ndims = (*(uint16_t*)(src + len_nbytes));
    // src += 8;
    uint16_t ndims;
    uint32_t orig_len; // TODO make len a u32
    src += read_metadata_simple(src, &ndims, &orig_len);
    // uint32_t _len32; // TODO make len a u32
    // src += read_metadata_simple(src, &ndims, &_len32);
    // uint64_t orig_len = _len32;

    bool just_cpy = orig_len < 8 * block_sz * group_sz_blocks;
    // just_cpy = just_cpy || orig_len & (((uint64_t)1) << 47);
    if (just_cpy) { // if data was too small
        memcpy(dest, src, orig_len * elem_sz);
        return orig_len;
    }

    // printf("------ decomp: saw original length, ndims = %llu, %d\n", orig_len, ndims);
    // printf("decomp: src addr, dest addr = %p, %p\n", (void*)src, (void*)dest);

    if (ndims == 0) {
        perror("ERROR: Received ndims of 0!");
        return 0;
    }

    if (debug > 0) {
        printf("-------- decompression (orig_len = %lld)\n", (long long)orig_len);
        if (debug > 3) {
            printf("saw compressed data (with possible extra at end):\n");
            dump_bytes(src, orig_len + 8, ndims * elem_sz);
            // dump_bytes(src + 4, orig_len + 8, ndims * elem_sz);
            // dump_elements(src, orig_len + 4, ndims);
        }
    }
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
    uint32_t group_sz = ndims * group_sz_per_dim;
    uint16_t nstripes = DIV_ROUND_UP(ndims, stripe_sz);

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
    bitwidth_t* stripe_bitwidths  = (bitwidth_t*)calloc(nstripes_in_vectors, 8);
    uint32_t* stripe_bitoffsets = (uint32_t*)calloc(nstripes, 4);

    // ================================ main loop

    if (debug) {
        printf("---- pre-loop (%d -> %d)\n",
            (int)(src - orig_src)*elem_sz, (int)(dest - orig_dest)*elem_sz);
    }

    uint64_t ngroups = orig_len / group_sz; // if we get an fp error, it's this
    for (uint64_t g = 0; g < ngroups; g++) {
        const uint8_t* header_src = (const uint8_t*)src;
        src = (int_t*)(((int8_t*)src) + total_header_bytes);

        if (debug) printf("======== g = %d\n", (int)g);

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
            if (debug) {
                printf("unpacked header: "); dump_bytes(header);
            }
            *header_write_ptr = header;
            header_write_ptr++;
        }
        // unpack header for the last stripe in the last block
        uint64_t packed_header = (*(uint32_t*)header_src) & final_header_mask;
        uint64_t header = _pdep_u64(packed_header, kHeaderUnpackMask);
        *header_write_ptr = header;

        // insert zeros between the unpacked headers for each block so that
        // the stripe bitwidths, etc, are easy to compute; this enforces
        // the invariant that each stripe contains header info from exactly
        // one block
        uint8_t* header_in_ptr = (uint8_t*)headers_tmp;
        // uint32_t header_pad_nstripes = DIV_ROUND_UP(ndims, stripe_nbytes);
        // uint32_t header_padded_ndims = header_pad_nstripes * stripe_nbytes;
        uint32_t header_pad_nstripes = DIV_ROUND_UP(ndims, stripe_sz);
        uint32_t header_padded_ndims = header_pad_nstripes * stripe_sz;
        for (uint32_t b = 0; b < group_sz_blocks; b++) {
            uint32_t src_offset = b * ndims;
            uint32_t dest_offset = b * header_padded_ndims;
            // uint32_t dest_offset = b * nstripes * stripe_nbytes;
            memcpy(headers + dest_offset, header_in_ptr + src_offset, ndims);
        }

        // printf("header_tmp:    "); dump_bytes(headers_tmp, nheader_stripes * 8);
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

                // if (debug) {
                //     printf("storing mask 0 at byte offset:  %d\n", (int)(store_addr2 - (uint8_t*)data_masks));
                //     printf("storing mask 1 at byte offset:  %d\n", (int)(store_addr2 + v_offset - (uint8_t*)data_masks));
                //     // printf("raw    header: "); dump_m256i<uint8_t>(raw_header);
                //     // printf("raw == 15?   : "); dump_m256i<uint8_t>(_mm256_cmpeq_epi8(raw_header, fifteens));
                //     printf("vector header: "); dump_m256i<uint8_t>(header);
                //     // printf("u32 masks: "); dump_m256i<uint8_t>(u32_masks);
                //     // printf("even u32s: "); dump_m256i<uint8_t>(even_u32s);
                //     // printf("odd u32s: "); dump_m256i<uint8_t>(odd_u32s);
                //     // printf("even bitwidths: "); dump_m256i<uint64_t>(even_bitwidths);
                //     // printf("odd bitwidths: "); dump_m256i<uint64_t>(odd_bitwidths);
                    // printf("stripe bitwidths: "); dump_m256i<uint32_t>(bitwidths);
                    // printf("vector masks0: "); dump_m256i<uint16_t>(masks0);
                    // printf("vector masks1: "); dump_m256i<uint16_t>(masks1);
                //     printf("\n");
                // }
            }
        }

        if (debug) {
            printf("padded masks:     "); dump_elements((uint16_t*)data_masks, group_header_sz);
            // printf("padded masks:     "); dump_bytes((uint8_t*)data_masks, group_header_sz * elem_sz);
            printf("padded bitwidths: "); dump_bytes(stripe_bitwidths, nstripes_in_vectors);
        }

        // ------------------------ inner loop; decompress each block
        uint64_t* masks = data_masks;
        bitwidth_t* bitwidths = stripe_bitwidths;
        for (int b = 0; b < group_sz_blocks; b++) { // for each block in group

            if (debug) {
                printf("---- %d.%d (%d -> %d):\n", (int)g, (int)b,
                    (int)(src - orig_src)*elem_sz, (int)(dest - orig_dest)*elem_sz);
            }

            // compute where each stripe begins, as well as width of a row
            stripe_bitoffsets[0] = 0;
            for (uint32_t stripe = 1; stripe < nstripes; stripe++) {
                stripe_bitoffsets[stripe] = (uint32_t)(stripe_bitoffsets[stripe - 1]
                    + bitwidths[stripe - 1]);
            }
            uint32_t in_row_nbits = (uint32_t)(stripe_bitoffsets[nstripes - 1] +
                bitwidths[nstripes - 1]);
            // uint32_t in_row_nbytes = (in_row_nbits >> 3) + ((in_row_nbits % 8) > 0);
            uint32_t in_row_nbytes = DIV_ROUND_UP(in_row_nbits, 8);
            uint32_t out_row_nbytes = ndims * elem_sz;

            // printf("---- block %d\n", b);
            // printf("padded masks again:     "); dump_bytes(data_masks, group_header_sz);
            // printf("padded bitwidths again: "); dump_elements(stripe_bitwidths, nstripes_in_vectors);
            // printf("local bitwidths: "); dump_elements(bitwidths, nstripes);

            // ar::print(stripe_bitwidths, nstripes, "stripe_bitwidths");
            // ar::print(bitwidths, nstripes, "stripe bitwidths for this block");
            // ar::print(stripe_bitoffsets, nstripes, "stripe_bitoffsets");
            // printf("row sz bits, row sz bytes: %d, %d\n", in_row_nbits, in_row_nbytes);

            // TODO see if prefetching and/or touching input and output cache
            // lines in order (instead of weird strided order below) helps
            // performance

            // if (debug > 1) {
            //     // printf("wrote headers: "); dump_bytes(header_dest, total_header_bytes);
            // //     ar::print(stripe_bitwidths, nstripes, "stripe_bitwidths");
            //     printf("row width bits: %d\n", in_row_nbits);
            //     printf("input row nbytes, output row nbytes = %d, %d\n", in_row_nbytes, out_row_nbytes);
            // }

            // ------------------------ unpack data for each stripe
            // for (uint32_t stripe = 0; stripe < nstripes; stripe++) {
            for (int stripe = nstripes - 1; stripe >= 0; stripe--) {
                if (debug > 1) {
                    printf("%d.%d.%d: ", (int)g, (int)b, (int)stripe);
                    // printf("offset bytes, offset bits, nbits, total_bits = %u, %u, %u, %d\n",
                    //     offset_bytes, offset_bits, nbits, total_bits);
                    printf("src, dest offsets = %d -> %d\n", (int)(src - orig_src), (int)(dest - orig_dest));
                }

                uint32_t offset_bits = stripe_bitoffsets[stripe] & 0x07;
                uint32_t offset_bytes = stripe_bitoffsets[stripe] >> 3;

                uint64_t mask = masks[stripe];
                uint8_t nbits = bitwidths[stripe];
                uint8_t total_bits = nbits + offset_bits;

                const int8_t* inptr = ((const int8_t*)src) + offset_bytes;
                uint8_t* outptr = ((uint8_t*)dest) + (stripe * stripe_nbytes);

                if (debug) {
                //     // printf("nbits at idx %d = %d\n", stripe + nstripes * b, nbits);
                    printf("total bits, offset bytes, bits = %u, %u, %u\n", total_bits, offset_bytes, offset_bits);
                    printf("using mask: "); dump_bytes(mask);
                }

                // this is the hot loop
                if (total_bits <= 64) { // input guaranteed to fit in 8B
                    for (int i = 0; i < block_sz; i++) {
                        uint64_t packed_data = (*(uint64_t*)inptr) >> offset_bits;
                        // if (debug) { printf("packed_data "); dump_bytes(packed_data); }
                        *(uint64_t*)outptr = _pdep_u64(packed_data, mask);
                        inptr += in_row_nbytes;
                        outptr += out_row_nbytes;
                    }
                } else { // input spans 9 bytes
                    // printf(">>> executing the slow path!\n");
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
                // if (debug) { printf("data we wrote:\n"); dump_bytes((uint8_t*)dest, block_sz*ndims*elem_sz, ndims * elem_sz); }
            } // for each stripe

            src += block_sz * in_row_nbytes / elem_sz;
            dest += block_sz * out_row_nbytes / elem_sz;
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

    // printf("bytes read: %lld\n", (uint64_t)(src - orig_src));

    // copy over trailing data
    // uint32_t remaining_len = orig_len % group_sz;
    // uint32_t remaining_len = orig_len - (src - orig_src);
    uint32_t remaining_len = (uint32_t)(orig_len - (uint32_t)(dest - orig_dest));
    if (debug) { printf("orig len, remaining len: %u, %u\n", orig_len, remaining_len); }
    // printf("read bytes: %lu\n", remaining_len);
    // if (debug > 2) { printf("remaining data: "); ar::print(src, remaining_len); }
    memcpy(dest, src, remaining_len * elem_sz);

    // uint32_t dest_sz = dest + remaining_len - orig_dest;
    // printf("decompressed data:\n"); dump_bytes(orig_dest, dest_sz, ndims);
    // ar::print(orig_dest, dest_sz, "decompressed data");

    if (debug > 2) {
        // printf("decompressed data:\n"); dump_bytes(orig_dest, orig_len * elem_sz, ndims * 4);
        printf("decompressed data:\n"); dump_elements(orig_dest, orig_len, ndims);
    }

    // printf("reached end of decomp\n");
    return dest + remaining_len - orig_dest;
}

int64_t decompress_rowmajor_8b(const int8_t* src, uint8_t* dest) {
    return decompress_rowmajor(src, dest);
}
int64_t decompress_rowmajor_16b(const int16_t* src, uint16_t* dest) {
    return decompress_rowmajor(src, dest);
}

// ========================================================== rowmajor delta

template<typename int_t, typename uint_t>
int64_t compress_rowmajor_delta(const uint_t* src, uint32_t len, int_t* dest,
                                uint16_t ndims, bool write_size)
{
    CHECK_INT_UINT_TYPES_VALID(int_t, uint_t);
    static const uint8_t elem_sz = sizeof(uint_t);
    static const uint8_t elem_sz_nbits = 8 * elem_sz;
    static const uint8_t nbits_sz_bits = elem_sz == 1 ? 3 : 4; // XXX only {8,16}b
    // constants that could, in principle, be changed (but not in this impl)
    static const int block_sz = 8;
    static const int stripe_sz_nbytes = 8;
    // constants that could actually be changed in this impl
    static const int group_sz_blocks = kDefaultGroupSzBlocks;
    static const int stripe_sz = stripe_sz_nbytes / elem_sz;

    const uint_t* orig_src = src;
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
    if (write_size) {
        dest += write_metadata_simple(dest, ndims, len);
    }
    // handle low dims and low length; we'd read way past the end of the
    // input in this case
    if (len < 8 * block_sz * group_sz_blocks) {
        uint32_t remaining_len = (uint32_t)(len - (src - orig_src));
        memcpy(dest, src, remaining_len * elem_sz);
        return dest + remaining_len - orig_dest;
    }

    if (debug > 0) {
        printf("-------- compression (len = %lld)\n", (long long)len);
        if (debug > 2) {
            printf("saw original data:\n"); dump_elements(src, len, ndims);
        }
    }
    if (debug > 1) { printf("total header bits, bytes: %d, %d\n", total_header_bits, total_header_bytes); }

    // ------------------------ temp storage
    uint8_t*  stripe_bitwidths  = (uint8_t*) malloc(nstripes*sizeof(uint8_t));
    uint32_t* stripe_bitoffsets = (uint32_t*)malloc(nstripes*sizeof(uint32_t));
    uint64_t* stripe_masks      = (uint64_t*)malloc(nstripes*sizeof(uint64_t));
    uint32_t* stripe_headers    = (uint32_t*)malloc(nstripes*sizeof(uint32_t));

    uint32_t total_header_bytes_padded = total_header_bytes + 4;
    uint8_t* header_bytes = (uint8_t*)calloc(total_header_bytes_padded, 1);

    // extra row is for storing previous values
    // TODO just look at src and special case first row
    // uint8_t* deltas = (uint8_t*)calloc(1, (block_sz + 1) * ndims);
    int_t* deltas = (int_t*)calloc(elem_sz, (block_sz + 1) * ndims);
    uint_t* prev_vals_ar = (uint_t*)(deltas + block_sz * ndims);

    // ================================ main loop

    uint64_t ngroups = len / group_sz;
    for (uint64_t g = 0; g < ngroups; g++) {
        int8_t* header_dest = (int8_t*)dest;
        dest = (int_t*)(((int8_t*)dest) + total_header_bytes);

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
            // printf("comp: starting deltas:\n"); dump_bytes(deltas, (block_sz + 1) * ndims, ndims);

            for (uint16_t dim = 0; dim < ndims; dim++) {
                // compute maximum number of bits used by any value of this dim,
                // while simultaneously computing deltas
                uint_t mask = 0;
                // uint32_t prev_val_offset = block_sz * ndims + dim;
                // uint8_t prev_val = deltas[prev_val_offset];
                uint_t prev_val = prev_vals_ar[dim];
                // uint8_t val = 0;
                for (uint8_t i = 0; i < block_sz; i++) {
                    uint32_t offset = (i * ndims) + dim;
                    uint_t val = src[offset];
                    int_t delta = (int_t)(val - prev_val);
                    uint_t bits = ZIGZAG_ENCODE_SCALAR(delta);
                    // uint_t bits = val; // TODO rm
                    mask |= bits;
                    deltas[offset] = bits;
                    prev_val = val;
                }
                prev_vals_ar[dim] = prev_val;

                if (elem_sz == 1) {
                    mask = NBITS_MASKS_U8[mask];
                } else if (elem_sz == 2) {
                    uint8_t upper_mask = NBITS_MASKS_U8[mask >> 8];
                    mask = upper_mask > 0 ? (upper_mask << 8) + 255 : NBITS_MASKS_U8[mask];
                    // mask = 0xffff; // TODO rm
                }
                uint8_t max_nbits = (32 - _lzcnt_u32((uint32_t)mask));

                uint16_t stripe = dim / stripe_sz;
                uint8_t idx_in_stripe = dim % stripe_sz;

                // accumulate stats about this stripe
                stripe_bitwidths[stripe] += max_nbits;
                stripe_masks[stripe] |= ((uint64_t)mask) << (idx_in_stripe * elem_sz_nbits);

                // accumulate header info for this stripe
                uint32_t write_nbits = max_nbits - (max_nbits == elem_sz_nbits); // map 8 to 7
                stripe_headers[stripe] |= write_nbits << (idx_in_stripe * nbits_sz_bits);
            }
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

            // ar::print(stripe_bitwidths, nstripes, "stripe_bitwidths");
            // printf("row width bits: %d\n", row_width_bits);

            // zero output so that we can just OR in bits (and also touch each
            // cache line in order to ensure that we prefetch, despite our
            // weird pattern of writes)
            memset(dest, 0, out_row_nbytes * block_sz);

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
                const uint8_t* inptr = ((const uint8_t*)deltas) + (stripe * stripe_sz_nbytes);

                if (debug > 1) {
                    printf("%d.%d.%d: ", (int)g, (int)b, (int)stripe);
                    // printf("offset bytes, offset bits, nbits, total_bits = %u, %u, %u, %d\n",
                    //     offset_bytes, offset_bits, nbits, total_bits);
                    printf("src, dest offsets = %d -> %d\n", (int)(src - orig_src), (int)(dest - orig_dest));
                    printf("mask: "); dump_bits(mask);
                }

                if (total_bits <= 64) { // always fits in one u64
                    for (int i = 0; i < block_sz; i++) { // for each sample in block
                        // 8B write to store (at least most of) the data
                        uint64_t data = *(uint64_t*)inptr;
                        uint64_t packed_data = _pext_u64(data, mask);
                        uint64_t write_data = packed_data << offset_bits;
                        *(uint64_t*)outptr = write_data | (*(uint64_t*)outptr);

                        outptr += out_row_nbytes;
                        inptr += ndims * elem_sz;
                    }
                } else { // data spans 9 bytes
                    // printf(">>> executing the slow path!\n");
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
        } // for each block
    } // for each group

    free(stripe_bitwidths);
    free(stripe_bitoffsets);
    free(stripe_masks);
    free(stripe_headers);
    free(deltas);

    uint32_t remaining_len = (uint32_t)(len - (src - orig_src));
    if (debug) { printf("remaining len: %d\n", remaining_len); }
    memcpy(dest, src, remaining_len * elem_sz);
    return dest + remaining_len - orig_dest;
}

int64_t compress_rowmajor_delta_8b(const uint8_t* src, uint32_t len,
    int8_t* dest, uint16_t ndims, bool write_size)
{
    return compress_rowmajor_delta(src, len, dest, ndims, write_size);
}
int64_t compress_rowmajor_delta_16b(const uint16_t* src, uint32_t len,
    int16_t* dest, uint16_t ndims, bool write_size)
{
    return compress_rowmajor_delta(src, len, dest, ndims, write_size);
}

// int64_t decompress_rowmajor_delta(const int8_t* src, uint8_t* dest) {
template<typename int_t, typename uint_t>
int64_t decompress_rowmajor_delta(const int_t* src, uint_t* dest) {
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
    static const uint8_t vector_sz = vector_sz_nbytes / elem_sz;
    // assert(stripe_sz % 8 == 0);
    assert(vector_sz % stripe_sz == 0);
    assert(vector_sz >= stripe_sz);

    // uint8_t* orig_dest = dest;
    // const int8_t* orig_src = src;

    uint_t* orig_dest = dest;
    const int_t* orig_src = src;

    int gDebug = debug;
    int debug = (elem_sz == 2) ? gDebug : 0;
    debug = false;

    // ================================ one-time initialization

    // ------------------------ read original data size, ndims
    // static const uint32_t len_nbytes = 6;
    // uint64_t one = 1; // make next line legible
    // uint64_t len_mask = (one << (8 * len_nbytes)) - 1;
    // uint64_t orig_len = (*(uint64_t*)src) & len_mask;
    // uint16_t ndims = (*(uint16_t*)(src + len_nbytes));
    // src += 8;
    uint16_t ndims;
    uint32_t orig_len; // TODO make len a u32
    src += read_metadata_simple(src, &ndims, &orig_len);

    bool just_cpy = orig_len < 8 * block_sz * group_sz_blocks;
    // just_cpy = just_cpy || orig_len & (((uint64_t)1) << 47);
    if (just_cpy) { // if data was too small
        memcpy(dest, src, orig_len * elem_sz);
        return orig_len;
    }
    if (ndims == 0) {
        perror("ERROR: Received ndims of 0!");
        return 0;
    }
    if (debug > 0) {
        printf("-------- decompression (orig_len = %lld)\n", (long long)orig_len);
        if (debug > 3) {
            printf("saw compressed data (with possible extra at end):\n");
            // dump_bytes(src, orig_len + 8, ndims * elem_sz);
            dump_bytes(((uint8_t*)src) + ndims, orig_len, ndims * elem_sz);
            // dump_elements(src, orig_len + 4, ndims);
        }
    }
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
    uint32_t group_sz = ndims * group_sz_per_dim;
    uint16_t nstripes = DIV_ROUND_UP(ndims, stripe_sz);
    uint32_t padded_ndims = round_up_to_multiple(ndims, vector_sz);
    uint16_t nvectors = DIV_ROUND_UP(padded_ndims, vector_sz);
    // uint16_t nvectors = padded_ndims / vector_sz + ((padded_ndims % vector_sz) > 0);

    if (debug) {
        printf("group sz, nstripes, padded_ndims, nvectors = %d, %d, %d, %d\n",
            group_sz, nstripes, padded_ndims, nvectors);
    }

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

    // printf("nvectors, padded_ndims = %d, %d\n", nvectors, padded_ndims);

    // ================================ main loop

    uint64_t ngroups = orig_len / group_sz; // if we get an fp error, it's this
    for (uint64_t g = 0; g < ngroups; g++) {
        const uint8_t* header_src = (const uint8_t*)src;
        src = (int_t*)(((int8_t*)src) + total_header_bytes);

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

            if (debug) {
                printf("---- %d.%d (%d -> %d):\n", (int)g, (int)b,
                    (int)(src - orig_src)*elem_sz, (int)(dest - orig_dest)*elem_sz);
            }

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

            // ------------------------ unpack data for each stripe
            // for (uint32_t stripe = 0; stripe < nstripes; stripe++) {
            for (int stripe = nstripes - 1; stripe >= 0; stripe--) {
                if (debug > 1) {
                    printf("%d.%d.%d: ", (int)g, (int)b, (int)stripe);
                    // printf("offset bytes, offset bits, nbits, total_bits = %u, %u, %u, %d\n",
                    //     offset_bytes, offset_bits, nbits, total_bits);
                    printf("src, dest offsets = %d -> %d\n", (int)(src - orig_src), (int)(dest - orig_dest));
                }
                uint32_t offset_bits = stripe_bitoffsets[stripe] & 0x07;
                uint32_t offset_bytes = stripe_bitoffsets[stripe] >> 3;

                uint64_t mask = masks[stripe];
                uint8_t nbits = bitwidths[stripe];
                uint8_t total_bits = nbits + offset_bits;

                const int8_t* inptr = ((const int8_t*)src) + offset_bytes;
                uint8_t* outptr = ((uint8_t*)deltas) + (stripe * stripe_nbytes);
                // const int8_t* inptr = src + offset_bytes;
                // uint8_t* outptr = dest + (stripe * stripe_sz);
                // uint32_t out_row_nbytes = ndims;
                // uint8_t* outptr = ((uint8_t*)deltas) + (stripe * stripe_sz);
                // uint32_t out_row_nbytes = padded_ndims * elem_sz;

                if (debug) {
                    // printf("nbits at idx %d = %d\n", stripe + nstripes * b, nbits);
                    printf("total bits, offset bytes, bits = %u, %u, %u\n", total_bits, offset_bytes, offset_bits);
                    printf("using mask: "); dump_bytes(mask);
                }

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

            // this works when we don't delta code in the compressor
            // for (uint8_t i = 0; i < block_sz; i++) {
            //     uint32_t in_offset = i * padded_ndims;
            //     uint32_t out_offset = i * ndims;
            //     memcpy(dest + out_offset, deltas + in_offset, out_row_nbytes);
            // }
            if (debug) {
                printf("deltas array:\n"); dump_elements(deltas, padded_ndims * block_sz, padded_ndims);
                printf("prev vals:\n"); dump_elements(prev_vals_ar, padded_ndims);
            }

            // zigzag + delta decode
            for (int32_t v = nvectors - 1; v >= 0; v--) {
                uint32_t vstripe_start = v * vector_sz;
                __m256i prev_vals = _mm256_loadu_si256((const __m256i*)
                    (prev_vals_ar + vstripe_start));
                if (debug) { printf("========\ninitial prev vals: "); dump_m256i(prev_vals); }
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

                    _mm256_storeu_si256((__m256i*)(dest + out_offset), vals);
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
        // printf("will now write to dest at offset %lld\n", (uint64_t)(dest - orig_dest));
    } // for each group

    free(headers_tmp);
    free(headers);
    free(data_masks);
    free(stripe_bitwidths);
    free(stripe_bitoffsets);
    free(deltas);
    free(prev_vals_ar);

    // copy over trailing data
    uint32_t remaining_len = (uint32_t)(orig_len - (dest - orig_dest));
    if (debug) { printf("remaining len: %d\n", remaining_len); }
    memcpy(dest, src, remaining_len * elem_sz);

    if (debug > 2) {
        // printf("decompressed data:\n"); dump_bytes(orig_dest, orig_len * elem_sz, ndims * 4);
        printf("decompressed data:\n"); dump_elements(orig_dest, orig_len, ndims);
    }

    // printf("reached end of decomp\n");
    return dest + remaining_len - orig_dest;
}

int64_t decompress_rowmajor_delta_8b(const int8_t* src, uint8_t* dest) {
    return decompress_rowmajor_delta(src, dest);
}
int64_t decompress_rowmajor_delta_16b(const int16_t* src, uint16_t* dest) {
    return decompress_rowmajor_delta(src, dest);
}
