//
//  transpose.h
//  Compress
//
//  Created by DB on 10/14/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

#ifndef transpose_h
#define transpose_h

#include <stdint.h>
#include "immintrin.h" // for pext, pdep

#include "debug_utils.hpp" // TODO rm


/* 2x8 (rowmajor) -> 8x2 (rowmajor) transpose.
 *
 * This transforms:
 *
 * a0 a1 a2 a3 a4 a5 a6 a7
 * b0 b1 b2 b3 b4 b5 b6 b7
 * c0 c1 c2 c3 c4 c5 c6 c7
 * d0 d1 d2 d3 d4 d5 d6 d7
 *
 * to
 *
 * a0 b0
 * a1 b1
 * a2 b2
 * a3 b3
 * a4 b4
 * a5 b5
 * a6 b6
 * a7 b7
 * c0 d0
 * c1 d1
 * c2 d2
 * c3 d3
 * c4 d4
 * c5 d5
 * c6 d6
 * c7 d7
 */
static inline void transpose_2x8_8b(const uint8_t* src, uint8_t* dest) {
    static const int a = 0;
    static const int b = 8;
    static const int c = 0;
    static const int d = 8;
    static const __m256i idxs = _mm256_setr_epi8(
        // first 16B of both input and output
        a+0, b+0,
        a+1, b+1,
        a+2, b+2,
        a+3, b+3,
        a+4, b+4,
        a+5, b+5,
        a+6, b+6,
        a+7, b+7,
        // second 16B of both input and output
        c+0, d+0,
        c+1, d+1,
        c+2, d+2,
        c+3, d+3,
        c+4, d+4,
        c+5, d+5,
        c+6, d+6,
        c+7, d+7);

    __m256i vsrc = _mm256_loadu_si256((const __m256i*)src);
    __m256i shuffled = _mm256_shuffle_epi8(vsrc, idxs);
    _mm256_storeu_si256((__m256i*)dest, shuffled);
}

/* 2x8 (rowmajor) -> 8x2 (rowmajor) transpose.
 *
 * This transforms:
 *
 * a0 a1 a2 a3 a4 a5 a6 a7
 * b0 b1 b2 b3 b4 b5 b6 b7
 *
 * to
 *
 * a0 b0
 * a1 b1
 * a2 b2
 * a3 b3
 * a4 b4
 * a5 b5
 * a6 b6
 * a7 b7
 */
static inline void transpose_2x8_16b(const uint16_t* src, uint16_t* dest) {
    static const int a = 0;
    static const int b = 0;
    static const __m256i idxs0 = _mm256_setr_epi8(
        // first 16B (8 elements) of both input and output
        a+0, a+1, 0xff, 0xff,
        a+2, a+3, 0xff, 0xff,
        a+4, a+5, 0xff, 0xff,
        a+6, a+7, 0xff, 0xff,
        // second 16B (8 elements) of both input and output
        0xff, 0xff, b+8,  b+9,
        0xff, 0xff, b+10, b+11,
        0xff, 0xff, b+12, b+13,
        0xff, 0xff, b+14, b+15
        );
    static const __m256i idxs1 = _mm256_setr_epi8(
        // first 16B (8 elements) of both input and output
        0xff, 0xff, b+0, b+1,
        0xff, 0xff, b+2, b+3,
        0xff, 0xff, b+4, b+5,
        0xff, 0xff, b+6, b+7,
        // second 16B (8 elements) of both input and output
        a+8,  a+9,  0xff, 0xff,
        a+10, a+11, 0xff, 0xff,
        a+12, a+13, 0xff, 0xff,
        a+14, a+15, 0xff, 0xff
        );

    __m256i vsrc = _mm256_loadu_si256((const __m256i*)src);
    __m256i swapped128_vsrc = _mm256_permute2x128_si256(vsrc, vsrc, 0x01);

    __m256i shuffled0 = _mm256_shuffle_epi8(vsrc, idxs0);
    __m256i shuffled1 = _mm256_shuffle_epi8(swapped128_vsrc, idxs1);
    __m256i blended = _mm256_or_si256(shuffled0, shuffled1);

    _mm256_storeu_si256((__m256i*)dest, blended);
}

/* 3x8 (rowmajor) -> 8x3 (rowmajor) transpose.
 *
 * This transforms:
 *
 * a0 a1 a2 a3 a4 a5 a6 a7
 * b0 b1 b2 b3 b4 b5 b6 b7
 * c0 c1 c2 c3 c4 c5 c6 c7
 *
 * to
 *
 * a0 b0 c0
 * a1 b1 c1
 * a2 b2 c2
 * a3 b3 c3
 * a4 b4 c4
 * a5 b5 c5
 * a6 b6 c6
 * a7 b7 c7
 * 0 0 0 0 0 0 0 0
 */
static inline void transpose_3x8_8b(const uint8_t* src, uint8_t* dest) {
    static const int a = 0;
    static const int b = 8;
    static const int c = 16 - 16;

    // shuffle idxs for first 2 rows of input (broadcast to both lanes)
    static const __m256i idxs0 = _mm256_setr_epi8(
        // low lane of output (reads from first 2 rows of input)
        a+0, b+0, 0xff,
        a+1, b+1, 0xff,
        a+2, b+2, 0xff,
        a+3, b+3, 0xff,
        a+4, b+4, 0xff,
        a+5,
        // high lane of output (also reads from first 2 rows of input)
             b+5, 0xff,
        a+6, b+6, 0xff,
        a+7, b+7, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff);

    static const __m256i idxs1 = _mm256_setr_epi8(
        // low lane of output (reads from row 2 of input)
        0xff, 0xff, c+0,
        0xff, 0xff, c+1,
        0xff, 0xff, c+2,
        0xff, 0xff, c+3,
        0xff, 0xff, c+4,
        0xff,
        // high lane of output (also reads row 2 of input)
              0xff, c+5,
        0xff, 0xff, c+6,
        0xff, 0xff, c+7,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff);

    __m256i vsrc0 = _mm256_loadu_si256((const __m256i*)src);
    __m256i first_two_rows = _mm256_permute2x128_si256(vsrc0, vsrc0, 0x00);
    __m256i third_row = _mm256_set1_epi64x(*(uint64_t*)(src + 16));

    __m256i shuffled0 = _mm256_shuffle_epi8(first_two_rows, idxs0);
    __m256i shuffled1 = _mm256_shuffle_epi8(third_row, idxs1);
    __m256i blended = _mm256_or_si256(shuffled0, shuffled1);

    _mm256_storeu_si256((__m256i*)dest, blended);
}



// XXX this function doesn't work, and is enough of a pain that I'm not
// going to try to get it working unless it becomes clear that we need it

/* 3x8 (rowmajor) -> 8x3 (rowmajor) transpose.
 *
 * This transforms:
 *
 * a0 a1 a2 a3 a4 a5 a6 a7
 * b0 b1 b2 b3 b4 b5 b6 b7
 * c0 c1 c2 c3 c4 c5 c6 c7
 *
 * to
 *
 * a0 b0 c0
 * a1 b1 c1
 * a2 b2 c2
 * a3 b3 c3
 * a4 b4 c4
 * a5 b5 c5
 * a6 b6 c6
 * a7 b7 c7
 * 0 0 0 0 0 0 0 0
 */
static inline void transpose_3x8_16b(const uint16_t* src, uint16_t* dest) {
    // int a = 0, b = 0;//, c = 0;
    // shuffle idxs for mat (output rows 0-1)
    //  a0-3  b0-3
    //  a4-7  b4-7
    int a0 = 0, a1 = -8;
    int b = 8;
    static const __m256i idxs01 = _mm256_setr_epi8(
        // low 16B of output
        a0+0,a0+1, b+0,b+1, 0xff,0xff,
        a0+2,a0+3, b+2,b+3, 0xff,0xff,
        a0+4,a0+5, b+4,b+5,
        // mid 16B of output
                              0xff,0xff,
        0xff,0xff, 0xff,0xff, 0xff,0xff,
        a1+8, a1+9, b+8,b+9,  0xff,0xff,
        a1+10,a1+11
    );
    // shuffle idxs for mat (lhs is what we loaded from mem to get rhs):
    // b2-7 c0-1
    // c2-7 ??
    int c0 = 12, c1 = -4;
    static const __m256i idxs_split_c = _mm256_setr_epi8(
        // low 16B of output
        0xff,0xff, 0xff,0xff, c0+0,c0+1,
        0xff,0xff, 0xff,0xff, c0+2,c0+3,
        0xff,0xff, 0xff,0xff,
        // mid 16B of output
                              c1+4,c1+5,
        0xff,0xff, 0xff,0xff, c1+6,c1+7,
        0xff,0xff, 0xff,0xff, c1+8,c1+9,
        0xff,0xff
    );
    // shuffle idxs for mat (output rows 1-2):
    //  b4-7  c4-7
    //  b0-3  c0-3
    // b = 0; c = 8;
    // static const __m256i idxs12 = _mm256_setr_epi8(
    //     // mid 16B of output
    //                           c+4,c+5,
    //     0xff,0xff, 0xff,0xff, c+6,c+7,
    //     0xff,0xff, 0xff,0xff, c+8,c+9,
    //     0xff,0xff,
    //     // high 16B of output
    //                b+10,b+11, c+10,c+11,
    //     0xff,0xff, b+12,b+13, c+12,c+13,
    //     0xff,0xff, b+14,b+15, c+14,c+15
    // );

    __m256i vsrc01 = _mm256_loadu_si256((const __m256i*)src);
    // __m256i vsrc12 = _mm256_loadu_si256((const __m256i*)(src + 8));
    __m256i a_and_b = _mm256_permute4x64_epi64(vsrc01, _MM_SHUFFLE(3, 1, 2, 0));
    __m256i split_c = _mm256_loadu_si256((const __m256i*)(src + 10));

    // construct the first 32B of the output
    __m256i shuffled01 = _mm256_shuffle_epi8(a_and_b, idxs01);
    __m256i shuffled_c = _mm256_shuffle_epi8(split_c, idxs_split_c);
    __m256i first_32B = _mm256_or_si256(shuffled01, shuffled_c);
    _mm256_storeu_si256((__m256i*)dest, first_32B);

    printf("input first 2 rows:\n"); dump_m256i<uint16_t>(vsrc01);
    printf("a and b:\n"); dump_m256i<uint16_t>(a_and_b);
    printf("split c:\n"); dump_m256i<uint16_t>(split_c);
    printf("shuffled01:\n"); dump_m256i<uint16_t>(shuffled01);

    // construct the remaining 16B of the output

    // __m256i shuffled12 = _mm256_shuffle_epi8(b_and_c, idxs12);
    // __m256i blended = _mm256_or_si256(shuffled0, shuffled1);


}

/* 4x8 (rowmajor) -> 8x4 (rowmajor) transpose.
 *
 * This transforms:
 *
 * a0 a1 a2 a3 a4 a5 a6 a7
 * b0 b1 b2 b3 b4 b5 b6 b7
 * c0 c1 c2 c3 c4 c5 c6 c7
 * d0 d1 d2 d3 d4 d5 d6 d7
 *
 * to
 *
 * a0 b0 c0 d0
 * a1 b1 c1 d1
 * a2 b2 c2 d2
 * a3 b3 c3 d3
 * a4 b4 c4 d4
 * a5 b5 c5 d5
 * a6 b6 c6 d6
 * a7 b7 c7 d7
 */
static inline void transpose_4x8_8b(const uint8_t* src, uint8_t* dest) {
    static const int a = 0;
    static const int b = 8;
    static const int c = 16 - 16;
    static const int d = 24 - 16;

    // shuffle idxs for first 2 rows of input (broadcast to both lanes)
    static const __m256i idxs0 = _mm256_setr_epi8(
        // low lane of output (reads from first 2 rows of input)
        a+0, b+0, 0xff, 0xff,
        a+1, b+1, 0xff, 0xff,
        a+2, b+2, 0xff, 0xff,
        a+3, b+3, 0xff, 0xff,
        // high lane of output (also reads from last 2 rows of input)
        0xff, 0xff, c+4, d+4,
        0xff, 0xff, c+5, d+5,
        0xff, 0xff, c+6, d+6,
        0xff, 0xff, c+7, d+7);

    // shuffle idxs for last 2 rows of input (broadcast to both lanes)
    static const __m256i idxs1 = _mm256_setr_epi8(
        // low lane of output (reads from last two rows of input)
        0xff, 0xff, c+0, d+0,
        0xff, 0xff, c+1, d+1,
        0xff, 0xff, c+2, d+2,
        0xff, 0xff, c+3, d+3,
        // high lane of output (reads from first two rows of input)
        a+4, b+4, 0xff, 0xff,
        a+5, b+5, 0xff, 0xff,
        a+6, b+6, 0xff, 0xff,
        a+7, b+7, 0xff, 0xff);

    __m256i vsrc = _mm256_loadu_si256((const __m256i*)src);
    __m256i swapped128_vsrc = _mm256_permute2x128_si256(vsrc, vsrc, 0x01);

    __m256i shuffled0 = _mm256_shuffle_epi8(vsrc, idxs0);
    __m256i shuffled1 = _mm256_shuffle_epi8(swapped128_vsrc, idxs1);
    __m256i blended = _mm256_or_si256(shuffled0, shuffled1);

    _mm256_storeu_si256((__m256i*)dest, blended);
}


#endif /* transpose_h */
