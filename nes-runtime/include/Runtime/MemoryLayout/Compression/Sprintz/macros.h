//
//  macros.h
//  Compress
//
//  Created by DB on 12/4/17.
//  Copyright © 2017 D Blalock. All rights reserved.
//

// #if defined(__GNUC__)
//     #define SPRINTZ_FORCE_INLINE __attribute__((always_inline))
// #elif defined(__clang__)
//     #define SPRINTZ_FORCE_INLINE __attribute__((always_inline))
// #elif defined(_MSC_VER)
//     #define SPRINTZ_FORCE_INLINE __forceinline
// #else
//     // this should still work if compiled as C99 without static keyword; see
//     // https://stackoverflow.com/a/25623448
//     #define SPRINTZ_FORCE_INLINE inline
// #endif

// TODO move impls to headers so we can actually force inline
#define SPRINTZ_FORCE_INLINE

// ------------------------ restrict keyword
// adapted from http://stackoverflow.com/a/5948101/1153180

#if defined(__GNUC__) && ((__GNUC__ > 3) || (__GNUC__ == 3 && __GNUC_MINOR__ >= 1))
    #define RESTRICT __restrict__
#elif defined(__clang__)
    #define RESTRICT __restrict__
    #define PREFETCH_RW_PERSISTENT(ADDR) PREFETCH_RW_WITH_STICKINESS(ADDR, 3)
#else
    #if defined(_MSC_VER) && _MSC_VER >= 1400
        #define RESTRICT
    #else
        #define RESTRICT __restrict
    #endif
#endif
