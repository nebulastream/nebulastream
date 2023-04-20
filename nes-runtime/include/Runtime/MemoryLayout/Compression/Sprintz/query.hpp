//
//  query.hpp
//  Compress
//
//  Created by DB on 2/13/18.
//  Copyright © 2018 D Blalock. All rights reserved.
//

#ifndef query_h
#define query_h

// namespace query {

#include <memory>
#include <vector>

#include "traits.hpp"
#include "util.h" // div_round_up

// namespace query {

namespace QueryTypes {
    enum Operation { NOOP = 0, REDUCE_MAX, REDUCE_SUM };
}

typedef struct QueryParams {
    QueryTypes::Operation op;
    bool materialize; /// whether to materialize the decompressed data
} QueryParams;

// }

// template<class vec_t> struct VecBox {};
// template<> struct VecBox<__m256i> {
//     using data_type = __m256i;
//     VecBox(const data_type& vec): v(vec) {}
//     __m256i v;
// };

// template<class DataT>
// __m256i max(VecBox<__m256i> x, VecBox<__m256i> y) {
//     return _mm256_max_
// }

// static inline auto max(Packet<32, int8_t, CpuCtx> x,
//     Packet<32, int8_t, CpuCtx> y) -> decltype(x)
// {
//     return _mm256_max_epi8(x.vec, y.vec);
// }
static inline auto max(Packet<32, uint8_t, CpuCtx> x,
    Packet<32, uint8_t, CpuCtx> y) -> decltype(x)
{
    return _mm256_max_epu8(x.vec, y.vec);
}
static inline auto max(Packet<32, uint16_t, CpuCtx> x,
                       Packet<32, uint16_t, CpuCtx> y) -> decltype(x)
{
    return _mm256_max_epu16(x.vec, y.vec);
}

static inline auto add(Packet<32, int32_t, CpuCtx> x,
    Packet<32, int32_t, CpuCtx> y) -> decltype(x)
{
    return _mm256_add_epi32(x.vec, y.vec);
}

// template<template<class...> Tparams0, template<class...> Tparams1>
// Packet<Tparams0> operator+=(const Packet<Tparams0>& x, Packet<Tparams1> y) {
// template<class PacketT0, class PacketT1>
// template<int Sz0, class DataT0, class Ctx0, int Sz1, class DataT1, class Ctx1>
// Packet<Sz0, DataT0, Ctx0>& operator+=(x,
//     Packet<Sz1, DataT1, Ctx1> y) -> decltype(x)
// {
//     return add(x, y);
// }

static inline void accumulate(
    Packet<32, int8_t, CpuCtx> x,
    Packet<32, int32_t, CpuCtx>& accum0, Packet<32, int32_t, CpuCtx>& accum1,
    Packet<32, int32_t, CpuCtx>& accum2, Packet<32, int32_t, CpuCtx>& accum3,
    int32_t nrepeats=1)
{
    // printf("called accumulate!\n");
    auto x_low = _mm256_extracti128_si256(x.vec, 0);
    auto x_high = _mm256_extracti128_si256(x.vec, 1);
    auto x0 = _mm256_cvtepi8_epi32(x_low);
    auto x1 = _mm256_cvtepi8_epi32(_mm_slli_si128(x_low, 8));
    auto x2 = _mm256_cvtepi8_epi32(x_high);
    auto x3 = _mm256_cvtepi8_epi32(_mm_slli_si128(x_high, 8));
    // using PacketT = decltype(accum0);
    // accum0 += PacketT(x0);
//    accum1 += x1;
//    accum2 += x2;
//    accum3 += x3;
    if (nrepeats > 1) {
        auto vnrepeats = _mm256_set1_epi32(nrepeats);
        x0 = _mm256_mullo_epi32(x0, vnrepeats);
        x1 = _mm256_mullo_epi32(x1, vnrepeats);
        x2 = _mm256_mullo_epi32(x2, vnrepeats);
        x3 = _mm256_mullo_epi32(x3, vnrepeats);
    }
    // printf("about to write to accumulators\n");
    accum0.vec = _mm256_add_epi32(accum0.vec, x0);
    // printf("wrote to one of the accumulators!\n");
    accum1.vec = _mm256_add_epi32(accum1.vec, x1);
    // printf("wrote to half the accumulators!\n");
    accum2.vec = _mm256_add_epi32(accum2.vec, x2);
    // printf("wrote to 3/4 of the accumulators!\n");
    accum3.vec = _mm256_add_epi32(accum3.vec, x3);
    // printf("finished accumulate!\n");
}

static inline void accumulate(
    Packet<32, uint8_t, CpuCtx> x,
    Packet<32, uint32_t, CpuCtx>& accum0, Packet<32, uint32_t, CpuCtx>& accum1,
    Packet<32, uint32_t, CpuCtx>& accum2, Packet<32, uint32_t, CpuCtx>& accum3,
    uint32_t nrepeats=1)
{
    auto x_low = _mm256_extracti128_si256(x.vec, 0);
    auto x_high = _mm256_extracti128_si256(x.vec, 1);
    auto x0 = _mm256_cvtepu8_epi32(x_low);
    auto x1 = _mm256_cvtepu8_epi32(_mm_slli_si128(x_low, 8));
    auto x2 = _mm256_cvtepu8_epi32(x_high);
    auto x3 = _mm256_cvtepu8_epi32(_mm_slli_si128(x_high, 8));
    if (nrepeats > 1) {
        auto vnrepeats = _mm256_set1_epi32(nrepeats);
        x0 = _mm256_mullo_epi32(x0, vnrepeats);
        x1 = _mm256_mullo_epi32(x1, vnrepeats);
        x2 = _mm256_mullo_epi32(x2, vnrepeats);
        x3 = _mm256_mullo_epi32(x3, vnrepeats);
    }
    accum0.vec = _mm256_add_epi32(accum0.vec, x0);
    accum1.vec = _mm256_add_epi32(accum1.vec, x1);
    accum2.vec = _mm256_add_epi32(accum2.vec, x2);
    accum3.vec = _mm256_add_epi32(accum3.vec, x3);
}

static inline void accumulate(
    Packet<32, uint16_t, CpuCtx> x,
    Packet<32, uint32_t, CpuCtx>& accum0, Packet<32, uint32_t, CpuCtx>& accum1,
    Packet<32, uint32_t, CpuCtx>& accum2, Packet<32, uint32_t, CpuCtx>& accum3,
    uint32_t nrepeats=1)
{
    auto x_low = _mm256_extracti128_si256(x.vec, 0);
    auto x_high = _mm256_extracti128_si256(x.vec, 1);
    auto x0 = _mm256_cvtepi16_epi32(x_low);
    auto x1 = _mm256_cvtepi16_epi32(x_high);
    if (nrepeats > 1) {
        auto vnrepeats = _mm256_set1_epi32(nrepeats);
        x0 = _mm256_mullo_epi32(x0, vnrepeats);
        x1 = _mm256_mullo_epi32(x1, vnrepeats);
    }
    // auto x2 = _mm256_cvtepu8_epi32(x_high);
    // auto x3 = _mm256_cvtepu8_epi32(_mm_slli_si128(x_high, 8));
    accum0.vec = _mm256_add_epi32(accum0.vec, x0);
    accum1.vec = _mm256_add_epi32(accum1.vec, x1);
    // accum2.vec = _mm256_add_epi32(accum2.vec, x2);
    // accum3.vec = _mm256_add_epi32(accum3.vec, x3);
}

// template<typename DataT>
// class VectorizedQuery {
//     using vec_t = typename data_traits<DataT>::vector_type;
//     using state_t = std::vector<vec_t>;
//     static const int scalar_sz = data_traits<DataT>::size;
//     static const int vec_sz = vector_traits<vec_t>::size;
//     static const int elems_per_vec = vec_sz / scalar_sz;
//     // shouldn't be possible unless I messed up in traits
//     static_assert(vec_sz % scalar_sz == 0, "Invalid scalar-vector pairing!");
// };

// #define _INSERT_VECTOR_TYPEDEFS_AND_CONSTS                                    \
//     using vec_t = typename scalar_traits<DataT>::vector_type;                 \
//     using state_t = std::vector<vec_t>;                                       \
//     static const int scalar_sz = scalar_traits<DataT>::size;                  \
//     static const int vec_sz = vector_traits<vec_t>::size;                     \
//     static const int elems_per_vec = vec_sz / scalar_sz;                      \
//     static_assert(vec_sz % scalar_sz == 0, "Invalid scalar-vector pairing!");

template<typename DataT>
class NoopQuery {
public:
    using vec_t = typename scalar_traits<DataT>::vector_type;
    using state_t = std::vector<DataT>;
    explicit NoopQuery(int64_t ndims) {}
    void operator()(uint32_t vstripe, const vec_t& prev_vals,
        const vec_t& vals, uint32_t nrepeats=1) { }
    state_t result() { return state_t{}; }
};

// class MaxQuery: public VectorizedQuery<DataT> {
template<typename DataT>
class MaxQuery {
public:
    // _INSERT_VECTOR_TYPEDEFS_AND_CONSTS
    using vec_t = typename scalar_traits<DataT>::vector_type;
//    using state_t = std::vector<vec_t>;
    using packet_t = typename scalar_traits<DataT>::packet_type;
    // using state_t = std::vector<packet_t>;
    // using state_t = std::unique_ptr<packet_t[]>;
    using state_t = aligned_array<packet_t>;
    static const int scalar_sz = scalar_traits<DataT>::size;
    static const int vec_sz = vector_traits<vec_t>::size;
    static const int elems_per_vec = vec_sz / scalar_sz;
    static_assert(vec_sz % scalar_sz == 0, "Invalid scalar-vector pairing!");

    // explicit MaxQuery(int64_t ndims):
    //     state(DIV_ROUND_UP(ndims, elems_per_vec)) {}
    explicit MaxQuery(int64_t ndims) {
        int needed_npackets = (int)div_round_up(ndims, elems_per_vec) + 3;
        // printf("MaxQuery: need %d packets for %d dims\n", needed_npackets, (int)ndims);
        bool zero = true; // XXX max could be less than zero
        state = aligned_array<packet_t>(needed_npackets, true);
        // printf("MaxQuery: sizeof(packet_t): %lu\n", sizeof(packet_t));
        // printf("MaxQuery: initial state vector size: %lu\n", state.size());
        // state.resize(needed_npackets);
        // state.reserve(needed_npackets);
        // printf("MaxQuery: resized state vector size, capacity: %lu, %lu\n", state.size(), state.capacity());
        // printf("MaxQuery: resized state vector size: %lu\n", state.size());
    }

    // ~MaxQuery() {
    //     printf("about to delete MaxQuery\n");
    //     // delete state;
    // }

    void operator()(uint32_t vstripe, const vec_t& prev_vals,
        const vec_t& vals, uint32_t nrepeats=1)
    {
        // state[vstripe] = max(state[vstripe], vals);
        state[0] = max(state[vstripe], vals); // TODO rm
    }
    const state_t& result() { return state; }

private:
    state_t state;
};

template<typename DataT>
class SumQuery {
public:
    using vec_t = typename scalar_traits<DataT>::vector_type;
    using accumulator_t = int32_t; // XXX handle other types
    using packet_t = typename scalar_traits<accumulator_t>::packet_type;
    // using state_t = std::vector<packet_t>;
    using state_t = aligned_array<packet_t>;
    // using state_t = std::unique_ptr<packet_t[]>;
//    static const int scalar_sz = MAX(sizeof(DataT), 4);  // at least 32b accum
    static const int accum_sz = sizeof(accumulator_t);  // at least 32b accum
    static const int vec_sz = vector_traits<vec_t>::size;
    static const int elems_per_vec = vec_sz / accum_sz;

    // Add up to 3 extra vecs so accumulate() can read past the end of
    // the actual state in 16b case
    // explicit SumQuery(int64_t ndims):
    //     state(div_round_up(ndims, elems_per_vec) + 3) {}
    explicit SumQuery(int64_t ndims) {
        int needed_npackets = (int)div_round_up(ndims, elems_per_vec) + 3;
        // int needed_npackets = DIV_ROUND_UP(ndims, elems_per_vec) + 20; // TODO rm
        // printf("SumQuery: ndims = %d, needed_npackets = %d\n", (int)ndims, needed_npackets);
        // state = std::unique_ptr<packet_t[]>(
        //     (packet_t*)_aligned_alloc(sizeof(packet_t), needed_npackets));
        state = aligned_array<packet_t>(needed_npackets);
        // state.reserve(needed_npackets);



        // SELF: pick up here by using uniq_ptr for state



        // state.resize(needed_npackets);
        // printf("SumQuery: about to push back some packets...\n");
        // for (int i = 0; i < needed_npackets; i++) {
        //     state.push_back(packet_t());
        //     // state[i].vec = _mm256_setzero_si256();
        // }
        // printf("SumQuery: initialized packets\n");
    }
        // This segfaults:
        // state(round_up_to_multiple(div_round_up(ndims, elems_per_vec), 4)) {}

    void operator()(uint32_t vstripe, const vec_t& prev_vals,
        const vec_t& vals, uint32_t nrepeats=1)
    {
        uint32_t start_idx = vstripe * 4 / sizeof(DataT);  // XXX 64bit DataT
        // printf("SumQuery: vstripe = %d, start_idx = %d, nrepeats = %d\n",
        //     (int)vstripe, (int)start_idx, (int)nrepeats);
        accumulate(vals, state[start_idx], state[start_idx + 1],
            state[start_idx + 2], state[start_idx + 3], nrepeats);
    }
    const state_t& result() { return state; }

private:
    state_t state;
};

#undef _INSERT_VECTOR_TYPEDEFS_AND_CONSTS

// } // namespace query
#endif /* query_h */
