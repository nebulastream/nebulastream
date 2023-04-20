//
//  traits.hpp
//  Compress
//
//  Created by DB on 2/13/18.
//  Copyright © 2018 D Blalock. All rights reserved.
//

#ifndef traits_h
#define traits_h

#include <array>

#include "immintrin.h" // XXX don't assume AVX2

// template<class vec_t> struct VecBox {};
// template<> struct VecBox<__m256i> {
//     using data_type = __m256i;
//     VecBox(const data_type& vec): v(vec) {}
//     __m256i v;
// };

// typedef struct Packet32x8i {
//     using scalar_t = int8_t;
//     using vec_t =
// // } Packet32x8i;
// struct Packet32x8i;
// struct Packet32x8u;
// struct Packet16x16i;
// struct Packet16x16u;

struct CpuCtx {};
template<int Nbytes, typename ScalarT, typename ContextT=CpuCtx>
struct Packet;

template<typename VecT> struct vector_traits {};
template<> struct vector_traits<__m256i> {
    static const bool is_integral = true;
    static const int size = 32;
};

// XXX don't assume AVX2
// TODO put this in a different file
template<typename DataT> struct scalar_traits {};
template<> struct scalar_traits<int8_t> {
    using scalar_type = int8_t; // identity
    using vector_type = __m256i;
    using packet_type = Packet<vector_traits<vector_type>::size, scalar_type>;
    static const int size = sizeof(scalar_type);
    static const bool is_integral = true;
};
template<> struct scalar_traits<uint8_t> {
    using scalar_type = uint8_t; // identity
    using vector_type = __m256i;
    using packet_type = Packet<vector_traits<vector_type>::size, scalar_type>;
    static const int size = sizeof(scalar_type);
    static const bool is_integral = true;
};
template<> struct scalar_traits<int16_t> {
    using scalar_type = int16_t; // identity
    using vector_type = __m256i;
    using packet_type = Packet<vector_traits<vector_type>::size, scalar_type>;
    static const int size = sizeof(scalar_type);
    static const bool is_integral = true;
};
template<> struct scalar_traits<uint16_t> {
    using scalar_type = uint16_t; // identity
    using vector_type = __m256i;
    using packet_type = Packet<vector_traits<vector_type>::size, scalar_type>;
    static const int size = sizeof(scalar_type);
    static const bool is_integral = true;
};
template<> struct scalar_traits<int32_t> {
    using scalar_type = int32_t; // identity
    using vector_type = __m256i;
    using packet_type = Packet<vector_traits<vector_type>::size, scalar_type>;
    static const int size = sizeof(scalar_type);
    static const bool is_integral = true;
};
template<> struct scalar_traits<uint32_t> {
    using scalar_type = uint32_t; // identity
    using vector_type = __m256i;
    using packet_type = Packet<vector_traits<vector_type>::size, scalar_type>;
    static const int size = sizeof(scalar_type);
    static const bool is_integral = true;
};


template<int Nbytes, typename ScalarT, typename ContextT> struct Packet {
    using self_t = Packet<Nbytes, ScalarT, ContextT>;
    using scalar_t = ScalarT;
    using vector_t = typename scalar_traits<scalar_t>::vector_type;
    Packet(const vector_t& v): vec(v) {}
    // Packet(const Packet& v) = default;
    // Packet() = default;

    // XXX ths is a total hack that assumes AVX2 ints; if we try to use the
    // default constructors, it ends up trapping
    Packet(const Packet& v) { vec = v.vec; }
    Packet() { vec = _mm256_setzero_si256(); }
    void operator=(const Packet& other) { vec = other.vec; }
    void operator=(vector_t v) { vec = v; }

    vector_t vec;

    // template<class OtherT>
    // self_t& operator+=(const OtherT& rhs) {

    // }
};



// typedef struct Packet32x8i {
//     using scalar_t = int8_t;
//     using vector_t = typename scalar_traits<scalar_t>::vector_type;
//     Packet32x8i(const vector_t& v): vec(v) {}
//     vector_t vec;
// } Packet32x8i;
// typedef struct Packet32x8u {
//     using scalar_t = int8_t;
//     using vector_t = typename scalar_traits<scalar_t>::vector_type;
//     Packet32x8u(const vector_t& v): vec(v) {}
//     vector_t vec;
// } Packet32x8u;
// typedef struct Packet16x16i {} Packet16x16i;
// typedef struct Packet16x16u {} Packet16x16u;

#endif /* traits_h */
