//
//  nn_utils.hpp
//  Dig
//
//  Created by DB on 2016-9-15
//  Copyright (c) 2016 DB. All rights reserved.
//

#ifndef __NN_UTILS_HPP
#define __NN_UTILS_HPP

#include <assert.h>
#include <stdint.h>
#include <vector>

using std::vector;

template<class T>
constexpr T max_dist() { return std::numeric_limits<T>::max(); }

namespace nn {

using idx_t = int64_t;
using dist_t = float;
namespace {
    static constexpr dist_t kMaxDist = std::numeric_limits<dist_t>::max();
}

// static const dist_t kMaxDist = max_dist<dist_t>();

// ------------------------------------------------ Neighbor
template <typename dist_t=nn::dist_t, typename idx_t=nn::idx_t>
class Neighbor {
    // typedef nn::idx_t idx_t;
    // typedef nn::dist_t dist_t;
public:
    idx_t idx;
    dist_t dist;

    Neighbor() = default;
    Neighbor(const Neighbor& rhs) = default;
    // Neighbor(float d, idx_t i):  idx(i), dist(static_cast<dist_t>(d)) {}
    Neighbor(idx_t i, int d):  idx(i), dist(static_cast<dist_t>(d)) {
        if (dist <= 0) { dist = nn::kMaxDist; }
    }
    Neighbor(idx_t i, float d):  idx(i), dist(static_cast<dist_t>(d)) {
        if (dist <= 0) { dist = nn::kMaxDist; }
    }
    // Neighbor(double d, idx_t i): idx(i), dist(static_cast<dist_t>(d)) {}
    Neighbor(idx_t i, double d): idx(i), dist(static_cast<dist_t>(d)) {
        if (dist <= 0) { dist = nn::kMaxDist; }
    }
};

static const int16_t kInvalidIdx = -1;

// ------------------------------------------------ Structs

template<class RowMatrixT>
struct QueryConfig {
    const RowMatrixT* q;
    vector<dist_t>* d_maxs;
    dist_t d_max;
    float search_frac;
};

// ------------------------------------------------ neighbor munging

template<template<class...> class Container, class NeighborT>
inline void sort_neighbors_ascending_distance(Container<NeighborT>& neighbors) {
    std::sort(std::begin(neighbors), std::end(neighbors),
        [](const NeighborT& a, const NeighborT& b) -> bool
        {
            return a.dist < b.dist;
        }
    );
}

template<template<class...> class Container, class NeighborT>
inline void sort_neighbors_ascending_idx(Container<NeighborT>& neighbors) {
    std::sort(std::begin(neighbors), std::end(neighbors),
        [](const NeighborT& a, const NeighborT& b) -> bool
        {
            return a.idx < b.idx;
        }
    );
}

/** given a sorted collection of the best neighbors found so far, (potentially)
 * inserts a new neighbor into the list such that the sorting is preserved;
 * assumes the neighbors container contains only valid neighbors and is sorted
 *
 * Returns the distance to the last (farthest) neighbor after possible insertion
 */
template<template<class...> class Container, class NeighborT>
inline typename NeighborT::dist_t
maybe_insert_neighbor(Container<NeighborT>& neighbors_bsf,
    NeighborT newNeighbor)
{
    assert(neighbors_bsf.size() > 0);
	size_t len = neighbors_bsf.size();
    size_t i = len - 1;
    auto dist = newNeighbor.dist;

    if (dist < neighbors_bsf[i].dist) {
        neighbors_bsf[i] = newNeighbor;
    }

    while (i > 0 && neighbors_bsf[i-1].dist > dist) {
        // swap new and previous neighbor
        NeighborT tmp = neighbors_bsf[i-1];

        neighbors_bsf[i-1] = neighbors_bsf[i];
        neighbors_bsf[i] = tmp;
        i--;
    }
    return neighbors_bsf[len - 1].dist;
}
template<template<class...> class Container, class NeighborT>
inline typename NeighborT::dist_t
maybe_insert_neighbor(Container<NeighborT>& neighbors_bsf,
    double dist, typename NeighborT::idx_t idx)
{
	return maybe_insert_neighbor(neighbors_bsf, NeighborT{idx, dist});
}

template<template<class...> class Container,
    template<class...> class Container2, class NeighborT>
inline typename NeighborT::dist_t
maybe_insert_neighbors(Container<NeighborT>& neighbors_bsf,
    const Container2<NeighborT>& potential_neighbors)
{
    dist_t d_bsf = nn::kMaxDist;
    for (auto& n : potential_neighbors) {
        d_bsf = maybe_insert_neighbor(neighbors_bsf, n);
    }
    return d_bsf;
}


template<typename T, typename NeighborT>
inline vector<NeighborT> knn_from_dists(const T* dists, size_t len, size_t k) {
    assert(k > 0);
    assert(len > 0);
    k = k < len ? k : len;
    vector<NeighborT> ret(k); // warning: populates it with k 0s
    for (idx_t i = 0; i < k; i++) {
		ret[i] = NeighborT(i, dists[i]);
    }
    sort_neighbors_ascending_distance(ret);
    for (idx_t i = k; i < len; i++) {
        maybe_insert_neighbor(ret, dists[i], i);
    }
    return ret;
}

template<typename T, typename R, typename NeighborT>
inline vector<NeighborT> neighbors_in_radius(const T* dists, size_t len,
    R threshold)
{
    vector<NeighborT> neighbors;
    for (idx_t i = 0; i < len; i++) {
        auto dist = dists[i];
        if (dists[i] <= threshold) {
			neighbors.emplace_back(i, dist);
        }
    }
    return neighbors;
}

template<typename T, typename R, typename NeighborT>
inline vector<NeighborT> neighbors_outside_radius(const T* dists,
    size_t len, R radius)
{
    vector<NeighborT> neighbors;
    for (idx_t i = 0; i < len; i++) {
        auto dist = dists[i];
        if (dists[i] >= radius) {
            neighbors.emplace_back(i, dist);
        }
    }
    return neighbors;
}

// inline vector<vector<idx_t> > idxs_from_nested_neighbors(
//     const vector<vector<Neighbor> >& neighbors)
// {
//     return map([](const vector<Neighbor>& inner_neighbors) -> std::vector<idx_t> {
//         return map([](const Neighbor& n) -> idx_t {
//             return n.idx;
//         }, inner_neighbors);
//     }, neighbors);
// }

} // namespace nn
#endif // __NN_UTILS_HPP
