
//  nn_search.hpp
//  Dig
//
//  Created by DB on 2016-9-15
//  Copyright (c) 2016 DB. All rights reserved.
//

#ifndef __NN_SEARCH_HPP
#define __NN_SEARCH_HPP

// #include "array_utils.hpp" // just for min()
#include "nn_utils.hpp"
#include "euclidean.hpp"

namespace nn {

namespace internal {

    //
    // any of these work to define a test functor
    //
	// auto hasRows = is_valid([](auto&& x) -> decltype(x.rows()) { });
	// auto hasRows = TEST_FOR_METHOD(rows());
    auto hasRows = CREATE_TEST(x, x.rows());
	// auto hasRows = CREATE_TEST_X(x.rows());

    // this func and the one below use two different macros to conditionally
    // allow substitution based on whether X.rows() will compile; we could
    // also put one of these in the template signature, but not both, because
    // then the funcs would only differ in optional template args and the
    // compiler would complain that the second one was redefining default
    // args / redefining the whole function.
    template<class RowMatrixT>
    auto _num_rows(const RowMatrixT& X, int64_t nrows_hint)
        -> ENABLE_IF(PASSES_TEST(X, hasRows), idx_t)
    {
        if (nrows_hint > 0) {
            return nrows_hint;
        }
        return X.rows();
    }

    template<class RowMatrixT>
    auto _num_rows(const RowMatrixT& X, int64_t nrows_hint)
        -> ENABLE_IF(!TYPE_PASSES_TEST(RowMatrixT, hasRows), idx_t)
    {
        assert(nrows_hint > 0);
        return static_cast<idx_t>(nrows_hint);
    }

    // either return vector of neighbors or just indices
    template<class Ret> struct emplace_neighbor {
        template<class Dist, class Idx>
        void operator()(std::vector<Ret>& vect, Dist d, Idx idx) {
            vect.emplace_back(static_cast<Ret>(idx));
        }
    };
    template<> struct emplace_neighbor<Neighbor> {
        template<class Dist, class Idx>
        void operator()(std::vector<Neighbor>& vect, Dist d, Idx idx) {
			vect.emplace_back(idx, d);
        }
    };
}


// ------------------------------------------------ naive (vectorized) search

namespace simple {

template<class MatrixT, class VectorT, class DistT>
inline vector<Neighbor> radius(const MatrixT& X, const VectorT& q,
    DistT radius_sq, idx_t nrows=-1)
{
    vector<Neighbor> trueKnn;
    // auto numrows = internal::_num_rows<RowMatrixT>{}(X, nrows);
    for (int32_t i = 0; i < internal::_num_rows(X, nrows); i++) {
        auto d = dist::simple::dist_sq(X.row(i), q);
        if (d < radius_sq) {
			trueKnn.emplace_back(i, d);
        }
    }
    return trueKnn;
}

template<class MatrixT, class VectorT>
inline Neighbor onenn(const MatrixT& X, const VectorT& q, idx_t nrows=-1) {
    Neighbor trueNN;
    dist_t d_bsf = kMaxDist;
    for (int32_t i = 0; i < internal::_num_rows(X, nrows); i++) {
		auto dist = dist::simple::dist_sq(X.row(i), q);
        if (dist < d_bsf) {
            d_bsf = dist;
			trueNN = Neighbor{i, dist};
        }
    }
    return trueNN;
}

template<class MatrixT, class VectorT>
inline vector<Neighbor> knn(const MatrixT& X, const VectorT& q, int k,
    idx_t nrows=-1)
{
    assert(k > 0);
    // assert(k <= internal::_num_rows(X, nrows));
	auto num_rows = internal::_num_rows(X, nrows);

    vector<Neighbor> trueKnn;
	for (int32_t i = 0; i < ar::min(k, num_rows); i++) {
        auto d = dist::simple::dist_sq(X.row(i), q);
		trueKnn.emplace_back(i, d);
    }
    sort_neighbors_ascending_distance(trueKnn);

    for (int32_t i = k; i < num_rows; i++) {
		auto d = dist::simple::dist_sq(X.row(i), q);
        maybe_insert_neighbor(trueKnn, d, i);
    }
    return trueKnn;
}

} // namespace simple

// ------------------------------------------------ brute force search

namespace brute {

    // ================================ single query, with + without row norms

    // ------------------------ radius
    template<class RowMatrixT, class VectorT>
    inline vector<Neighbor> radius(const RowMatrixT& X, const VectorT& query,
                                         float radius_sq)
    {
        auto dists = dist::squared_dists_to_vector(X, query);
        return neighbors_in_radius(dists.data(), dists.size(), radius_sq);
    }
    template<class RowMatrixT, class VectorT, class ColVectorT>
    inline vector<Neighbor> radius(const RowMatrixT& X, const VectorT& query,
        float radius_sq, const ColVectorT& rowSquaredNorms)
    {
        auto dists = dist::squared_dists_to_vector(X, query, rowSquaredNorms);
        return neighbors_in_radius(dists.data(), dists.size(), radius_sq);
    }

    // ------------------------ onenn
    template<class RowMatrixT, class VectorT>
    inline Neighbor onenn(const RowMatrixT& X, const VectorT& query) {
        typename RowMatrixT::Index idx;
        static_assert(VectorT::IsRowMajor, "query must be row-major");
		auto diffs = X.rowwise() - query;
		auto norms = diffs.rowwise().squaredNorm();
		auto dist = norms.minCoeff(&idx);
        return Neighbor{idx, dist};
    }
    template<class RowMatrixT, class VectorT, class ColVectorT>
    inline Neighbor onenn(const RowMatrixT& X, const VectorT& query,
        const ColVectorT& rowSquaredNorms)
    {
        // static_assert(VectorT::IsRowMajor, "query must be row-major");
        typename RowMatrixT::Index idx;
        if (VectorT::IsRowMajor) {
            auto dists = rowSquaredNorms - (2 * (X * query.transpose()));
            auto min_dist = dists.minCoeff(&idx);
            min_dist += query.squaredNorm();
			return Neighbor{idx, min_dist};
		} else {
            auto dists = rowSquaredNorms - (2 * (X * query));
            auto min_dist = dists.minCoeff(&idx) + query.squaredNorm();
            min_dist += query.squaredNorm();
            return Neighbor{idx, min_dist};
        }

        // SELF: pick up here by reimplementing this using rowSquaredNorms

        // // typename RowMatrixT::Index idx;
        // auto diffs = X.rowwise() - query;
        // auto norms = diffs.rowwise().squaredNorm();
        // auto dist = norms.minCoeff(&idx);
        // return Neighbor{idx, dist};
    }

    // ------------------------ knn
    template<class RowMatrixT, class VectorT>
    vector<Neighbor> knn(const RowMatrixT& X, const VectorT& query, size_t k)
    {
        assert(k > 0);
        if (VectorT::IsRowMajor) {
    		auto dists = dist::squared_dists_to_vector(X, query);
            return knn_from_dists(dists.data(), dists.size(), k);
        } else {
            auto dists = dist::squared_dists_to_vector(X, query.transpose());
            return knn_from_dists(dists.data(), dists.size(), k);
        }
    }
    template<class RowMatrixT, class VectorT, class ColVectorT>
    vector<Neighbor> knn(const RowMatrixT& X, const VectorT& query,
        size_t k, const ColVectorT& rowSquaredNorms)
    {
        assert(k > 0);
        auto dists = dist::squared_dists_to_vector(X, query, rowSquaredNorms);
        return knn_from_dists(dists.data(), dists.size(), k);
    }

    // ================================ batch of queries

    template<class RowMatrixT, class RowMatrixT2, class ColVectorT>
    inline vector<vector<Neighbor> > radius_batch(const RowMatrixT& X,
        const RowMatrixT2& queries, float radius_sq, const ColVectorT& rowNorms)
    {
        auto dists = dist::squared_dists_to_vectors(X, queries, rowNorms);
        assert(queries.rows() == dists.cols());
        auto num_queries = queries.rows();

        vector<vector<Neighbor> > ret;
        for (idx_t j = 0; j < num_queries; j++) {
            ret.emplace_back(neighbors_in_radius(dists.data(),
                                                 dists.size(), radius_sq));
        }
        return ret;
    }

    template<class RowMatrixT, class RowMatrixT2, class ColVectorT>
    inline vector<vector<Neighbor> > knn_batch(const RowMatrixT& X,
        const RowMatrixT2& queries, size_t k, const ColVectorT& rowNorms)
    {
        assert(k > 0);
        auto dists = dist::squared_dists_to_vectors(X, queries, rowNorms);
        assert(queries.rows() == dists.cols());
        auto num_queries = queries.rows();
        vector<vector<Neighbor> > ret;
        for (idx_t j = 0; j < num_queries; j++) {
            ret.emplace_back(knn_from_dists(dists.col(j).data(),
                                            dists.rows(), k));
        }
        return ret;
    }

    template<class RowMatrixT, class RowMatrixT2, class ColVectorT>
    inline vector<Neighbor> onenn_batch(const RowMatrixT& X,
        const RowMatrixT& queries, const ColVectorT& rowNorms)
    {
        auto wrapped_neighbors = knn_batch(X, queries, 1, rowNorms);
        return ar::map([](const vector<Neighbor> el) {
            return el[0];
        }, wrapped_neighbors);
    }

} // namespace brute

// ------------------------------------------------ early abandoning search

namespace abandon {

// ================================ single query

// ------------------------ radius

template<class Ret=Neighbor, class RowMatrixT=char, class VectorT=char,
    class DistT=char>
vector<Ret> radius(const RowMatrixT& X, const VectorT& query,
    DistT radius_sq, idx_t nrows=-1)
{
    vector<Ret> ret;
    for (idx_t i = 0; i < internal::_num_rows(X, nrows); i++) {
        auto dist = dist::abandon::dist_sq(X.row(i), query, radius_sq);
        if (dist < radius_sq) {
            internal::emplace_neighbor<Ret>{}(ret, dist, i);
        }
    }
    return ret;
}
// template<class RowMatrixT, class VectorT, class DistT>
// vector<Neighbor> radius(const RowMatrixT& X, const VectorT& query,
//     DistT radius_sq)
//     // const VectorT& query, idx_t num_rows, DistT radius_sq)
// {
//     vector<Neighbor> ret;
//     for (idx_t i = 0; i < X.rows(); i++) {
//         auto dist = dist::abandon::dist_sq(X.row(i).eval(), query, radius_sq);
//         if (dist <= radius_sq) {
//             ret.emplace_back(dist, i);
//         }
//     }
//     return ret;
// }

// template<class RowMatrixT, class VectorT, class DistT>
// vector<vector<Neighbor> > radius_batch(const RowMatrixT& X,
//     const RowMatrixT& queries, dist_t radius_sq)
// {
//     vector<vector<Neighbor> > ret;
//     for (idx_t j = 0; j < queries.rows(); j++) {
//         ret.emplace_back(radius(X, queries.row(j).eval(), radius_sq));
//     }
//     return ret;
// }

// template<class RowMatrixT, class VectorT, class IdxVectorT, class DistT>
// vector<Neighbor> radius_order(const RowMatrixT& X,
//     const VectorT& query_sorted, const IdxVectorT& order,
//     DistT radius_sq)
//     // const VectorT& query_sorted, const IdxVectorT& order, idx_t num_rows,
// {
//     vector<Neighbor> ret;
//     for (idx_t i = 0; i < X.rows(); i++) {
//         auto dist = dist::abandon::dist_sq_order_presorted(query_sorted, X.row(i).eval(),
//             order, radius_sq);
//         if (dist <= radius_sq) {
//             ret.emplace_back(dist, i);
//         }
//     }
//     return ret;
// }


// template<class RowMatrixT, class VectorT1, class VectorT2, class IdxVectorT,
//     class DistT>
// vector<Neighbor> radius_adaptive(const RowMatrixT& X, const VectorT1& query,
//     const VectorT2& means, VectorT1& query_tmp, IdxVectorT& order_tmp,
//     idx_t num_rows_thresh, DistT radius_sq)
// {
//     if (X.rows() >= num_rows_thresh) {
//         dist::abandon::create_ordered_query(query, means, query_tmp.data(), order_tmp.data());
//         return radius_order(X, query_tmp, order_tmp, radius_sq);
//     } else {
//         return radius(X, query, radius_sq);
//     }
// }



// ------------------------ 1nn

template<class RowMatrixT, class VectorT, class DistT=dist_t>
Neighbor onenn(const RowMatrixT& X, const VectorT& query,
    DistT d_bsf=kMaxDist, idx_t nrows=-1)
{
    Neighbor ret{kInvalidIdx, d_bsf};
	for (idx_t i = 0; i < internal::_num_rows(X, nrows); i++) {
        auto d = dist::abandon::dist_sq(X.row(i), query, d_bsf);
		// auto d = dist::dist_sq(X.row(i).eval(), query);
        assert(d >= 0);
        if (d < d_bsf) {
            d_bsf = d;
            ret = Neighbor{i, d};
        }
    }
    return ret;
}

// template<class RowMatrixT, class VectorT, class IdxVectorT,
// 	class DistT=dist_t>
// Neighbor onenn_order(const RowMatrixT& X,
//     const VectorT& query_sorted, const IdxVectorT& order,
//     DistT d_bsf=kMaxDist)
//     // const VectorT& query_sorted, const IdxVectorT& order, idx_t num_rows,
// {
//     Neighbor ret{.dist = d_bsf, .idx = kInvalidIdx};
//     for (idx_t i = 0; i < X.rows(); i++) {
//         auto dist = dist::abandon::dist_sq_order_presorted(query_sorted,
//             X.row(i).eval(), order, d_bsf);
//         if (dist < ret.dist) {
//             d_bsf = dist;
//             ret = Neighbor{i, dist};
//         }
//     }
//     return ret;
// }

// template<class RowMatrixT, class VectorT1, class VectorT2, class IdxVectorT,
//     class DistT=dist_t>
// Neighbor onenn_adaptive(const RowMatrixT& X, const VectorT1& query,
//     const VectorT2& means, VectorT1& query_tmp, IdxVectorT& order_tmp,
//     idx_t num_rows_thresh, DistT d_bsf=kMaxDist)
//     // idx_t num_rows, idx_t num_rows_thresh, DistT d_bsf=kMaxDist)
// {
//     if (X.rows() >= num_rows_thresh) {
//         create_ordered_query(query, means, query_tmp.data(), order_tmp.data());
//         return onenn_order(X, query_tmp, order_tmp, d_bsf);
//     } else {
//         return onenn(X, query, d_bsf);
//     }
// }

// ------------------------ knn

// #define TRACK_ABANDON_STATS

template<class RowMatrixT, class VectorT, class DistT=dist_t>
vector<Neighbor> knn(const RowMatrixT& X,
    const VectorT& query, int k, DistT d_bsf=kMaxDist, idx_t nrows=-1)
    // const VectorT& query, idx_t num_rows, int k, DistT d_bsf=kMaxDist)
{
	auto num_rows = internal::_num_rows(X, nrows);
    assert(k > 0);
    // assert(k <= num_rows);
    // PRINT("------------------------ abandon knn: starting a new knn query")

	// static_assert(!std::is_integral<DistT>::value, "");

    vector<Neighbor> ret;
    for (int32_t i = 0; i < ar::min(k, num_rows); i++) {
        auto d = dist::simple::dist_sq(X.row(i), query);
        ret.emplace_back(i, d);
    }
    sort_neighbors_ascending_distance(ret);
	d_bsf = ret[k-1].dist < d_bsf ? ret[k-1].dist : d_bsf;

    // auto initial_d_bsf = d_bsf;
    // PRINT_VAR(initial_d_bsf);

#ifdef TRACK_ABANDON_STATS
    int64_t num_abandons = 0;
    int64_t abandon_iters = 0;
#endif

	// because we abandon, we don't prefetch
	constexpr int num_prefetch_rows = 4;// <= num_rows ? 16 : num_rows;
    for (int i = 0; i < num_prefetch_rows; i++) {
		PREFETCH_TRANSIENT(X.row(i).data());
    }

	// vector<Neighbor> ret(k, Neighbor{kInvalidIdx, d_bsf});
	for (idx_t i = k; i < num_rows; i++) {
        if (num_prefetch_rows > 0 && i < (num_rows - num_prefetch_rows)) {
            PREFETCH_TRANSIENT(X.row(i + num_prefetch_rows).data());
        }
#ifdef TRACK_ABANDON_STATS
        dist_t d = dist::abandon::dist_sq(X.row(i), query, d_bsf,
            &num_abandons, &abandon_iters);
#else
		dist_t d = dist::abandon::dist_sq(X.row(i), query, d_bsf);
#endif
        // if (d_bsf < ret[k-1].dist) {
            // PRINT("found new best neighbor: ");
            // printf("new best neighbor: %lld, %g\n", i, d);
            // PRINT_VAR(num_abandons);
            // PRINT_VAR(i);
            // PRINT_VAR(d);
        // }
        // if (d < d_bsf) { PRINT_VAR(d_bsf); } // TODO remove
        d_bsf = maybe_insert_neighbor(ret, d, i); // figures out whether dist is lower
    }

#ifdef TRACK_ABANDON_STATS
    // PRINT_VAR(num_abandons);
    printf("abandoned %lld / %lld dist computations (%g blocks avg)\n",
        num_abandons, num_rows, abandon_iters / (double)num_rows);
#endif
    return ret;
}

// template<class RowMatrixT, class VectorT, class IdxVectorT,
// 	class DistT=dist_t>
// vector<Neighbor> knn_order(const RowMatrixT& X,
//     const VectorT& query_sorted, const IdxVectorT& order,
//     int k, DistT d_bsf=kMaxDist, idx_t nrows=-1)
//     // const VectorT& query_sorted, const IdxVectorT& order, idx_t num_rows,
// {
//     assert(k > 0);
//     vector<Neighbor> ret(k, {.dist = d_bsf, .idx = kInvalidIdx});
//     for (idx_t i = 0; i < X.rows(); i++) {
//         auto dist = dist_sq_order_presorted(query_sorted, X.row(i).eval(),
//             order, d_bsf);
//         d_bsf = maybe_insert_neighbor(ret, dist, i); // figures out whether dist is lower
//     }
//     return ret;
// }

// template<class RowMatrixT, class VectorT1, class VectorT2, class IdxVectorT,
//     class DistT=dist_t>
// vector<Neighbor> knn_adaptive(const RowMatrixT& X, const VectorT1& query,
//     const VectorT2& means, VectorT1& query_tmp, IdxVectorT& order_tmp,
//     idx_t num_rows_thresh, int k, DistT d_bsf=kMaxDist, idx_t nrows=-1)
// {
//     assert(k > 0);
//     if (X.rows() >= num_rows_thresh) {
//         create_ordered_query(query, means, query_tmp.data(), order_tmp.data());
//         return knn_order(X, query_tmp, order_tmp, d_bsf, nrows);
//     } else {
//         return knn(X, query, d_bsf, nrows);
//     }
// }

} // namespace abandon

} // namespace nn
#endif // __NN_SEARCH_HPP
