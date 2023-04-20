//
//  search.hpp
//  Compress
//
//  Created by DB on 1/9/18.
//  Copyright © 2018 D Blalock. All rights reserved.
//

#ifndef search_h
#define search_h

#include <stdint.h>
#include <type_traits>
#include <vector>

//#include "Dense"

#include "nn_utils.hpp"

// template<int

#ifndef MIN
    #define MIN(X, Y) ((X) < (Y) ? (X) : (Y))
#endif
#ifndef MAX
    #define MAX(X, Y) ((X) > (Y) ? (X) : (Y))
#endif


namespace StorageOrderE { enum { ColMajor = 0, RowMajor = 1 }; }
enum class ReductionE { Sum, Min, Max };
enum class ElemwiseOpE { Identity, SquareDiff, Prod };
enum class NormalizationE { Identity, SSE };
namespace WindowOpE { enum { L2, Corr, Cosine, Dot, Mean, Var, Median }; }
enum class SearchKindE { Knn, Radius };

template <int Op> class WindowOpTraits {};
template <> class WindowOpTraits<WindowOpE::L2> {
    static const ElemwiseOpE ElemwiseOp = ElemwiseOpE::SquareDiff;
    static const ReductionE Reduction = ReductionE::Sum;
    static const NormalizationE Normalization = NormalizationE::Identity;
};
template <> class WindowOpTraits<WindowOpE::Cosine> {
    static const ElemwiseOpE ElemwiseOp = ElemwiseOpE::Prod;
    static const ReductionE Reduction = ReductionE::Sum;
    static const NormalizationE Normalization = NormalizationE::SSE;
};
template <> class WindowOpTraits<WindowOpE::Dot> {
    static const ElemwiseOpE ElemwiseOp = ElemwiseOpE::SquareDiff;
    static const ReductionE Reduction = ReductionE::Sum;
    static const NormalizationE Normalization = NormalizationE::Identity;
};

template <class data_t> class DataTypeTraits {};
template <> class DataTypeTraits<uint8_t> { using AccumulatorT = uint16_t; };
template <> class DataTypeTraits<uint16_t> { using AccumulatorT = uint32_t; };


// class OnlineWindowStat {
//     uint32_t window_nrows;
//     uint16_t window_ncols;
// };

// TODO have a bunch of OnlineWindowStat classes with this same API
// template<class DataT, bool IsDense, int StorageOrder=StorageOrderE::RowMajor>
// EDIT: needs rowmajor storage order or windows won't actually include the
// initial entries in each column, unless we also get the total number of
// rows in the whole dataset
template<class DataT, bool IsDense>
class OnlineMeanRowmajor {
public:
    using dist_t = typename DataTypeTraits<DataT>::AccumulatorT;
    static const bool is_dense = IsDense;

    OnlineMeanRowmajor(uint32_t nrows, uint32_t ncols):
        _nrows(nrows), _ncols(ncols)
    {
        reset();
    }

    OnlineMeanRowmajor(uint32_t nrows, uint32_t ncols,
        std::vector<uint16_t>& which_dims):
        _nrows(nrows), _ncols(ncols), _which_dims(which_dims)
    {
        if (IsDense) {
            printf("ERROR: can't specify subset of dims for Dense OnlineMean!");
            exit(1);
        }
        reset();
    }

    void init(const dist_t* window_start) {
        if (IsDense) {
            for (uint32_t i = 0; i < _nrows; i++) {
                for (uint32_t j = 0; j < _ncols; j++) {
                    _sums[j] += window_start[i * _ncols + j];
                }
            }
        } else {
            for (uint32_t i = 0; i < _nrows; i++) {
                for (uint32_t j_idx = 0; j_idx < _which_dims.size(); j_idx++) {
                    auto j = _which_dims[j_idx];
                    _sums[j_idx] += window_start[i * _ncols + j];
                }
            }
        }
    }

    void update(const dist_t* old_window_row, const dist_t* new_window_row) {
        if (IsDense) {
            for (uint32_t j = 0; j < _ncols; j++) {
                _sums[j] += (new_window_row[j] - old_window_row[j]);
            }
        } else {
            for (uint32_t j_idx = 0; j_idx < _which_dims.size(); j_idx++) {
                auto j = _which_dims[j_idx];
                _sums[j_idx] += (new_window_row[j] - old_window_row[j]);
            }
        }
    }

    void write_means(dist_t* out) const {
        for (uint32_t j = 0; j < _sums.size(); j++) {
            out[j] = _sums[j] / _nrows;
        }
    }

    void reset () {
        if (_sums.size() == 0) {
            auto sums_size = IsDense ? _ncols : _which_dims.size();
            for (size_t i = 0; i < sums_size; i++) {
                _sums[i].push_back(0);
            }
        } else {
            for (size_t i = 0; i < _sums.size(); i++) {
                _sums[i] = 0;
            }
        }
    }

    uint32_t nrows() const { return _nrows(); }
    uint16_t ncols() const { return _ncols(); }

private:
    std::vector<uint16_t> _which_dims;
    std::vector<dist_t> _sums;
    uint32_t _nrows;
    uint16_t _ncols;
};


template<typename DataT, int StorageOrder=StorageOrderE::RowMajor>
class BufferView {
public:
    DataT *const data;
    uint32_t nrows;
    uint32_t ndims;
};

template<typename DataT, int WindowOp, int StorageOrder=StorageOrderE::RowMajor>
class Query {
public:
    using data_t = DataT;
    static const int window_op = WindowOp;
    static const int storage_order = WindowOp;
    const BufferView<data_t, storage_order> buff;
    // DataT *const data;
    vector<uint32_t> which_dims;
    // uint32_t nrows;
    uint8_t k;
};



template<typename data_t, typename QueryT>
auto sparse_dist(const data_t* window_start, uint16_t ndims, const QueryT& q)
    -> typename DataTypeTraits<data_t>::AccumulatorT
{
    using dist_t = typename DataTypeTraits<data_t>::AccumulatorT;
    using dtraits_t = WindowOpTraits<QueryT::WindowOp>;
    static const ElemwiseOpE elem_op = dtraits_t::ElemwiseOp;

    static_assert(elem_op == ElemwiseOpE::SquareDiff ||
        elem_op == ElemwiseOpE::Prod, "Unsupported elementwise operation!");

    dist_t dist = 0;
    for (uint32_t winrow = 0; winrow < q.buff.nrows; winrow++) {
        const data_t* row_ptr = window_start + winrow * ndims;
        const data_t* q_row_ptr = q.buff.data + winrow * ndims;
        // for each (used) element in the row
        for (uint16_t dim_idx = 0; dim_idx < q.which_dims.size(); dim_idx++) {
            data_t x_val = row_ptr[q.which_dims[dim_idx]];
            data_t q_val = q_row_ptr[dim_idx];
            if (elem_op == ElemwiseOpE::SquareDiff) {
                dist_t diff = x_val - q_val;
                dist += diff * diff;
            } else if (elem_op == ElemwiseOpE::Prod) {
                dist += x_val * q_val;
            }
        }
    }
    return dist;
}
template<typename DataT, typename QueryT>
auto dense_dist(const DataT* window_start, uint16_t ndims, const QueryT& q)
    -> typename DataTypeTraits<DataT>::AccumulatorT
{
    using dist_t = typename DataTypeTraits<DataT>::AccumulatorT;
    using dtraits_t = WindowOpTraits<QueryT::WindowOp>;
    static const ElemwiseOpE elem_op = dtraits_t::ElemwiseOp;

    dist_t dist = 0;
    for (uint32_t winrow = 0; winrow < q.buff.nrows; winrow++) {
        const DataT* row_ptr = window_start + winrow * ndims;
        const DataT* q_row_ptr = q.buff.data + winrow * ndims;
        // for each (used) element in the row
        for (uint16_t dim = 0; dim < ndims; dim++) {
            if (elem_op == ElemwiseOpE::SquareDiff) {
                dist_t diff = row_ptr[dim] - q_row_ptr[dim];
                dist += diff * diff;
            } else if (elem_op == ElemwiseOpE::Prod) {
                dist += row_ptr[dim] * q_row_ptr[dim];
            }
        }
    }
    return dist;
}

// vector<Neighbor> knn_rowmajor(const data_t* data, uint32_t nrows, uint16_t ndims,
    // uint32_t window_len, vector<idxs> which_dims, uint8_t k)
template<typename DataT, typename QueryT>
vector<nn::Neighbor<DataT> > knn_rowmajor(const BufferView<DataT, StorageOrderE::RowMajor>& X,
    const QueryT& Q)
{
    using dist_t = typename DataTypeTraits<DataT>::AccumulatorT;
    using neighbor_t = nn::Neighbor<dist_t, uint32_t>;
    using dtraits_t = WindowOpTraits<QueryT::WindowOp>;
    static const ElemwiseOpE elem_op = dtraits_t::ElemwiseOp;

    static_assert(std::is_same<DataT, typename QueryT::data_t>::value,
        "Query expects a different datatype than the data slice!");

    // initialize knn with first k windows (and stop there if number of windows
    // doesn't exceed k)
    int64_t nwindows = X.buff.nrows - Q.buff.nrows + 1;
    vector<neighbor_t> ret;
    for (uint32_t i = 0; i < MIN(Q.k, nwindows); i++) {

    }
    if (nwindows <= Q.k) {
        return ret;
    }


    uint32_t q_ndims = Q.which_dims.size();
    if (q_ndims == 0) { // dense case; query uses all dimensions
        q_ndims = X.buff.ndims;


    } else { // sparse case
        dist_t dist = 0;
        for (uint32_t w = 0; w < nwindows; w++) {  // for each window
            // for each row in the window
            // for (uint32_t winrow = 0; winrow < q.buff.nrows; winrow++) {
            //     const data_t* row_ptr = X.data + (w + winrow) * d.ndims;
            //     const data_t* q_row_ptr = Q.buff.data + winrow * d.ndims;
            //     // for each (used) element in the row
            //     for (uint16_t dim_idx = 0; dim_idx < q_ndims; dim_idx++) {
            //         data_t x_val = row_ptr[q.which_dims[dim_idx]];
            //         data_t q_val = q_row_ptr[dim_idx];
            //         if (elem_op == ElemwiseOpE::SquareDiff) {
            //             dist_t diff = x_val - q_val;
            //             dist += diff * diff;
            //         } else if (elem_op == ElemwiseOpE::Prod) {
            //             dist += x_val * q_val;
            //         }
            //     }
            // }
        }
    }

}


#endif /* search_h */
