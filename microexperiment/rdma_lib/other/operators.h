//
// Created by Toso, Lorenzo on 2019-01-15.
//

#ifndef MAMPI_OPERATORS_H
#define MAMPI_OPERATORS_H

#include <cuda.h>
#include <cuda_runtime_api.h>


extern __device__
void gpu_insert_tuple(
        int64_t* __restrict__ hash_table,
        size_t hash_table_mask,
        int64_t key,
        int64_t payload
);

extern __global__
void gpu_build_hash_map(
        size_t num_elements,
        const int64_t* const __restrict__ join_column_data,
        size_t hash_table_size,
        int64_t* __restrict__ hash_table,
        size_t index_offset = 0
);

extern __device__
bool gpu_probe_tuple(
        const int64_t* const __restrict__ hash_table,
        uint64_t hash_table_mask,
        int64_t search_key,
        int64_t* found_payload
);

extern __global__
void gpu_probe(
        int64_t num_elements,
        const int64_t* const __restrict__ join_attribute_data,
        int64_t hash_table_length,
        const int64_t* const __restrict__ hash_table,
        int64_t* __restrict__ aggregation_result
);

extern __global__
void gpu_count_matches(
        int64_t num_elements,
        const int64_t* const __restrict__ results,
        unsigned int * __restrict__ matches
);



#endif //MAMPI_OPERATORS_H
