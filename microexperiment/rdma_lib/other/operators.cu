//
// Created by Toso, Lorenzo on 2019-01-15.
//

/*
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/.
 *
 *
 * Copyright 2018 German Research Center for Artificial Intelligence (DFKI)
 * Author: Clemens Lutz <clemens.lutz@dfki.de>
 *
 * Note that parts of this code are based on the Hawk query compiler by
 * Sebastian Breß et al.
 * See https://github.com/TU-Berlin-DIMA/Hawk-VLDBJ for details.
 */


/*
 * Assumptions:
 *
 * Hash table's size is 2^x - 1
 * Key with value==-1 is reserved for NULL values
 * Key and payload are int64_t
 * Hash table is initialized with all entries set to -1
 *
 * TODO:
 * - use HtEntry struct for key/payload pairs and adjust insert/probe logic
 */

/* See Richter et al., Seven-Dimensional Analysis of Hashing Methods
 * Multiply-shift hash function
 * Requirement: hash factor is an odd 64-bit integer
*/
#include "operators.h"
#include <cstddef>
#include <stdio.h>

#define HASH_FACTOR 123456789123456789ull

#define NULL_KEY 0xFFFFFFFFFFFFFFFFll


__device__
void gpu_insert_tuple(
        int64_t* __restrict__ hash_table, // 128* -1
        size_t hash_table_mask,           // 127
        int64_t key,                        // key
        int64_t payload                     // 0
)
{
    uint32_t index = static_cast<uint32_t>(key);
    index *= HASH_FACTOR;

    for (uint32_t i = 0; i < hash_table_mask + 1; ++i, index += 2) {
        // Effectively index = index % ht_size
        index &= hash_table_mask;

        // Effectively a index -= index % 2
        // This is done because each key/payload pair occupies 2 slots in ht array
        index &= ~1ul;

        unsigned long long int null_key = NULL_KEY;
        int64_t old = hash_table[index];
        if (old == NULL_KEY) {
            old = (int64_t)atomicCAS((unsigned long long int*)&hash_table[index], null_key, (unsigned long long int)key);
            if (old == NULL_KEY) {
                hash_table[index + 1] = payload;
                return;
            }
        }
    }
}


__global__
void gpu_build_hash_map(
        size_t num_elements,
        const int64_t* const __restrict__ join_column_data,
        size_t hash_table_size,
        int64_t* __restrict__ hash_table,
        size_t index_offset
)
{
    const uint32_t global_tid = blockIdx.x *blockDim.x + threadIdx.x;
    const uint32_t number_of_threads = blockDim.x * gridDim.x;


    for(uint32_t tuple_index = global_tid; tuple_index < num_elements; tuple_index += number_of_threads)
    {
        gpu_insert_tuple(
                hash_table,
                hash_table_size - 1,
                join_column_data[tuple_index],
                tuple_index + index_offset
        );
    }
}

__device__
bool gpu_probe_tuple(
        int64_t const* const __restrict__ hash_table,
        size_t hash_table_mask,
        int64_t search_key,
        int64_t* found_payload
)
{
    uint32_t index = static_cast<uint32_t>(search_key);
    index *= HASH_FACTOR;

    for (uint32_t i = 0; i < hash_table_mask + 1; ++i, index += 2) {
        index &= hash_table_mask;
        index &= ~1ul;


        if (hash_table[index] == search_key) {
            *found_payload = hash_table[index + 1];
            return true;
        } else if (hash_table[index] == NULL_KEY) {
            return false;
        }
    }

    return false;
}


__global__
void gpu_probe(
        int64_t num_elements,
        const int64_t* const __restrict__ join_attribute_data,
        int64_t hash_table_length,
        const int64_t* const __restrict__ hash_table,
        int64_t* __restrict__ aggregation_result
)
{
    const uint32_t global_idx = blockIdx.x * blockDim.x + threadIdx.x;
    const uint32_t number_of_threads = blockDim.x * gridDim.x;

    for(uint32_t tuple_index = global_idx; tuple_index < num_elements; tuple_index += number_of_threads)
    {
        int64_t hash_table_payload;
        if (
                gpu_probe_tuple(
                        hash_table,
                        hash_table_length - 1,
                        join_attribute_data[tuple_index],
                        &hash_table_payload)
                )
        {
            aggregation_result[tuple_index] = hash_table_payload;
        } else {
            aggregation_result[tuple_index] = -1;
        }
    }
}

extern __global__
void gpu_count_matches(
        int64_t num_elements,
        const int64_t* const __restrict__ results,
        unsigned int * matches
)
{
    const uint32_t global_idx = blockIdx.x * blockDim.x + threadIdx.x;
    const uint32_t number_of_threads = blockDim.x * gridDim.x;

    uint32_t local_sum = 0;

    for(uint32_t tuple_index = global_idx; tuple_index < num_elements; tuple_index += number_of_threads)
    {
        if(results[tuple_index] != -1 )
            local_sum++;
    }
    atomicAdd(matches, local_sum);

}