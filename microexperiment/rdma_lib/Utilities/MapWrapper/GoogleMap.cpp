//
// Created by Toso, Lorenzo on 2019-03-26.
//

#include "GoogleMap.h"
#include <climits>
#include <omp.h>

GoogleMap::GoogleMap(size_t tuple_count){
    map = google::dense_hash_map<size_t, size_t>(tuple_count);
    map.set_empty_key(ULLONG_MAX - 1);
    map.min_load_factor(0.0);

}

void GoogleMap::build_parallel(const BuildTable &buildTable, const Limits & build_limits, size_t threadCount)
{
    for (size_t index = build_limits.start_index; index < build_limits.end_index; index++) {
        map[buildTable.keys[index]] = index;
    }
}

std::vector<ResultTable> GoogleMap::probe_parallel(const BuildTable &buildTable, const ProbeTable &probeTable, const Limits & probe_limits, size_t threadCount) const
{
    if (threadCount == 0)
        threadCount = omp_get_max_threads()/2;

    size_t recordsPerThread = probe_limits.size() / threadCount;
    size_t remainingRecords = probe_limits.size() % threadCount;
    omp_set_dynamic(0);

    std::vector<ResultTable> result_tables(threadCount);

    #pragma omp parallel num_threads(threadCount)
    {
        size_t threadId = omp_get_thread_num();
        size_t sliceSize = recordsPerThread;
        size_t startAt = omp_get_thread_num() * sliceSize + probe_limits.start_index;

        if(threadId == threadCount-1)
            sliceSize += remainingRecords;

        for(size_t index = startAt; index < startAt + sliceSize; index++)
        {
            _probe(probeTable, buildTable, map, result_tables[threadId], index);
        }

    };

    return result_tables;

}