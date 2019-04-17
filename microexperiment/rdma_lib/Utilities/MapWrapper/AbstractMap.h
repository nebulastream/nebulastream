//
// Created by Toso, Lorenzo on 2019-03-26.
//

#pragma once

#include "Tables.h"
#include "Limits.h"

class AbstractMap {

#define _probe(probe_tuples, build_tuples, hashMap, results, probe_index) {\
    auto found = hashMap.find(probe_tuples.keys[probe_index]);\
    if (found != hashMap.end()) {\
        auto &build_index = found->second;\
        results.push_back(\
                probe_tuples.keys[probe_index], \
                build_tuples.payloads[build_index],\
                probe_tuples.payloads[probe_index]);\
    }\
}

public:

    virtual void build_parallel(const BuildTable &buildTable, const Limits & build_limits, size_t threadCount = 0) = 0;
    virtual std::vector<ResultTable> probe_parallel(
            const BuildTable &buildTable,
            const ProbeTable &probeTable,
            const Limits & probe_limits,
            size_t threadCount) const = 0 ;

};



