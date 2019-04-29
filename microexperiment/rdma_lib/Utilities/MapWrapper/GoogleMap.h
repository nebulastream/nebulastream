//
// Created by Toso, Lorenzo on 2019-03-26.
//

#pragma once

#include "AbstractMap.h"
#include "dense_hash_map/dense_hash_map.h"


class GoogleMap : public AbstractMap {

private:
    google::dense_hash_map<size_t, size_t> map;

public:
    explicit GoogleMap(size_t tuple_count);

    void build_parallel(const BuildTable &buildTable, const Limits & build_limits, size_t threadCount = 0) override;
    std::vector<ResultTable> probe_parallel(const BuildTable &buildTable,
                                            const ProbeTable &probeTable,
                                            const Limits & probe_limits,
                                            size_t threadCount = 0) const override;


};



