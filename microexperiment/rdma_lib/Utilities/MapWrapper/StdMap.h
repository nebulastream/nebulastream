//
// Created by Toso, Lorenzo on 2019-03-26.
//

#pragma once

#include <unordered_map>
#include "AbstractMap.h"

class StdMap : public AbstractMap{

private:
    std::unordered_map<size_t, size_t> map;

public:
    explicit StdMap(size_t tuple_count);

    void build_parallel(const BuildTable &buildTable, const Limits & build_limits, size_t threadCount = 0) override;
    std::vector<ResultTable> probe_parallel(const BuildTable &buildTable,
                                            const ProbeTable &probeTable,
                                            const Limits & probe_limits,
                                            size_t threadCount = 0) const override;

};



