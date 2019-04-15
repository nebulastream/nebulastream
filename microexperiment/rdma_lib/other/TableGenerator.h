//
// Created by Toso, Lorenzo on 2019-01-25.
//

#pragma once

#include "Tables.h"

#define PROBE_SEED 0
#define BUILD_SEED 0

class TableGenerator {
public:
    static BuildTable generate_build_table(size_t tuple_count, double selectivity, size_t & max_key);
    static ProbeTable generate_probe_table_uniform(size_t tuple_count, size_t max_key);
    static ProbeTable generate_probe_table_zipf(size_t tuple_count, size_t max_key, double alpha);

    // These method work very well but create larger keys on th eprobe side, which causes problems with the google hash map
    static BuildTable fast_generate_build_table(size_t tuple_count);
    static ProbeTable fast_generate_probe_table_uniform(size_t build_tuple_count, size_t probe_tuple_count, double selectivity);
    static ProbeTable fast_generate_probe_table_zipf(size_t build_tuple_count, size_t probe_tuple_count, double selectivity, double alpha);
};



