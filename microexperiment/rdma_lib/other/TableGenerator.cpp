//
// Created by Toso, Lorenzo on 2019-01-25.
//

#include <cstdlib>
#include <random>
#include <array>
#include "TableGenerator.h"
#include <ZipfDistribution.h>
#include <omp.h>



BuildTable TableGenerator::generate_build_table(size_t tuple_count, double selectivity, size_t & max_key) {
    std::mt19937 mt(BUILD_SEED);
    std::uniform_real_distribution<double> dist(0.0, 1.0);

    BuildTable table(tuple_count);
    size_t key = 1;
    for(size_t i = 0; i < tuple_count; i++)
    {
        auto r = dist(mt);
        while (r < 1-selectivity) {
            key++;
            r = dist(mt);
        }
        table.keys[i] = key;
        for (size_t j = 0; j < B_PAYLOAD_SIZE; j++)
            table.payloads[i][j] = (char)key;
        key++;
    }
    max_key = key-1;

    return table;
}

ProbeTable TableGenerator::generate_probe_table_zipf(size_t tuple_count, size_t max_key, double alpha) {

    std::mt19937 gen(PROBE_SEED); //Standard mersenne_twister_engine seeded with rd()
    zipf_distribution<size_t, double> dis(max_key, alpha);

    ProbeTable table(tuple_count);

#pragma omp parallel for
    for(size_t i = 0 ; i < tuple_count; i++){
        auto key = dis(gen);

        for (char &p : table.payloads[i])
            p = (char)key;

        table.keys[i] = key;
    }

    return table;

}

ProbeTable TableGenerator::generate_probe_table_uniform(size_t tuple_count, size_t max_key) {
    std::mt19937 gen(PROBE_SEED); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_int_distribution<size_t> dis(1, max_key+1);

    ProbeTable table(tuple_count);


//#pragma omp parallel for
    for(size_t i = 0 ; i < tuple_count; i++){
        auto key = dis(gen);

        table.keys[i] = key;
        for (size_t j = 0; j < P_PAYLOAD_SIZE; j++)
            table.payloads[i][j] = (char)key;

    }
    return table;
}


BuildTable TableGenerator::fast_generate_build_table(size_t tuple_count) {

    BuildTable table(tuple_count);

#pragma omp parallel for
    for (size_t key = 1; key <= tuple_count; key++)
    {
        table.keys[key-1] = key;
        for (char &i : table.payloads[key-1])
            i = (char)key;

    }
    return table;
}

ProbeTable TableGenerator::fast_generate_probe_table_zipf(size_t build_tuple_count, size_t probe_tuple_count, double selectivity, double alpha) {

    std::mt19937 gen(PROBE_SEED); //Standard mersenne_twister_engine seeded with rd()
    size_t max_key = static_cast<size_t>(build_tuple_count / selectivity) + 1;
    zipf_distribution<size_t, double> dis(max_key, alpha);

    ProbeTable table(probe_tuple_count);

#pragma omp parallel for
    for(size_t i = 0 ; i < probe_tuple_count; i++){
        auto key = dis(gen);

        for (char &p : table.payloads[i])
            p = (char)key;

        table.keys[i] = key;
    }

    return table;

}

ProbeTable TableGenerator::fast_generate_probe_table_uniform(size_t build_tuple_count, size_t probe_tuple_count, double selectivity) {
    std::mt19937 gen(PROBE_SEED); //Standard mersenne_twister_engine seeded with rd()

    size_t max_key = static_cast<size_t>(build_tuple_count / selectivity) + 1;
    std::uniform_int_distribution<size_t> dis(1, max_key);

    ProbeTable table(probe_tuple_count);

#pragma omp parallel for
    for(size_t i = 0 ; i < probe_tuple_count; i++){
        auto key = dis(gen);

        for (char &p : table.payloads[i])
            p = (char)key;

        table.keys[i] = key;
    }

    return table;
}
