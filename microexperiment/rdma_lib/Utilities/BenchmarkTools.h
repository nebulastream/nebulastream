//
// Created by Toso, Lorenzo on 2018-11-23.
//

#ifndef MAMPI_BENCHMARKTOOLS_H
#define MAMPI_BENCHMARKTOOLS_H
#pragma once

#include <string>
#include <json11/json11.hpp>
#include "ComputationParameters.h"

using namespace json11;


struct TestParameters{
    std::string cpu_or_gpu;
    size_t local_numa = 0;
    size_t local_ib = 0;
    size_t remote_ib = 0;
    std::string result_path;
};

class BenchmarkTools {


public:

    static const size_t ITERATIONS = 10;
    static const size_t REPETITIONS_FOR_LARGE = 100;
    static const size_t REPETITIONS_FOR_SMALL = 1000;

    static const size_t KILOBYTE = ((1024L));
    static const size_t MEGABYTE = ((KILOBYTE*1024L));
    static const size_t GIGABYTE = ((MEGABYTE*1024L));

    static const std::vector<size_t> BUFFER_SIZES;

    static const std::vector<size_t> BUFFER_COUNTS;



    static void write_to_file(const std::string &out_path, Json json);

    static void output_results(const std::vector<size_t> &measured_times, const std::string &out_path,
                        ComputationParameters &parameters, const std::string & name,
                        size_t BUFFER_SIZE, size_t NUMBER_OF_BUFFERS, size_t REPETITIONS_PER_ITERATION);

    static void
    output_results(std::vector<unsigned long> measured_times, const TestParameters &connection_parameters, ComputationParameters computationParameters, const std::string & name_prefix);
};

#endif //MAMPI_BENCHMARKTOOLS_H
