//
// Created by Toso, Lorenzo on 2019-01-10.
//

#include <sys/time.h>
#include <iostream>

#include <math.h>
#include <unistd.h>
#include <random>
#include "TimeTools.hpp"
#include "BenchmarkTools.h"
#include "../ComputationParameters.h"

#include "Debug.h"
#include "MPIHelper.h"


size_t ITERATIONS = 100;
size_t REPETITIONS = 1000000;
size_t BUFFER_SIZE = 1024*1024*1024;


void run_test(const std::string &output_path) {
    ConnectionCollection c;
    NodeParameters nodeParameters("", false, false);

    auto parameters = ComputationParameters::generate(c, nodeParameters);
;

    void * mem_source = malloc(BUFFER_SIZE);

    std::mt19937 mt(0);

    int* p = (int*)mem_source;
    std::uniform_int_distribution<int> dist(0, static_cast<int>(BUFFER_SIZE / sizeof(int)));

    for(size_t i = 0; i < BUFFER_SIZE/sizeof(int); i++)
        p[i] = dist(mt);

    std::vector<Timestamp> measured_times;
    for(size_t iteration = 0; iteration < ITERATIONS; iteration++){
        auto start_time = TimeTools::now();

        int next_src_index = 0;

        for (size_t i = 0; i < REPETITIONS; i++) {
            volatile int* s_address = ((int*)mem_source) + next_src_index;
            next_src_index = *s_address;
        }
        auto end_time = TimeTools::now();
        measured_times.push_back((end_time-start_time)/REPETITIONS);
        QUICK_PRINT("Time: %lu\n", (end_time-start_time));
    }
    BenchmarkTools::output_results(measured_times, output_path, parameters, "Local_RAM_Latency", 1, 1, REPETITIONS);
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Missing parameter!" <<
                  std::endl;
        std::cerr << "Usage: " << argv[0] << " <Output path>" <<
                  std::endl;
        return -1;
    }
    std::string result_path = argv[1];

    MPIHelper::set_process_count(1);
    MPIHelper::set_rank(0);
    run_test(result_path);

    return 0;
}