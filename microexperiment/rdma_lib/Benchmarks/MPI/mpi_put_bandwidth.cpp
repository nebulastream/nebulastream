#include <stdio.h>
#include <mpi.h>
#include <chrono>
#include <vector>
#include <iostream>

#include "TimeTools.hpp"
#include "../../ComputationParameters.h"
#include "BenchmarkTools.h"
#include "MPIHelper.h"

using namespace nlohmann;

#define ITERATIONS 10
#define REPETITIONS_FOR_LARGE 100
#define REPETITIONS_FOR_SMALL 1000


int run_test(const std::string & result_path) {
    size_t rank = MPIHelper::get_rank();

    size_t REPETITIONS_PER_ITERATION;
    for (auto NUMBER_OF_BUFFERS : BenchmarkTools::BUFFER_COUNTS) {
        for (auto BUFFER_SIZE : BenchmarkTools::BUFFER_SIZES) {
            if (BUFFER_SIZE > 1 * BenchmarkTools::MEGABYTE)
                REPETITIONS_PER_ITERATION = REPETITIONS_FOR_LARGE;
            else
                REPETITIONS_PER_ITERATION = REPETITIONS_FOR_SMALL;
            int bytes_per_buffer = static_cast<int>(BUFFER_SIZE / NUMBER_OF_BUFFERS);


            if(rank == 0){
                printf("BUFFER_SIZE: %lu\n", BUFFER_SIZE);
                printf("NUMBER_OF_BUFFERS: %lu\n", NUMBER_OF_BUFFERS);
            }

            char* local_buffer = new char[BUFFER_SIZE];

            MPI_Win win;
            MPI_Win_create(local_buffer, BUFFER_SIZE, 1, MPI_INFO_NULL, MPI_COMM_WORLD, &win);

            std::vector<size_t> measured_times;
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {

                MPI_Win_fence(0, win);
                auto start_time = TimeTools::now();

                for (int repetition = 0; repetition < REPETITIONS_PER_ITERATION; repetition++) {
                    if (rank == 0 ) {
                        for (int j = 0; j < NUMBER_OF_BUFFERS; j++) {
                            char* local_address = &local_buffer[j * (BUFFER_SIZE / NUMBER_OF_BUFFERS)];
                            MPI_Put(local_address, bytes_per_buffer, MPI_BYTE, 1, 0, bytes_per_buffer, MPI_BYTE, win);
                        }
                    }

                    MPI_Win_fence(0, win);
                }
                auto end_time = TimeTools::now();
                measured_times.push_back((end_time - start_time) / REPETITIONS_PER_ITERATION);
            }


            auto parameters = ComputationParameters::generate(false, false);
            if (rank == 0) {
                BenchmarkTools::output_results(measured_times, result_path, parameters, "MPI_Put_Bandwidth", BUFFER_SIZE,
                               NUMBER_OF_BUFFERS, REPETITIONS_PER_ITERATION);
            }
            MPI_Win_fence(0, win);

            MPI_Win_free(&win);

            delete[] local_buffer;
        }
    }
    return 0;
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

    MPI_Init(&argc, &argv);
    int rank;
    int processor_name;
    char name[MPI_MAX_PROCESSOR_NAME];
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Get_processor_name(name, &processor_name);

    printf("Rank %d running on %s\n", rank, name);
    MPI_Barrier(MPI_COMM_WORLD);

    run_test(result_path);

    MPI_Finalize();
    return 0;
}
