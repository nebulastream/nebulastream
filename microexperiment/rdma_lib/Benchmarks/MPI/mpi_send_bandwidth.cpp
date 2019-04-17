#include <stdio.h>
#include <mpi.h>
#include <chrono>
#include <vector>
#include <iostream>

#include "TimeTools.hpp"
#include "../../external/json.hpp"
#include "../ComputationParameters.h"
#include "BenchmarkTools.h"

using namespace nlohmann;

#define ITERATIONS 10
#define REPETITIONS_FOR_LARGE 100
#define REPETITIONS_FOR_SMALL 1000


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
    int process_count;
    int processor_name;
    char name[MPI_MAX_PROCESSOR_NAME];
    //MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI::ERRORS_THROW_EXCEPTIONS);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &process_count);
    MPI_Get_processor_name(name, &processor_name);

    printf("Rank %d running on %s\n", rank, name);


    size_t REPETITIONS_PER_ITERATION;
    for (auto NUMBER_OF_BUFFERS : BenchmarkTools::BUFFER_COUNTS) {
        for (auto BUFFER_SIZE : BenchmarkTools::BUFFER_SIZES) {
            if (BUFFER_SIZE > 1 * BenchmarkTools::MEGABYTE)
                REPETITIONS_PER_ITERATION = REPETITIONS_FOR_LARGE;
            else
                REPETITIONS_PER_ITERATION = REPETITIONS_FOR_SMALL;

            MPI_Barrier(MPI_COMM_WORLD);
            if(rank == 0){
                printf("BUFFER_SIZE: %lu\n", BUFFER_SIZE);
                printf("NUMBER_OF_BUFFERS: %lu\n", NUMBER_OF_BUFFERS);
            }

            char* local_buffer = new char[BUFFER_SIZE];

            std::vector<size_t> measured_times;
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {

                MPI_Request requests[NUMBER_OF_BUFFERS];
                MPI_Barrier(MPI_COMM_WORLD);
                auto start_time = TimeTools::now();

                for (int repetition = 0; repetition < REPETITIONS_PER_ITERATION; repetition++) {
                    for (int j = 0; j < NUMBER_OF_BUFFERS; j++) {
                        char* local_address = &local_buffer[j * (BUFFER_SIZE / NUMBER_OF_BUFFERS)];
                        int element_count = static_cast<int>(BUFFER_SIZE / NUMBER_OF_BUFFERS);

                        if(rank == 0)
                            MPI_Isend(local_address, element_count, MPI_BYTE, 1, 0, MPI_COMM_WORLD, &requests[j]);
                        else
                            MPI_Irecv(local_address, element_count, MPI_BYTE, 0, 0, MPI_COMM_WORLD, &requests[j]);
                    }
                    MPI_Waitall(static_cast<int>(NUMBER_OF_BUFFERS), requests, MPI_STATUSES_IGNORE);

                    MPI_Barrier(MPI_COMM_WORLD);
                }
                auto end_time = TimeTools::now();
                measured_times.push_back((end_time - start_time) / REPETITIONS_PER_ITERATION);
            }


            auto parameters = ComputationParameters::generate(false, false);
            if (rank % 2 == 0) {
                BenchmarkTools::output_results(measured_times, result_path, parameters, "MPI_Send_Bandwidth", BUFFER_SIZE,
                               NUMBER_OF_BUFFERS, REPETITIONS_PER_ITERATION);
            }
            MPI_Barrier(MPI_COMM_WORLD);

            delete[] local_buffer;
        }
    }
    MPI_Finalize();
    return 0;
}

