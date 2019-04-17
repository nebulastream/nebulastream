//
// Created by Toso, Lorenzo on 2018-11-27.
//
#include <mpi.h>
#include <sys/time.h>

#include "MPIHelper.h"
#include <math.h>
#include <unistd.h>
#include <iostream>
#include "TimeTools.hpp"
#include "BenchmarkTools.h"
#include "../ComputationParameters.h"

size_t ITERATIONS = 100;
size_t REPETITIONS = 10000;

void run_test(const std::string &output_path) {

    char * read_write_buffer;
    printf("Alloc_mem\n");
    MPI_Alloc_mem(10, MPI_INFO_NULL, &read_write_buffer);

    MPI_Win win;
    printf("Win_create\n");
    MPI_Win_create(read_write_buffer, 1, sizeof(char), MPI_INFO_NULL, MPI_COMM_WORLD, &win);

    size_t rank = MPIHelper::get_rank();

    printf("Start Measurement\n");
    std::vector<Timestamp> measured_times;
    for(int iteration = 0; iteration < ITERATIONS; iteration++) {
        printf("Starting iteration %d!\n", iteration);
        read_write_buffer[0] = (char) 0;
        MPI_Barrier(MPI_COMM_WORLD);

        MPI_Status status;
        auto start_time = TimeTools::now();
        for (int i = 0; i < REPETITIONS; i++) {
            read_write_buffer[0] = 0;
            if(rank != 0){
                read_write_buffer[0] = 1;
                MPI_Send(read_write_buffer, 1, MPI_BYTE, 0, 0, MPI_COMM_WORLD);
                MPI_Recv(read_write_buffer, 1, MPI_BYTE, 0, 0, MPI_COMM_WORLD, &status);
                if(read_write_buffer[0] != 2)
                    printf("error\n");
            }
            else {
                MPI_Recv(read_write_buffer, 1, MPI_BYTE, 1, 0, MPI_COMM_WORLD, &status);
                if(read_write_buffer[0] != 1)
                    printf("error\n");
                read_write_buffer[0] = 2;
                MPI_Send(read_write_buffer, 1, MPI_BYTE, 1, 0, MPI_COMM_WORLD);
            }
        }
        auto end_time = TimeTools::now();
        MPI_Barrier(MPI_COMM_WORLD);

        measured_times.push_back(((end_time-start_time)/REPETITIONS)/2);
        printf("Done with iteration %d!\n", iteration);

    }
    printf("Done with Measurement!\n");

    auto parameters = ComputationParameters::generate(false, false);
    if(rank == 0) {
        BenchmarkTools::output_results(measured_times, output_path, parameters, "MPI_Send_Receive_Latency", 1, 1, REPETITIONS);
    }
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
    int process_count;
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