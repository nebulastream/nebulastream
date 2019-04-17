//
// Created by Toso, Lorenzo on 2018-11-26.
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
    size_t rank = MPIHelper::get_rank();
    char * read_write_buffer;
    char * send_receive_buffer;
    auto parameters = ComputationParameters::generate(false, false);

    MPI_Alloc_mem(10, MPI_INFO_NULL, &read_write_buffer);
    MPI_Alloc_mem(128, MPI_INFO_NULL, &send_receive_buffer);


    MPI_Win win;
    MPI_Win_create(read_write_buffer, 1, sizeof(char), MPI_INFO_NULL, MPI_COMM_WORLD, &win);
    MPI_Win_fence(0, win);

    if(rank != 0){
        printf("Starting Receive.\n");
        MPI_Recv(send_receive_buffer, 128, MPI_BYTE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("Finished Receive.\n");

        for(int iteration = 0; iteration < ITERATIONS; iteration++) {

            printf("Starting iteration %d!\n", iteration);
            read_write_buffer[0] = (char) 0;
            MPI_Win_fence(0, win);

            for (int i = 0; i < REPETITIONS; i++) {
                MPI_Win_fence(0, win);
                while (read_write_buffer[0] != 1);
                //printf("Received rw1!\n");
                //read_write_buffer[0] = 2;
                //MPI_Put(read_write_buffer, 1, MPI_BYTE, 1, 0, 1, MPI_BYTE, win);
                //MPI_Win_fence(0, win);
            }
            MPI_Win_fence(0, win);
            printf("Done with iteration %d!\n", iteration);

        }
        printf("Done with Measurement!\n");

    } else {
        usleep(2000000);
        printf("Starting Send.\n");
        MPI_Send(send_receive_buffer, 128, MPI_BYTE, 1, 0, MPI_COMM_WORLD);
        printf("Finished Send.\n");



        std::vector<Timestamp> measured_times;
        printf("Starting Measurement!\n");
        for(int iteration = 0; iteration < ITERATIONS; iteration++) {

            printf("Starting iteration %d!\n", iteration);
            read_write_buffer[0] = (char) 0;
            MPI_Win_fence(0, win);
            //printf("xxx\n");

            auto start_time = TimeTools::now();
            for (int i = 0; i < REPETITIONS; i++) {
                read_write_buffer[0] = 1;
                MPI_Put(read_write_buffer, 1, MPI_BYTE, 1, 0, 1, MPI_BYTE, win);
                MPI_Win_fence(0, win);
                //MPI_Win_fence(0, win);
                //while (read_write_buffer[0] != 2);
                //printf("Received rw2!\n");
            }
            auto end_time = TimeTools::now();
            MPI_Win_fence(0, win);
            measured_times.push_back((end_time-start_time)/REPETITIONS);
            printf("Done with iteration %d!\n", iteration);

        }

        printf("Done with Measurement!\n");

        printf("---Fence+Put Latency: ---\n");
        BenchmarkTools::output_results(measured_times, output_path, parameters, "MPI_Put_Latency", 1, 1, REPETITIONS);
    }


    std::vector<Timestamp> measured_times;
    for(int iteration = 0; iteration < ITERATIONS; iteration++) {
        auto start_time = TimeTools::now();
        for (int i = 0; i < REPETITIONS; i++) {
            //MPI_Win_fence(0, win);
            MPI_Win_fence(0, win);
        }
        auto end_time = TimeTools::now();
        MPI_Win_fence(0, win);
        measured_times.push_back((end_time-start_time)/REPETITIONS);
    }
    if( rank == 0){
        printf("---Fence Latency: ---\n");
        BenchmarkTools::output_results(measured_times, output_path, parameters, "MPI_Put_Latency_Fence", 1, 1, REPETITIONS);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Free_mem(read_write_buffer);
    MPI_Free_mem(send_receive_buffer);
    MPI_Win_free(&win);
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