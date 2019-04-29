//
// Created by Toso, Lorenzo on 2018-11-21.
//


#include <mpi.h>
#include <sys/time.h>
#include <infinity/infinity.h>
#include "MPIHelper.h"
#include <math.h>
#include <unistd.h>
#include "TimeTools.hpp"
#include "BenchmarkTools.h"
#include "../ComputationParameters.h"
#include <iostream>

#define SERVER_IP "192.168.5.20"
#define PORT 55355


#define ITERATIONS 10
#define REPETITIONS_FOR_LARGE 100
#define REPETITIONS_FOR_SMALL 1000


void run_test(const std::string &output_path) {
    size_t rank = MPIHelper::get_rank();
    auto context = new infinity::core::Context();
    auto* qpFactory = new infinity::queues::QueuePairFactory(context);
    infinity::queues::QueuePair* qp;

    if (rank != 0) {
        usleep(2000000);
        qp = qpFactory->connectToRemoteHost(SERVER_IP, PORT);
        printf("Connected\n");
    } else {
        printf("Waiting for incoming connection\n");
        qpFactory->bindToPort(PORT);
        qp = qpFactory->acceptIncomingConnection();
    }
    MPI_Barrier(MPI_COMM_WORLD);


    size_t REPETITIONS_PER_ITERATION;
    for (auto NUMBER_OF_BUFFERS : BenchmarkTools::BUFFER_COUNTS) {
        for (auto BUFFER_SIZE : BenchmarkTools::BUFFER_SIZES) {
            if (BUFFER_SIZE > 1 * BenchmarkTools::MEGABYTE)
                REPETITIONS_PER_ITERATION = REPETITIONS_FOR_LARGE;
            else
                REPETITIONS_PER_ITERATION = REPETITIONS_FOR_SMALL;

            uint32_t bytes_per_buffer = static_cast<uint32_t>(BUFFER_SIZE / NUMBER_OF_BUFFERS);

            MPI_Barrier(MPI_COMM_WORLD);
            if(rank == 0){
                printf("BUFFER_SIZE: %lu\n", BUFFER_SIZE);
                printf("NUMBER_OF_BUFFERS: %lu\n", NUMBER_OF_BUFFERS);
            }

            char* local_buffer = new char[BUFFER_SIZE];


            infinity::memory::Buffer** receiveBuffers;
            infinity::memory::Buffer* sendBuffer;

            if(rank == 0){
                sendBuffer = new infinity::memory::Buffer(context, local_buffer, BUFFER_SIZE);
            } else {
                receiveBuffers = new infinity::memory::Buffer* [NUMBER_OF_BUFFERS];
                for (uint32_t i = 0; i < NUMBER_OF_BUFFERS; ++i) {
                    char* local_address = &local_buffer[i * bytes_per_buffer];
                    receiveBuffers[i] = new infinity::memory::Buffer(context, local_address, bytes_per_buffer);
                    context->postReceiveBuffer(receiveBuffers[i]);
                }
            }

            MPI_Barrier(MPI_COMM_WORLD);
            printf("First message\n");

            /*
            infinity::core::receive_element_t receiveElement;
            if (rank == 0) {
                qp->send(sendBuffer, 0, bytes_per_buffer, infinity::queues::OperationFlags(), context->defaultRequestToken);
                context->defaultRequestToken->waitUntilCompleted();
            } else {
                while (!context->receive(&receiveElement));
                context->postReceiveBuffer(receiveElement.buffer);
            }*/

            infinity::core::receive_element_t receiveElement;
            std::vector<infinity::requests::RequestToken*> requests(NUMBER_OF_BUFFERS);
            if(rank == 0){
                for(int j=0; j < NUMBER_OF_BUFFERS; j++) {
                    requests[j] = new infinity::requests::RequestToken(context);
                }
            }

            std::vector<Timestamp> measuredTimes;
            for (int iteration = 0; iteration < ITERATIONS; iteration++){
                MPI_Barrier(MPI_COMM_WORLD);

                auto start_time = TimeTools::now();
                for (int repetition = 0; repetition < REPETITIONS_PER_ITERATION; repetition++) {
                    for (int j = 0; j < NUMBER_OF_BUFFERS; j++) {
                        if(rank == 0){
                            qp->send(sendBuffer, j * bytes_per_buffer, bytes_per_buffer, infinity::queues::OperationFlags(), requests[j]);
                        } else {
                            while (!context->receive(&receiveElement));
                        }
                    }
                    for(int j = 0; j < NUMBER_OF_BUFFERS; j++) {
                        if(rank == 0)
                            requests[j]->waitUntilCompleted();
                        else
                            context->postReceiveBuffer(receiveBuffers[j]);
                    }
                }
                auto end_time = TimeTools::now();
                auto time_in_ns = (end_time - start_time) / REPETITIONS_PER_ITERATION;
                measuredTimes.push_back(time_in_ns);
                MPI_Barrier(MPI_COMM_WORLD);
            }
            printf("Done with measurement\n");

            // Invalidate buffer
            printf("Invalidate last buffers\n");
            for (int j = 0; j < NUMBER_OF_BUFFERS; j++) {
                if (rank == 0) {
                    infinity::requests::RequestToken requestToken(context);
                    qp->send(sendBuffer, j * bytes_per_buffer, bytes_per_buffer, infinity::queues::OperationFlags(), &requestToken);
                    requestToken.waitUntilCompleted();
                } else {
                    while (!context->receive(&receiveElement));
                }
            }



            auto parameters = ComputationParameters::generate(false, false);
            if(rank == 0){
                BenchmarkTools::output_results(measuredTimes, output_path, parameters, "Verbs_Send_Bandwidth", BUFFER_SIZE, NUMBER_OF_BUFFERS, REPETITIONS_PER_ITERATION);
            }

            if (rank != 0) {
                for (uint32_t i = 0; i < NUMBER_OF_BUFFERS; ++i) {
                    delete receiveBuffers[i];
                }
                delete receiveBuffers;
            } else {
                for (uint32_t i = 0; i < NUMBER_OF_BUFFERS; ++i) {
                    delete requests[i];
                }
                delete sendBuffer;
            }
            delete[] local_buffer;
        }
    }

    delete qp;
    delete qpFactory;
    delete context;
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