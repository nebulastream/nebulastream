//
// Created by Toso, Lorenzo on 2018-11-21.
//


#include <mpi.h>
#include <sys/time.h>
#include <infinity/infinity.h>
#include "MPIHelper.h"
#include <math.h>
#include <unistd.h>
#include <omp.h>
#include <iostream>
#include "TimeTools.hpp"
#include "BenchmarkTools.h"
#include "../ComputationParameters.h"

#define SERVER_IP "192.168.5.20"
#define PORT 55355

//#define THREADING
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
            if (rank == 0) {
                printf("BUFFER_SIZE: %lu\n", BUFFER_SIZE);
                printf("NUMBER_OF_BUFFERS: %lu\n", NUMBER_OF_BUFFERS);
            }

            auto read_write_buffer = new infinity::memory::Buffer(context, BUFFER_SIZE);
            auto send_receive_buffer = new infinity::memory::Buffer(context, sizeof(infinity::memory::RegionToken));
            infinity::memory::RegionToken* buffer_token;

            if (rank == 0) {
                context->postReceiveBuffer(send_receive_buffer);
            } else {
                buffer_token = read_write_buffer->createRegionToken();
                memcpy(send_receive_buffer->getData(), buffer_token, sizeof(infinity::memory::RegionToken));
            }

            MPI_Barrier(MPI_COMM_WORLD);

            if (rank != 0) {
                qp->send(send_receive_buffer, context->defaultRequestToken);
                context->defaultRequestToken->waitUntilCompleted();
                printf("Sent token\n");
            } else {
                infinity::core::receive_element_t receiveElement;
                while (!context->receive(&receiveElement));
                printf("Received token\n");

                buffer_token = static_cast<infinity::memory::RegionToken*>(malloc(
                        sizeof(infinity::memory::RegionToken)));
                memcpy(buffer_token, send_receive_buffer->getData(), sizeof(infinity::memory::RegionToken));
                printf("Copied buffer_token\n");

                //context->postReceiveBuffer(receiveElement.buffer);
            }
            MPI_Barrier(MPI_COMM_WORLD);


            auto parameters = ComputationParameters::generate(false,
#ifdef THREADING
        true
#else
    false
#endif
);
            if (rank == 0) {

                infinity::requests::RequestToken requestToken(context);
                auto fence = infinity::queues::OperationFlags();
                fence.fenced = true;

                std::vector<Timestamp> measuredTimes;
                for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                    //MPI_Barrier(MPI_COMM_WORLD);

                    auto start_time = TimeTools::now();
                    for (int repetition = 0; repetition < REPETITIONS_PER_ITERATION; repetition++) {

#ifdef THREADING
    #pragma omp parallel for num_threads(10)
#endif
                        for (int j = 0; j < NUMBER_OF_BUFFERS; j++) {

                            if (j != NUMBER_OF_BUFFERS - 1) {
                                qp->write(read_write_buffer, j * bytes_per_buffer, buffer_token, j * bytes_per_buffer,
                                          bytes_per_buffer, infinity::queues::OperationFlags());
                            } else {
                                qp->write(read_write_buffer, j * bytes_per_buffer, buffer_token, j * bytes_per_buffer,
                                          bytes_per_buffer, fence, &requestToken);
                                requestToken.waitUntilCompleted();
                            }
                        }
                    }
                    auto end_time = TimeTools::now();
                    auto time_in_ns = (end_time - start_time) / REPETITIONS_PER_ITERATION;
                    measuredTimes.push_back(time_in_ns);
                    //MPI_Barrier(MPI_COMM_WORLD);
                }
                printf("Done with measurement\n");


                BenchmarkTools::output_results(measuredTimes, output_path, parameters, "Verbs_Write_Bandwidth", BUFFER_SIZE,
                               NUMBER_OF_BUFFERS, REPETITIONS_PER_ITERATION);
            }
            MPI_Barrier(MPI_COMM_WORLD);
            delete buffer_token;
            delete read_write_buffer;
            delete send_receive_buffer;
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