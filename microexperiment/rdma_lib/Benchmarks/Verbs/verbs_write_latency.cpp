//
// Created by Toso, Lorenzo on 2018-11-26.
//
#include <mpi.h>
#include <sys/time.h>
#include <infinity/core/Context.h>
#include <infinity/queues/QueuePairFactory.h>
#include <infinity/queues/QueuePair.h>
#include <infinity/memory/Buffer.h>
#include <infinity/memory/RegionToken.h>
#include <infinity/requests/RequestToken.h>
#include <iostream>

#include "MPIHelper.h"
#include <math.h>
#include <unistd.h>
#include "TimeTools.hpp"
#include "BenchmarkTools.h"
#include "../ComputationParameters.h"

#define SERVER_IP "192.168.5.30"
#define PORT 55355

size_t ITERATIONS = 100;
size_t REPETITIONS = 10000;

void run_test(const std::string &output_path) {
    int rank = MPIHelper::get_rank();
    auto parameters = ComputationParameters::generate(false, false);

    auto context = new infinity::core::Context();
    auto* qpFactory = new infinity::queues::QueuePairFactory(context);
    infinity::queues::QueuePair* qp;


    auto read_write_buffer = new infinity::memory::Buffer(context, 1);
    auto send_receive_buffer = new infinity::memory::Buffer(context, 128);
    auto local_buffer_token = read_write_buffer->createRegionToken();

    if(rank != 0){

        qpFactory->bindToPort(PORT);
        printf("acceptIncomingConnection.\n");
        qp = qpFactory->acceptIncomingConnection(local_buffer_token, sizeof(infinity::memory::RegionToken));
        printf("Connected 1!.\n");

        context->postReceiveBuffer(send_receive_buffer);

        infinity::core::receive_element_t receiveElement;
        while(!context->receive(&receiveElement));
        printf("Received!\n");
        auto remoteBufferToken = (infinity::memory::RegionToken *) receiveElement.buffer->getData();
        printf("Received remote token!\n");

        infinity::requests::RequestToken requestToken(context);

        for(int iteration = 0; iteration < ITERATIONS; iteration++) {

            printf("Starting iteration %d!\n", iteration);
            ((char*) read_write_buffer->getData())[0] = (char) 0;
            MPI_Barrier(MPI_COMM_WORLD);

            for (int i = 0; i < REPETITIONS; i++) {
                while (((char*) read_write_buffer->getData())[0] != 1);
                //printf("Received %d successfully.\n", 1);
                ((char*) read_write_buffer->getData())[0] = 2;
                qp->write(read_write_buffer, remoteBufferToken, 1, &requestToken);
                requestToken.waitUntilCompleted();
                //printf("Write %lu successful.\n", iteration*REPETITIONS + i);
                context->pollSendCompletionQueue();
            }
            printf("Done with iteration %d!\n", iteration);

        }
        printf("Done with Measurement!\n");

    } else {
        usleep(2000000);
        printf("Connecting to remote!.\n");
        qp = qpFactory->connectToRemoteHost(SERVER_IP, PORT);
        printf("Connected 1!.\n");

        auto remoteBufferToken = (infinity::memory::RegionToken *) qp->getUserData();

        memcpy(send_receive_buffer->getData(), local_buffer_token, sizeof(infinity::memory::RegionToken));
        infinity::requests::RequestToken requestToken(context);
        qp->send(send_receive_buffer, 128, &requestToken);
        requestToken.waitUntilCompleted();
        printf("Sent remote token!\n");


        std::vector<Timestamp> measured_times;
        printf("Starting Measurement!\n");
        for(int iteration = 0; iteration < ITERATIONS; iteration++) {

            printf("Starting iteration %d!\n", iteration);
            ((char*) read_write_buffer->getData())[0] = (char) 0;
            MPI_Barrier(MPI_COMM_WORLD);

            auto start_time = TimeTools::now();
            for (int i = 0; i < REPETITIONS; i++) {
                ((char*) read_write_buffer->getData())[0] = (char) 1;
                qp->write(read_write_buffer, remoteBufferToken, 1, &requestToken);
                requestToken.waitUntilCompleted();
                //printf("Write successful!.\n");
                while (((char*) read_write_buffer->getData())[0] != 2);
                //printf("Received %d successfully.\n", i+1);

            }
            auto end_time = TimeTools::now();
            measured_times.push_back(((end_time-start_time)/REPETITIONS)/2);
            printf("Done with iteration %d!\n", iteration);

        }
        printf("Done with Measurement!\n");

        BenchmarkTools::output_results(measured_times, output_path, parameters, "Verbs_Write_Latency", 1, 1, REPETITIONS);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    delete read_write_buffer;
    delete send_receive_buffer;
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