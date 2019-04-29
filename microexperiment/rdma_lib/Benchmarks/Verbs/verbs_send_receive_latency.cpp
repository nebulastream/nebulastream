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

#include "MPIHelper.h"
#include <math.h>
#include <unistd.h>
#include <iostream>
#include "TimeTools.hpp"
#include "BenchmarkTools.h"
#include "../ComputationParameters.h"

#define SERVER_IP "192.168.5.30"
#define PORT 55355

size_t ITERATIONS = 100;
size_t REPETITIONS = 1000;

void run_test(const std::string &output_path) {

    int rank = MPIHelper::get_rank();
    auto context = new infinity::core::Context();
    auto* qpFactory = new infinity::queues::QueuePairFactory(context);
    infinity::queues::QueuePair* qp;


    auto receive_buffer = new infinity::memory::Buffer(context, 1);
    auto send_buffer = new infinity::memory::Buffer(context, 1);

    if(rank != 0) {

        qpFactory->bindToPort(PORT);
        printf("acceptIncomingConnection.\n");
        qp = qpFactory->acceptIncomingConnection();
    }
    else {
        usleep(2000000);
        qp = qpFactory->connectToRemoteHost(SERVER_IP, PORT);

    }
    MPI_Barrier(MPI_COMM_WORLD);


    std::vector<Timestamp> measured_times;
    printf("Starting Measurement!\n");
    for(int iteration = 0; iteration < ITERATIONS; iteration++) {

        //printf("Starting iteration %d!\n", iteration);
        infinity::core::receive_element_t receiveElement;
        infinity::requests::RequestToken requestToken(context);

        MPI_Barrier(MPI_COMM_WORLD);

        if(rank != 0)
            ((char*)send_buffer->getData())[0] = 2;
        else
            ((char*)send_buffer->getData())[0] = 1;

        for( int j = 0; j < 1000; j++)
            context->postReceiveBuffer(receive_buffer);

        MPI_Barrier(MPI_COMM_WORLD);
        auto start_time = TimeTools::now();

        for (int i = 0; i < REPETITIONS; i++) {
            ((char*)receive_buffer->getData())[0] = 0;

            if(rank == 0) {
                qp->send(send_buffer, 1, &requestToken);
                requestToken.waitUntilCompleted();

                while(!context->receive(&receiveElement));

                //if(((char*)receive_buffer->getData())[0] != 2)
                //    printf("error in repetition %lu\n", iteration*REPETITIONS + i);
            }
            else {
                while(!context->receive(&receiveElement));

                //if(((char*)receive_buffer->getData())[0] != 1)
                //    printf("error in repetition %lu\n", iteration*REPETITIONS + i);

                qp->send(send_buffer, 1, &requestToken);
                requestToken.waitUntilCompleted();
            }
        }

        auto end_time = TimeTools::now();
        measured_times.push_back(((end_time-start_time)/REPETITIONS)/2);
        //printf("Done with iteration %d!\n", iteration);

    }
    printf("Done with Measurement!\n");
    auto parameters = ComputationParameters::generate(false, false);

    if(rank == 0){
        BenchmarkTools::output_results(measured_times, output_path, parameters, "Verbs_Send_Receive_Latency", 1, 1, REPETITIONS);
    }


    delete send_buffer;
    delete receive_buffer;
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