//
// Created by Toso, Lorenzo on 2018-11-26.
//
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
#include <NumaUtilities.h>
#include <ConnectionInfoProvider/ConnectionUtilities.h>
#include "TimeTools.hpp"
#include "BenchmarkTools.h"
#include "ComputationParameters.h"
#include "ConnectionInfoProvider/SimpleInfoProvider.h"
#include "VerbsConnection.h"
#include "DeviceMemoryAllocator/DeviceMemoryAllocator.h"
#include "DeviceMemoryAllocator/CPUMemoryAllocator.h"
//#include "DeviceMemoryAllocator/GPUMemoryAllocator.h"
#include "Debug.h"

#define PORT 55355

size_t ITERATIONS = 1000;
size_t REPETITIONS = 4000;
DeviceMemoryAllocator * allocator;


void run_test(const struct TestParameters & test_parameters) {
    printf("Started test routine\n");
    size_t rank = MPIHelper::get_rank();
    size_t target_rank = rank == 0 ? 1 : 0;

    auto hostname = MPIHelper::get_host_name();
    if (hostname[hostname.size()-1] == '3'){
        hostname[hostname.size()-1] = '2';
    } else {
        hostname[hostname.size()-1] = '3';
    }
    auto remote_ip = ConnectionUtilities::get_ip(hostname, test_parameters.remote_ib, 0);

    SimpleInfoProvider info(target_rank, static_cast<u_int16_t>(test_parameters.local_ib), 1, PORT, remote_ip);
    VerbsConnection connection(&info);
    printf("Connection established.\n");

    char * receive_memory = static_cast<char*>(allocator->allocate_on_device(1));
    char * send_memory = static_cast<char*>(allocator->allocate_on_device(1));
    auto receive_buffer = connection.register_buffer(receive_memory, 1);
    auto send_buffer = connection.register_buffer(send_memory, 1);

    connection.barrier();

    std::vector<Timestamp> measured_times;
    printf("Starting Measurement!\n");
    for(size_t iteration = 0; iteration < ITERATIONS; iteration++) {
        TRACE("Iteration: %lu\n", iteration);
        infinity::core::receive_element_t receiveElement;
        RequestToken * requestToken = connection.create_request_token();

        connection.barrier();

        for( size_t j = 0; j < REPETITIONS; j++)
            connection.post_receive(receive_buffer);

        connection.barrier();
        auto start_time = TimeTools::now();

        for (size_t i = 0; i < REPETITIONS; i++) {

            if(rank == 0) {
                connection.send_blocking(send_buffer, requestToken);
                connection.wait_for_receive(receiveElement);
            }
            else {
                connection.wait_for_receive(receiveElement);
                connection.send_blocking(send_buffer, requestToken);
            }
        }

        auto end_time = TimeTools::now();
        measured_times.push_back(((end_time-start_time)/REPETITIONS/2));
        TRACE("Done with iteration %lu!\n", iteration);
        delete requestToken;
    }
    printf("Done with Measurement!\n");
    NodeParameters nodeParameters("", allocator->uses_gpu(), false);
    auto parameters = ComputationParameters::generate(connection, nodeParameters);
    BenchmarkTools::output_results(measured_times, test_parameters, parameters, "GPUSendLatency_");

    allocator->delete_on_device(send_memory);
    allocator->delete_on_device(receive_memory);
    delete send_buffer;
    delete receive_buffer;
}

//void

int main(int argc, char** argv) {
    if (argc < 7) {
        std::cerr << "Missing parameter!" <<
                  std::endl;
        std::cerr << "Usage: " << argv[0] << "<CPU/GPU> <localNUMA> <localIB_Index> <remoteIB_Index> <RANK> <Output path>" <<
                  std::endl;
        return -1;
    }

    TestParameters parameters;
    parameters.local_numa = static_cast<size_t>( std::stoi( argv[2] ));
    parameters.local_ib = static_cast<size_t>( std::stoi( argv[3] ));
    parameters.remote_ib = static_cast<size_t>( std::stoi( argv[4] ));
    parameters.result_path = std::string(argv[6]);
    parameters.cpu_or_gpu = std::string(argv[1]);

    if(std::string(argv[1]) == "GPU" || std::string(argv[1]) == "gpu" ){
//        allocator = new GPUMemoryAllocator();
    } else {
        allocator = new CPUMemoryAllocator(parameters.local_numa);
    }

    size_t rank = static_cast<size_t>( std::stoi( argv[5] ));

    MPIHelper::set_rank(rank);
    MPIHelper::set_process_count(2);
    pin_to_numa(parameters.local_numa);


    run_test(parameters);


    delete allocator;
}
