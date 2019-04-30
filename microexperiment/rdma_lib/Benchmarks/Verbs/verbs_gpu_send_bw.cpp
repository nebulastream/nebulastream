#include <sys/time.h>
#include <infinity/infinity.h>
#include <math.h>
#include <unistd.h>
#include <iostream>
#include "../Utilities/TimeTools.hpp"
#include "../Utilities/BenchmarkTools.h"
#include "../ComputationParameters.h"
#include "VerbsConnection.h"
#include "ConnectionInfoProvider/SimpleInfoProvider.h"
#include "Debug.h"
#include "MPIHelper.h"
#include "DeviceMemoryAllocator/DeviceMemoryAllocator.h"
#include <DeviceMemoryAllocator/GPUMemoryAllocator.h>
#include <DeviceMemoryAllocator/CPUMemoryAllocator.h>
#include "NumaUtilities.h"
#include "ConnectionInfoProvider/ConnectionUtilities.h"

//#define SERVER_IP "192.168.5.30"
#define PORT 55355
DeviceMemoryAllocator * allocator;


void run_test(const std::string &output_path, size_t local_ib_index, size_t remote_ib_index) {
    printf("Started test routine\n");
    size_t rank = MPIHelper::get_rank();
    size_t target_rank = rank == 0 ? 1 : 0;

    auto hostname = MPIHelper::get_host_name();
    if (hostname[hostname.size()-1] == '3'){
        hostname[hostname.size()-1] = '2';
    } else {
        hostname[hostname.size()-1] = '3';
    }
    auto remote_ip = ConnectionUtilities::get_ip(hostname, remote_ib_index, 0);

    SimpleInfoProvider info(target_rank, static_cast<u_int16_t>(local_ib_index), 1, PORT, remote_ip);
    VerbsConnection connection(&info);
    printf("Connection established.\n");

    size_t REPETITIONS_PER_ITERATION;
    for (auto NUMBER_OF_BUFFERS : BenchmarkTools::BUFFER_COUNTS) {
        for (auto BUFFER_SIZE : BenchmarkTools::BUFFER_SIZES) {
            if (BUFFER_SIZE > 1 * BenchmarkTools::MEGABYTE)
                REPETITIONS_PER_ITERATION = BenchmarkTools::REPETITIONS_FOR_LARGE;
            else
                REPETITIONS_PER_ITERATION = BenchmarkTools::REPETITIONS_FOR_SMALL;

            size_t bytes_per_buffer = BUFFER_SIZE / NUMBER_OF_BUFFERS;
            if(bytes_per_buffer < 1)
                continue;

            connection.barrier();

            printf("BUFFER_SIZE: %lu\n", BUFFER_SIZE);
            printf("NUMBER_OF_BUFFERS: %lu\n", NUMBER_OF_BUFFERS);


            std::vector<char*> local_buffers(NUMBER_OF_BUFFERS);
            std::vector<infinity::memory::Buffer*> registered_buffers(NUMBER_OF_BUFFERS);

            for(size_t i = 0; i < NUMBER_OF_BUFFERS; i++){
                local_buffers[i] = (char*)allocator->allocate_on_device(bytes_per_buffer);
                registered_buffers[i] = connection.register_buffer(local_buffers[i], bytes_per_buffer);
            }

            TRACE("Registered all buffers\n");

            std::vector<infinity::requests::RequestToken*> requests(NUMBER_OF_BUFFERS);
            infinity::core::receive_element_t receiveElement;

            if ( rank == 0 ) {
                for (size_t i = 0; i < NUMBER_OF_BUFFERS; ++i) {
                    TRACE("Posting Receive\n");
                    connection.post_receive(registered_buffers[i]);
                    TRACE("Posted Receive of size %lu\n", bytes_per_buffer);
                }
            }
            else {
                for(size_t j=0; j < NUMBER_OF_BUFFERS; j++) {
                    requests[j] = connection.create_request_token();
                }
            }

            std::vector<Timestamp> measuredTimes;
            for (size_t iteration = 0; iteration < BenchmarkTools::ITERATIONS; iteration++) {
                //TRACE("Starting iteration %d\n", iteration);
                if(rank != 0)
                    usleep(1000*50);

                connection.barrier();

                auto start_time = TimeTools::now();
                for (size_t repetition = 0; repetition < REPETITIONS_PER_ITERATION; repetition++) {
                    if(rank == 0) {
                        for (size_t j = 0; j < NUMBER_OF_BUFFERS; j++) {
                            //TRACE("Waiting for receive\n");
                            connection.wait_for_receive(receiveElement);
                            connection.post_receive(receiveElement.buffer);
                            //TRACE("Received.\n");
                        }
                    }
                    else {
                        for (size_t j = 0; j < NUMBER_OF_BUFFERS; j++) {
                            //TRACE("Posting send of size %lu\n", bytes_per_buffer);
                            connection.send(registered_buffers[j], requests[j]);
                        }
                        for (size_t j = 0; j < NUMBER_OF_BUFFERS; j++) {
                            requests[j]->waitUntilCompleted();
                        }
                    }
                }
                auto end_time = TimeTools::now();
                auto time_in_ns = (end_time - start_time) / REPETITIONS_PER_ITERATION;
                measuredTimes.push_back(time_in_ns);
                connection.barrier();
            }
            printf("Done with measurement\n");

            // Invalidate buffer
            printf("Invalidate last buffers\n");
            for (size_t j = 0; j < NUMBER_OF_BUFFERS; j++) {
                if ( rank == 0)
                    connection.wait_for_receive(receiveElement);
                else {
                    connection.send(registered_buffers[j], requests[0]);
                    requests[0]->waitUntilCompleted();
                }
            }
            TRACE("Buffers invalidated\n");

            NodeParameters nodeParameters("", allocator->uses_gpu(), false);
            auto parameters = ComputationParameters::generate(connection, nodeParameters);

            BenchmarkTools::output_results(measuredTimes, output_path, parameters, "Verbs_GPU_BW", BUFFER_SIZE, NUMBER_OF_BUFFERS, REPETITIONS_PER_ITERATION);

            TRACE("Results written\n");

            for (size_t i = 0; i < NUMBER_OF_BUFFERS; ++i) {
                delete registered_buffers[i];
                allocator->delete_on_device(local_buffers[i]);
            }
            if( rank == 0){
            }
            else {
                for (size_t i = 0; i < NUMBER_OF_BUFFERS; ++i) {
                    delete requests[i];
                }
            }
            TRACE("Freed memory\n");
        }
    }
}

int main(int argc, char** argv) {
    if (argc < 8) {
        std::cerr << "Missing parameter!" <<
                  std::endl;
        std::cerr << "Usage: " << argv[0] << "<CPU/GPU> <localNUMA> <localIB_Index> <remoteIB_Index> <RANK> <PROCESS_COUNT> <Output path>" <<
                  std::endl;
        return -1;
    }

    size_t local_numa = static_cast<size_t>( std::stoi( argv[2] ));
    if(std::string(argv[1]) == "GPU" || std::string(argv[1]) == "gpu" ){
        allocator = new GPUMemoryAllocator();
    } else {
        allocator = new CPUMemoryAllocator(local_numa);
    }
    size_t local_ib_index = static_cast<size_t>( std::stoi( argv[3] ));
    size_t remote_ib_index = static_cast<size_t>( std::stoi( argv[4] ));
    size_t rank = static_cast<size_t>( std::stoi( argv[5] ));
    size_t process_count = static_cast<size_t>( std::stoi( argv[6] ));

    MPIHelper::set_rank(rank);
    MPIHelper::set_process_count(process_count);
    std::string result_path = argv[7];
    pin_to_numa(local_numa);

    run_test(result_path, local_ib_index, remote_ib_index);

    delete allocator;
}