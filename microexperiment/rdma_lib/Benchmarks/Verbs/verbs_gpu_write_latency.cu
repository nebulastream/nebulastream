//
// Created by Toso, Lorenzo on 2019-01-09.
//
#include <sys/time.h>

#include "MPIHelper.h"
#include <math.h>
#include <unistd.h>
#include <iostream>
#include <NumaUtilities.h>
#include <cuda_runtime.h>
#include "TimeTools.hpp"
#include "BenchmarkTools.h"
#include "../ComputationParameters.h"
#include "ConnectionInfoProvider/SimpleInfoProvider.h"
#include "VerbsConnection.h"
#include "DeviceMemoryAllocator/DeviceMemoryAllocator.h"
#include "DeviceMemoryAllocator/CPUMemoryAllocator.h"
#include "DeviceMemoryAllocator/GPUMemoryAllocator.h"
#include "Debug.h"

#define SERVER_IP "192.168.5.30"
#define PORT 55355

//#define GPU
#define WRITE
//#define READ
//#define SEND

size_t BUFFER_SIZE = 1;
size_t ITERATIONS = 100;
size_t REPETITIONS = 1000;


#ifdef GPU
__global__
void gpu_wait_for_value_and_reset(volatile char* memory, volatile char expected_value, volatile char reset_value){
    while(memory[0] != expected_value);
    memory[0] = reset_value;
}
#endif


void run_test(const std::string &output_path, size_t numa_node) {
    size_t rank = MPIHelper::get_rank();
    size_t target_rank = rank == 0 ? 1 : 0;

    SimpleInfoProvider info(target_rank, 3-static_cast<u_int16_t>(numa_node * 3), 1, PORT, SERVER_IP);
    VerbsConnection connection(&info);

#ifdef GPU
    char * receive_memory;
    char * send_memory;
    cudaMalloc(&receive_memory, BUFFER_SIZE);
    cudaMalloc(&send_memory, BUFFER_SIZE);
    cudaMemset(send_memory, static_cast<char>(rank+1), BUFFER_SIZE);
    cudaMemset(receive_memory, 0, BUFFER_SIZE);
#else
    char * receive_memory = static_cast<char*>(malloc(BUFFER_SIZE));
    char * send_memory = static_cast<char*>(malloc(BUFFER_SIZE));
    memset(receive_memory,0,BUFFER_SIZE);
#endif
    auto receive_buffer = connection.register_buffer(receive_memory, BUFFER_SIZE);
    auto send_buffer = connection.register_buffer(send_memory, BUFFER_SIZE);

    auto remote_receive_token = connection.exchange_region_tokens(receive_buffer);
    RequestToken* pRequestToken = connection.create_request_token();

    connection.barrier();

    std::vector<Timestamp> measured_times;
    printf("Starting Measurement!\n");
    for(size_t iteration = 0; iteration < ITERATIONS; iteration++)
    {
        TRACE("Iteration: %d\n", iteration);


#ifdef SEND
        ReceiveElement receiveElement;
        connection.barrier();
        for( size_t j = 0; j < REPETITIONS; j++)
            connection.post_receive(receive_buffer);
#endif
#ifdef READ
        *send_memory = (char)0;
        *receive_memory = (char)1;
#endif

        connection.barrier();
        auto start_time = TimeTools::now();

            for (size_t i = 0; i < REPETITIONS; i++) {
                if(rank == 0) {

#ifdef WRITE
                    *send_memory = (char)1;
                    connection.write(send_buffer, remote_receive_token.get());
                    while(((volatile char*)receive_memory)[0] != (char)1){}
                    receive_memory[0] = 0;
#endif
#ifdef READ
                    connection.read(send_buffer, remote_receive_token.get(), pRequestToken);
                    while(((volatile char*)send_memory)[0] != (char)1){}
                    send_memory[0] = 0;
#endif
#ifdef SEND
                    connection.send(send_buffer);
                    connection.wait_for_receive(receiveElement);
#endif
                }
                else {
#ifdef WRITE
                    *send_memory = (char)1;
                    while(((volatile char*)receive_memory)[0] != (char)1){}
                    receive_memory[0] = 0;
                    connection.write(send_buffer, remote_receive_token.get());
#endif
#ifdef SEND
                    connection.wait_for_receive(receiveElement);
                    connection.send(send_buffer);
#endif

                }
            }

            if(rank == 0) {
            }
            else {

#ifdef WRITE
#ifdef GPU
                gpu_wait_for_value_and_reset<<<1,1>>>(receive_memory, REPETITIONS-1, 0);
                cudaDeviceSynchronize();
#else
                //while(*((volatile char*)receive_memory) != REPETITIONS-1){}
#endif
#endif
            }

        auto end_time = TimeTools::now();

#ifdef READ
        measured_times.push_back(((end_time-start_time)/REPETITIONS));
#else
        measured_times.push_back(((end_time-start_time)/REPETITIONS/2));
#endif
        TRACE("Done with iteration %d!\n", iteration);

    }
    printf("Done with Measurement!\n");
    NodeParameters nodeParameters("",
#ifdef GPU
            true
#else
            false
#endif
            ,false);
    auto parameters = ComputationParameters::generate(connection, nodeParameters);
    BenchmarkTools::output_results(measured_times, output_path, parameters,"VerbsLatency", 1, 1, REPETITIONS);

    delete send_buffer;
    delete receive_buffer;

#ifdef GPU
    cudaFree(send_memory);
    cudaFree(receive_memory);
#else
    free(send_memory);
    free(receive_memory);
#endif
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "Missing parameter!" <<
                  std::endl;
        std::cerr << "Usage: " << argv[0] << "<NUMA_NODE> <RANK> <Output path>" <<
                  std::endl;
        return -1;
    }

    size_t numa_node = std::stoi( argv[1]);
    pin_to_numa(numa_node);

    size_t rank = static_cast<size_t>( std::stoi( argv[2] ));

    MPIHelper::set_rank(rank);
    MPIHelper::set_process_count(2);
    std::string result_path = argv[3];
    run_test(result_path, numa_node);
}