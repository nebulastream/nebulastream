//
// Created by Toso, Lorenzo on 2019-01-10.
//

#include <sys/time.h>
#include <iostream>

#include <math.h>
#include <unistd.h>
#include <random>
#include <cuda_runtime_api.h>
//#include <MPIHelper.h>
#include "TimeTools.hpp"
#include "BenchmarkTools.h"
//#include "../ComputationParameters.h"



//#define UP
#define ANNOYING_BUFFER


size_t ITERATIONS = 20;
size_t REPETITIONS = 5;



size_t MIN_BUFFER_SIZE = 512;
size_t MAX_BUFFER_SIZE = 1024*1024*1024;

//size_t BUFFER_SIZE = 1024*1024*1024*(size_t)2;


size_t STREAM_COUNT = 1;

void run_test(const std::string &output_path) {

    for(size_t BUFFER_SIZE = MIN_BUFFER_SIZE; BUFFER_SIZE <= MAX_BUFFER_SIZE; BUFFER_SIZE*=2)
    {
        printf("Buffer size: %lu bytes (%.2f GB)\n", BUFFER_SIZE, BUFFER_SIZE/1024.0/1024.0/1024.0);

        size_t ANNOYING_BUFFER_SIZE = BUFFER_SIZE * (size_t)REPETITIONS;

        /*
        std::vector<void*> src_host_buffers(STREAM_COUNT);
        std::vector<void*> dst_host_buffers(STREAM_COUNT);
        std::vector<void*> device_buffers(STREAM_COUNT);
        std::vector<cudaStream_t> streams(STREAM_COUNT);
        std::vector<cudaEvent_t> events_start(STREAM_COUNT);
        std::vector<cudaEvent_t> events_end(STREAM_COUNT);
        std::vector<float> timings(STREAM_COUNT);

        for(size_t i=0; i<STREAM_COUNT; i++){
            src_host_buffers[i] = malloc(BUFFER_SIZE);
            cudaHostRegister(src_host_buffers[i], BUFFER_SIZE, cudaHostRegisterDefault);
            dst_host_buffers[i] = malloc(BUFFER_SIZE);
            cudaHostRegister(dst_host_buffers[i], BUFFER_SIZE, cudaHostRegisterDefault);
            cudaMalloc(&device_buffers[i], BUFFER_SIZE);
            cudaStreamCreate(&streams[i]);
            cudaEventCreate(&events_start[i]);
            cudaEventCreate(&events_end[i]);
        }
        */

#ifdef ANNOYING_BUFFER
        void * src_annoying_buffer;
        cudaMallocHost(&src_annoying_buffer, ANNOYING_BUFFER_SIZE);
        void * annoying_device_buffer;
        cudaMalloc(&annoying_device_buffer, ANNOYING_BUFFER_SIZE);
        cudaStream_t annoyingStream;
        cudaStreamCreate(&annoyingStream);
#endif

        void * src_host_buffer;
        cudaMallocHost(&src_host_buffer, BUFFER_SIZE);
        void * device_buffer;
        cudaMalloc(&device_buffer, BUFFER_SIZE);
        cudaStream_t mainStream;
        cudaStreamCreate(&mainStream);

        //void * dst_host_buffer = malloc(BUFFER_SIZE);

        std::vector<Timestamp> measured_times;
        for(size_t iteration = 0; iteration < ITERATIONS; iteration++) {

#ifdef ANNOYING_BUFFER
#ifdef UP
            cudaMemcpyAsync(src_annoying_buffer, annoying_device_buffer, ANNOYING_BUFFER_SIZE, cudaMemcpyDeviceToHost, annoyingStream);
#else
            cudaMemcpyAsync(annoying_device_buffer, src_annoying_buffer, ANNOYING_BUFFER_SIZE, cudaMemcpyHostToDevice, annoyingStream);
#endif
#endif
            auto start = TimeTools::now();
            for(size_t repetition = 0; repetition < REPETITIONS; repetition++) {
#ifdef UP
                cudaMemcpyAsync(device_buffer, src_host_buffer, BUFFER_SIZE, cudaMemcpyHostToDevice, mainStream);
#else
                cudaMemcpyAsync(src_host_buffer, device_buffer, BUFFER_SIZE, cudaMemcpyDeviceToHost, mainStream);
#endif
                cudaStreamSynchronize(mainStream);
            }
            auto end = TimeTools::now();
            measured_times.push_back((end-start)/REPETITIONS);

#ifdef ANNOYING_BUFFER
            cudaDeviceSynchronize();
#endif
        }
        cudaStreamDestroy(mainStream);
        cudaFreeHost(src_host_buffer);
        cudaFree(device_buffer);

#ifdef ANNOYING_BUFFER
        cudaStreamDestroy(annoyingStream);
        cudaFreeHost(src_annoying_buffer);
        cudaFree(annoying_device_buffer);
#endif

        auto parameters = ComputationParameters::generate(NodeParameters("", false, false));
        BenchmarkTools::output_results(measured_times, output_path, parameters, "GPU_PCI_BW", BUFFER_SIZE, 1, REPETITIONS);
/*
        for(size_t i=0; i<STREAM_COUNT; i++){

            cudaHostUnregister(src_host_buffers[i]);
            cudaHostUnregister(dst_host_buffers[i]);
            free(src_host_buffers[i]);
            free(dst_host_buffers[i]);
            cudaFree(device_buffers[i]);
            cudaStreamDestroy(streams[i]);
            cudaEventDestroy(events_start[i]);
            cudaEventDestroy(events_end[i]);
        }*/
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

    //MPIHelper::set_process_count(1);
    //MPIHelper::set_rank(0);
    run_test(result_path);

    return 0;
}