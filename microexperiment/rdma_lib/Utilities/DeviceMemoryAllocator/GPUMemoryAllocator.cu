//
// Created by Toso, Lorenzo on 2019-01-08.
//

#include "GPUMemoryAllocator.h"
#include <cuda.h>
#include <cuda_runtime_api.h>
#include <cstdio>
#include "Debug.h"
#include <chrono>

void * GPUMemoryAllocator::allocate_on_device(size_t size) {
    TRACE("Allocating buffer of size %lu on GPU.\n", size);
    void* local_buffer;
    if( cudaMalloc(&local_buffer,size) != cudaSuccess){
        fprintf(stderr, "ERROR ALLOCATING CUDA-MEMORY!!!\n");
    }

    return local_buffer;
}


void GPUMemoryAllocator::delete_on_device(void* buffer) const {
    TRACE("Allocating buffer on GPU.\n");
    cudaFree(buffer);
}

void GPUMemoryAllocator::copy_to_device(void* local, void* device, size_t size) const {
    TRACE("Copying memory to GPU.\n");
    cudaMemcpy(device, local, size, cudaMemcpyHostToDevice);
}
void GPUMemoryAllocator::copy_from_device(void* local, void* device, size_t size) const {
    TRACE("Copying memory from GPU.\n");
    cudaMemcpy(local, device, size, cudaMemcpyDeviceToHost);
}

void GPUMemoryAllocator::set_on_device(char value, void* device, size_t size) const {
    cudaMemset(device, value, size);
}

void GPUMemoryAllocator::inc_on_device(void* device, size_t size) const {
    char * temp = static_cast<char*>(malloc(size));
    copy_from_device(temp, device, size);
    for (size_t i = 0; i < size; i++){
        temp[i]++;
    }
    copy_to_device(temp, device, size);
    free(temp);
}

void GPUMemoryAllocator::dec_on_device(void* device, size_t size) const {
    char * temp = static_cast<char*>(malloc(size));
    copy_from_device(temp, device, size);
    for (size_t i = 0; i < size; i++){
        temp[i]--;
    }
    copy_to_device(temp, device, size);
    free(temp);
}


__global__
void gpu_wait_for_value(volatile char* memory, volatile char expected_value){
    while(memory[0] != expected_value);
}
__global__
void gpu_wait_for_value_and_reset(volatile char* memory, volatile char expected_value, volatile char reset_value){
    while(memory[0] != expected_value);
    memory[0] = reset_value;
}


void GPUMemoryAllocator::wait_for_value(char* memory, char expected_value) const {
    gpu_wait_for_value<<<1,1>>>(memory, expected_value);
    cudaDeviceSynchronize();
}

void GPUMemoryAllocator::wait_for_value_and_reset(char* memory, char expected_value, char reset_value) const {
    gpu_wait_for_value_and_reset<<<1,1>>>(memory, expected_value, reset_value);
    cudaDeviceSynchronize();
}

void GPUMemoryAllocator::copy_on_device(void* dest, void* source, size_t size) {
    cudaMemcpy(dest, source, size, cudaMemcpyDeviceToDevice);
}

bool GPUMemoryAllocator::uses_gpu() const {
    return true;
}


__global__
void add(int size, char * ints, int * sum)
{
    int thread_index = threadIdx.x;
    int block_index = blockIdx.x;
    int block_size = blockDim.x;
    int unique_thread_index = thread_index + block_index * block_size;

    for (int stride = 1; stride <= size/2; stride *= 2) {
        int my_index = 2 * stride * unique_thread_index;

        if(my_index >= size || my_index + stride >= size)
            return;

        ints[my_index] = ints[my_index] + ints[my_index+stride];
    }
    if(thread_index == 0 && block_index == 0)
        *sum = ints[0];
}

/*
int GPUMemoryAllocator::sum(void* device, size_t size) {
    int * sum;
    cudaMalloc(&sum, sizeof(int));

    auto start = static_cast<size_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count());
    add<<<16, 1>>>((int)16, (char*)device, sum);
    cudaDeviceSynchronize();
    auto ending = static_cast<size_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count());

    int cpu_sum;
    cudaMemcpy(&cpu_sum, sum, sizeof(int), cudaMemcpyDeviceToHost);

    printf("Sum of %lu values took %lumus\n", size, (ending-start)/1000);
    return cpu_sum;
}
 */