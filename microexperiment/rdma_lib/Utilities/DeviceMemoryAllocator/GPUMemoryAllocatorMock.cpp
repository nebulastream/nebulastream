//
// Created by Toso, Lorenzo on 2019-01-08.
//

#include "GPUMemoryAllocator.h"
#include "Debug.h"


void * GPUMemoryAllocator::allocate_on_device(size_t size) {
    TRACE("Method unavilable\n");
    return nullptr;
}


void GPUMemoryAllocator::delete_on_device(void* buffer) const {
    TRACE("Method unavilable\n");
    return;
}

void GPUMemoryAllocator::copy_to_device(void* local, void* device, size_t size) const {
    TRACE("Method unavilable\n");
    return;
}
void GPUMemoryAllocator::copy_from_device(void* local, void* device, size_t size) const {
    TRACE("Method unavilable\n");
    return;
}

void GPUMemoryAllocator::set_on_device(char value, void* device, size_t size) const {
    TRACE("Method unavilable\n");
    return;
}

void GPUMemoryAllocator::inc_on_device(void* device, size_t size) const {
    TRACE("Method unavilable\n");
    return;
}

void GPUMemoryAllocator::dec_on_device(void* device, size_t size) const {
    TRACE("Method unavilable\n");
    return;
}
void GPUMemoryAllocator::wait_for_value(char* memory, char expected_value) const {
    TRACE("Method unavilable\n");
    return;
}
void GPUMemoryAllocator::wait_for_value_and_reset(char* memory, char expected_value, char reset_value) const {
    TRACE("Method unavilable\n");
    return;
}

bool GPUMemoryAllocator::uses_gpu() const {
    return false;
}

/*
int DeviceMemoryAllocator::sum(void* device, size_t size) const {
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