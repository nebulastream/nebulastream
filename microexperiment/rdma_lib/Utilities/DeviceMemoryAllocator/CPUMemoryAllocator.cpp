//
// Created by Toso, Lorenzo on 2019-01-08.
//

#include "CPUMemoryAllocator.h"
#include <stdio.h>
#include <cstdlib>
#include <cstring>
#include "Debug.h"
#include "TimeTools.hpp"
#include <numa.h>

void * CPUMemoryAllocator::allocate_on_device(size_t size){
    TRACE("Allocating buffer on CPU.\n");
    auto ptr = numa_alloc_onnode(size, numa_node);
    if(size > 0)
        ((char*)ptr)[0] = 0;
    ptr_sizes[ptr] = size;//.insert({ptr, size});
    return ptr;
}

void CPUMemoryAllocator::delete_on_device(void* buffer) const {
    TRACE("Deleting buffer on CPU.\n");
    auto found = ptr_sizes.find(buffer);
    if (found == ptr_sizes.end())
        return;

    numa_free(buffer, found->second);
}

void CPUMemoryAllocator::copy_to_device(void* local, void* device, size_t size) const {
    TRACE("Copying to CPU buffer.\n");
    memcpy(device, local, size);
}
void CPUMemoryAllocator::copy_from_device(void* local, void* device, size_t size) const {
    TRACE("Copying from CPU buffer.\n");
    memcpy(local, device, size);
}

void CPUMemoryAllocator::set_on_device(char value, void* device, size_t size) const {
    for (size_t i = 0; i < size; i++){
        ((char*)device)[i] = value;
    }
}

void CPUMemoryAllocator::inc_on_device(void* device, size_t size) const {
    for (size_t i = 0; i < size; i++){
        ((char*)device)[i]++;
    }
}

void CPUMemoryAllocator::dec_on_device(void* device, size_t size) const {
    for (size_t i = 0; i < size; i++){
        ((char*)device)[i]--;
    }
}

void wait_for_value_volatile(const volatile char* memory, volatile char expected_value) {
    while(memory[0] != expected_value);
}

void CPUMemoryAllocator::wait_for_value(char* memory, char expected_value) const {
    return wait_for_value_volatile(memory, expected_value);
}
void CPUMemoryAllocator::wait_for_value_and_reset(char* memory, char expected_value, char reset_value) const {
    wait_for_value_volatile(memory, expected_value);
    memory[0] = reset_value;
}

CPUMemoryAllocator::CPUMemoryAllocator(uint16_t numa_node)
:numa_node(numa_node){

}


/*
int CPUMemoryAllocator::sum(void* device, size_t size) const {
    int sum = 0;
    auto start = TimeTools::now();

    for(size_t i = 0; i < size; i++){
        sum += ((char*)device)[i];
    }
    auto ending = TimeTools::now();
    printf("Sum of %lu values took %lumus\n", size, (ending-start)/1000);
    return sum;
}*/


