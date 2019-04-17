//
// Created by Toso, Lorenzo on 2019-01-08.
//

#pragma once

#include <cstdint>
#include <unordered_map>
#include "DeviceMemoryAllocator.h"

class CPUMemoryAllocator : public DeviceMemoryAllocator {
private:
    uint16_t numa_node;
    std::unordered_map<void*, size_t> ptr_sizes;
public:
    explicit CPUMemoryAllocator(uint16_t numa_node);

    void* allocate_on_device(size_t size) override;
    void  delete_on_device(void * buffer) const override;
    void  copy_to_device(void * local, void * device, size_t size) const override;
    void  copy_from_device(void * local, void * device, size_t size) const override;
    void  set_on_device(char value, void * device, size_t size) const override;
    void  inc_on_device(void * device, size_t size) const override;
    void  dec_on_device(void * device, size_t size) const override;

    void wait_for_value(char* memory, char expected_value) const override;
    void wait_for_value_and_reset(char* memory, char expected_value, char reset_value) const override;
};



