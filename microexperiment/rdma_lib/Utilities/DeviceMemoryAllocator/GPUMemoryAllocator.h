//
// Created by Toso, Lorenzo on 2019-01-08.
//

#pragma once


#include <cstdio>
#include "DeviceMemoryAllocator.h"

class GPUMemoryAllocator : public DeviceMemoryAllocator {
public:
    void* allocate_on_device(size_t size) override;
    void  delete_on_device(void * buffer) const override;
    void  copy_to_device(void * local, void * device, size_t size) const override;
    void  copy_from_device(void * local, void * device, size_t size) const override;
    static void copy_on_device(void* dest, void* source, size_t size);
    void  set_on_device(char value, void * device, size_t size) const override;
    void  inc_on_device(void * device, size_t size) const override;
    void  dec_on_device(void * device, size_t size) const override;
    bool uses_gpu() const override;

    void wait_for_value(char* memory, char expected_value) const override;
    void wait_for_value_and_reset(char* memory, char expected_value, char reset_value) const override;

};



