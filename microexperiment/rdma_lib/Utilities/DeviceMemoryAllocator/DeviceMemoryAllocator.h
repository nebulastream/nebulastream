//
// Created by Toso, Lorenzo on 2018-12-21.
//

#ifndef MAMPI_DEVICEMEMORYALLOCATOR_H
#define MAMPI_DEVICEMEMORYALLOCATOR_H

#include <cstddef>

class DeviceMemoryAllocator {
public:
    virtual ~DeviceMemoryAllocator() = default;

    virtual void* allocate_on_device(size_t size) = 0;
    virtual void  delete_on_device(void * buffer) const = 0;
    virtual void  copy_to_device(void * local, void * device, size_t size) const = 0;
    virtual void  copy_from_device(void * local, void * device, size_t size) const = 0;
    virtual void  set_on_device(char value, void * device, size_t size) const = 0;
    virtual void  inc_on_device(void * device, size_t size) const = 0;
    virtual void  dec_on_device(void * device, size_t size) const = 0;

    virtual bool uses_gpu() const { return false; };

    //static int sum(void * device, size_t size);
    virtual void wait_for_value(char* memory, char expected_value) const = 0;
    virtual void wait_for_value_and_reset(char* memory, char expected_value, char reset_value) const = 0;
};
#endif //MAMPI_DEVICEMEMORYALLOCATOR_H
