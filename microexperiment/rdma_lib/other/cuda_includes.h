//
// Created by Toso, Lorenzo on 2019-03-11.
//

#pragma once
#ifndef MAMPI_CUDA_INCLUDES_H
#define MAMPI_CUDA_INCLUDES_H

#ifdef NO_CUDA

enum CUDA_MEMCPY{
    cudaMemcpyHostToDevice
};


namespace thrust{
    template <typename T>
    class device_vector{
    public:
        class const_iterator{};
    };
}

struct cudaStream_t{};

//void cudaMalloc(void**a, unsigned long long b){}
//void cudaMemcpy(void* dst, void* src, unsigned long long size, CUDA_MEMCPY type){}
//void cudaFree(void*c){}


#else

#include <thrust/device_vector.h>
#include <cuda_runtime_api.h>
#include <cuda.h>


#endif
#endif //MAMPI_CUDA_INCLUDES_H
