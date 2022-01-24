/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#ifndef NES_CUDAKERNELWRAPPER_HPP
#define NES_CUDAKERNELWRAPPER_HPP

#include <Util/jitify/jitify.hpp>
#include <cuda.h>
#include <cuda_runtime.h>
/**
 * @brief This class is a wrapper for executing CUDA kernel as part of ExecutablePipelineStage
 * @tparam InputRecord data type to be processed by the kernel
 * @tparam Size number of tuple to process by the kernel
 */
template<class InputRecord, int Size>
class CUDAKernelWrapper {
  public:
    CUDAKernelWrapper() = default;

    /**
     * @brief Allocate GPU memory to store the input and output of the kernel program. Must be called before kernel execution in
     * the execute() method.
     * @param kernelCode source code of the kernel
     */
    void setup(const char* const kernelCode) {
        cudaMalloc(&deviceInputBuffer, Size * sizeof(InputRecord));
        cudaMalloc(&deviceOutputBuffer, Size * sizeof(InputRecord));

        static jitify::JitCache kernelCache;
        kernelProgramPtr = std::make_shared<jitify::Program>(kernelCache.program(kernelCode, 0));
    }

    /**
     * @brief execute the kernel program
     * @param hostInputBuffer input buffer in the host memory. The kernel reads from this buffer and copy back the output to this buffer.
     */
    void execute(InputRecord* hostInputBuffer) {
        // copy the hostInputBuffer (input) to deviceInputBuffer
        cudaMemcpy(deviceInputBuffer, hostInputBuffer, Size * sizeof(InputRecord), cudaMemcpyHostToDevice);

        // prepare a kernel launch configuration
        dim3 grid(1);
        dim3 block(32);

        // execute the kernel program
        using jitify::reflection::type_of;
        kernelProgramPtr->kernel("simpleAdditionKernel")
            .instantiate()
            .configure(grid, block) // the configuration
            .launch(deviceInputBuffer, Size, deviceOutputBuffer); // the parameter of the kernel program

        // copy the result of kernel execution back to the gpu
        cudaMemcpy(hostInputBuffer, deviceOutputBuffer, Size * sizeof(InputRecord), cudaMemcpyDeviceToHost);
    }

    /**
     * @brief free the GPU memory
     */
    void clean() {
        cudaFree(deviceInputBuffer);
        cudaFree(deviceOutputBuffer);
    }

  private:
    InputRecord* deviceInputBuffer; // device memory for kernel input
    InputRecord* deviceOutputBuffer; // device memory for kernel output
    std::shared_ptr<jitify::Program> kernelProgramPtr;
};

#endif//NES_CUDAKERNELWRAPPER_HPP