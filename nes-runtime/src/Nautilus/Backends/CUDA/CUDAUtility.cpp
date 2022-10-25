/*
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

#include <Nautilus/Backends/CUDA/CUDAUtility.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/jitify/jitify.hpp>
#include <cuda_runtime.h>

#define NUMBER_OF_TUPLE 10

namespace NES::Nautilus::Backends::CUDA {

fptr CUDAUtility::compileNESIRToMachineCode(std::shared_ptr<NES::Nautilus::IR::IRGraph> ir) {
    NES_DEBUG("IR String: " << ir->toString());
    // TODO: generate kernel based on the IR

    return CUDAUtility::exec;
}

int CUDAUtility::exec() {
    // Prepare a simple CUDA kernel that perform a simple aggregation
    // This should be generated from the NautilusIR
    const char* const kernelCode = "SimpleAggregation.cu\n"
                                   "__global__ void simpleAgg(const int *records, const int count, int *result)  {\n"
                                   "    auto i = blockIdx.x * blockDim.x + threadIdx.x;\n"
                                   "\n"
                                   "    if (i < count) {\n"
                                   "        atomicAdd(result, records[i]);\n"
                                   "    }\n"
                                   "}\n";

    static jitify::JitCache kernelCache;
    std::shared_ptr<jitify::Program> kernelProgramPtr =
        std::make_shared<jitify::Program>(kernelCache.program(kernelCode, 0, 0, 0));

    int* res;
    int* records;

    cudaMallocHost(&res, sizeof(int));
    cudaMallocHost(&records, NUMBER_OF_TUPLE * sizeof(int));

    // initiate values for res and records
    res[0] = 1;

    for (int i = 0; i < NUMBER_OF_TUPLE; i++) {
        records[i] = 10;
    }

    // prepare launch configuration
    dim3 dimBlock(1024, 1, 1);
    dim3 dimGrid((NUMBER_OF_TUPLE + dimBlock.x - 1) / dimBlock.x, 1, 1);

    // execute the kernel program
    using jitify::reflection::type_of;
    kernelProgramPtr->kernel(std::move("simpleAgg"))
        .instantiate()
        .configure(dimGrid, dimBlock)
        .launch(records, NUMBER_OF_TUPLE, res);// the parameter of the kernel program

    cudaDeviceSynchronize();
    int result = res[0];

    // cleanup
    cudaFreeHost(records);
    cudaFreeHost(res);

    return result;
}

}// namespace NES::Nautilus::Backends::CUDA