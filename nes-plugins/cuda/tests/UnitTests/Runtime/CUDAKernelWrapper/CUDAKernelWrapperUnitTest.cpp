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

#include <BaseUnitTest.hpp>
#include <Runtime/GPURuntime/CUDAKernelWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>

namespace NES::Runtime::CUDAKernelWrapper {
class CUDAKernelWrapperUnitTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("CUDAKernelWrapperUnitTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup CUDAKernelWrapperUnitTest test class.");
    }

    void SetUp() override { Testing::BaseUnitTest::SetUp(); }
};

// Test cuda wrapper in compiling and executing a simple CUDA Kernel processing a single integer field
TEST_F(CUDAKernelWrapperUnitTest, CUDAKernelWrapperOnSimpleAddition__GPU) {
    // Prepare a simple CUDA kernel which adds 42 to the recordValue and then write it to the result
    const char* const SimpleKernel_cu =
        "SimpleKernel.cu\n"
        "__global__ void simpleAdditionKernel(const int* recordValue, const int count, int* result) {\n"
        "    auto i = blockIdx.x * blockDim.x + threadIdx.x;\n"
        "\n"
        "    if (i < count) {\n"
        "        result[i] = recordValue[i] + 42;\n"
        "    }\n"
        "}\n";

    uint32_t numberOfTuples = 10;

    CUDAKernelWrapper<int, int> cudaKernelWrapper;
    // set up the kernel program and allocate gpu buffer
    cudaKernelWrapper.setup(SimpleKernel_cu, numberOfTuples * sizeof(int));

    int* inputTuples = static_cast<int*>(std::malloc(numberOfTuples * sizeof(int)));
    int* outputTuples = static_cast<int*>(std::malloc(numberOfTuples * sizeof(int)));

    NES::Runtime::CUDAKernelWrapper::KernelDescriptor kernelDescriptor = {"simpleAdditionKernel", dim3(1), dim3(32)};
    cudaKernelWrapper.execute(inputTuples, numberOfTuples, outputTuples, numberOfTuples, kernelDescriptor);

    std::free(inputTuples);
    std::free(outputTuples);
}

}// namespace NES::Runtime::CUDAKernelWrapper
