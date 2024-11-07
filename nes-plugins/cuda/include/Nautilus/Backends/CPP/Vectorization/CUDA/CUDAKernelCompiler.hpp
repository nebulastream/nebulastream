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

#ifndef NES_PLUGINS_CUDA_INCLUDE_NAUTILUS_BACKENDS_CPP_VECTORIZATION_CUDA_CUDAKERNELCOMPILER_HPP_
#define NES_PLUGINS_CUDA_INCLUDE_NAUTILUS_BACKENDS_CPP_VECTORIZATION_CUDA_CUDAKERNELCOMPILER_HPP_

#include <Nautilus/Backends/CPP/Vectorization/KernelCompiler.hpp>
#include <Nautilus/Backends/CPP/Vectorization/KernelExecutable.hpp>
#include <Nautilus/CodeGen/CPP/Function.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>

namespace NES::Nautilus::Backends::CPP::Vectorization::CUDA {

/*
 * @brief A class that wrap cuda kernel code with necessary boiler plat to perform data transfer and kernel invocation
 */
class CUDAKernelCompiler : public KernelCompiler {
  public:
    struct Descriptor {
        std::string kernelFunctionName;
        std::string wrapperFunctionName;
        uint64_t inputSchemaSize;
        uint32_t threadsPerBlock;
        uint64_t sharedMemorySize;
        uint64_t numberOfSlices;
        uint64_t sliceSize;
    };

    /**
     * @brief Constructor.
     * @param descriptor the kernel compiler descriptor
     */
    explicit CUDAKernelCompiler(Descriptor descriptor);

  private:
    std::shared_ptr<Nautilus::Tracing::ExecutionTrace>
    createTrace(const std::shared_ptr<Runtime::Execution::Operators::Operator>& nautilusOperator) override;

    std::shared_ptr<IR::IRGraph> createIR(const std::shared_ptr<Nautilus::Tracing::ExecutionTrace>& trace) override;

    std::unique_ptr<CodeGen::CodeGenerator> createCodeGenerator(const std::shared_ptr<IR::IRGraph>& irGraph) override;

    std::unique_ptr<KernelExecutable> createExecutable(std::unique_ptr<CodeGen::CodeGenerator> codeGenerator,
                                                       const CompilationOptions& options) override;

    /**
     * @brief Generates a function that call the CUDAKernelWrapper to wrap kernel invocation.
     * @return A shared pointer to generated cuda kernel wrapper function.
     */
    std::shared_ptr<CodeGen::CPP::Function> getCudaKernelWrapper(const IR::BasicBlockPtr& functionBasicBlock);

    /**
     * @brief Generates a functions to run checks against CUDA-related error when calling CUDA API.
     * @return A shared pointer to the generated cuda error check function.
     */
    std::shared_ptr<CodeGen::CPP::Function> cudaErrorCheck();

    /**
     * @brief Generates a function to get the tuple buffer to work on.
     * @return A shared pointer to the generated get buffer function.
     */
    std::shared_ptr<CodeGen::CPP::Function> getBuffer();

    /**
     * @brief Generates a function to retrieve the number of tuple in the tuple buffer.
     * @return A shared pointer to the get number of tuple function.
     */
    std::shared_ptr<CodeGen::CPP::Function> getNumberOfTuples();

    /**
     * @brief Generates a function to obtain the creation timestamp of the tuple buffer.
     * @return A shared pointer to the get creationts function.
     */
    std::shared_ptr<CodeGen::CPP::Function> getCreationTs();

    /**
     * @brief Generates a function to get the pointer to the slice store buffer.
     * @return A shared pointer to the get slice store function.
     */
    std::shared_ptr<CodeGen::CPP::Function> getSliceStore();

    /**
     * @brief Generates a pre-defined sum function.
     * @return A shared pointer to the sum function.
     */
    std::shared_ptr<CodeGen::CPP::Function> sum();

    /**
     * @brief Generates a function to set buffer metadata to valid.
     * @return A shared pointer to the setAsValidMetadata function.
     */
    std::shared_ptr<CodeGen::CPP::Function> setAsValidInMetadata();

    Descriptor descriptor;
    std::string getKernelInvocationCode(const std::string& inputBufferVar,
                                        const std::string& handlerVar,
                                        const std::string& workerIdVar,
                                        uint64_t inputSchemaSize,
                                        uint32_t threadsPerBlock,
                                        uint64_t sharedMemorySize,
                                        const std::string& kernelFunctionName,
                                        uint64_t numberOfSlices,
                                        uint64_t sliceSize) const;
};

}// namespace NES::Nautilus::Backends::CPP::Vectorization::CUDA

#endif// NES_PLUGINS_CUDA_INCLUDE_NAUTILUS_BACKENDS_CPP_VECTORIZATION_CUDA_CUDAKERNELCOMPILER_HPP_
