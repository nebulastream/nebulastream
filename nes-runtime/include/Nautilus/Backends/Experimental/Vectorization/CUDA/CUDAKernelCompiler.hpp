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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_CUDA_CUDAKERNELCOMPILER_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_CUDA_CUDAKERNELCOMPILER_HPP_

#include <Nautilus/Backends/Experimental/Vectorization/KernelCompiler.hpp>
#include <Nautilus/CodeGen/CPP/Function.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>

namespace NES::Nautilus::Backends::CUDA {

class CUDAKernelCompiler : public KernelCompiler {
public:
    struct Descriptor {
        std::string kernelFunctionName;
        std::string wrapperFunctionName;
        uint64_t inputSchemaSize;
        uint32_t threadsPerBlock;
    };

    /**
     * @brief Constructor.
     * @param descriptor the kernel compiler descriptor
     */
    explicit CUDAKernelCompiler(Descriptor descriptor);

private:
    std::shared_ptr<Nautilus::Tracing::ExecutionTrace> createTrace(const std::shared_ptr<Runtime::Execution::Operators::Operator>& nautilusOperator) override;

    std::shared_ptr<IR::IRGraph> createIR(const std::shared_ptr<Nautilus::Tracing::ExecutionTrace>& trace) override;

    std::unique_ptr<CodeGen::CodeGenerator> createCodeGenerator(const std::shared_ptr<IR::IRGraph>& irGraph) override;

    std::unique_ptr<KernelExecutable> createExecutable(std::unique_ptr<CodeGen::CodeGenerator> codeGenerator, const CompilationOptions& options) override;

    std::shared_ptr<CodeGen::CPP::Function> getCudaKernelWrapper(const IR::BasicBlockPtr& functionBasicBlock);

    std::shared_ptr<CodeGen::CPP::Function> cudaErrorCheck();

    std::shared_ptr<CodeGen::CPP::Function> getBuffer();

    Descriptor descriptor;
};

} // namespace NES::Nautilus::Backends::CUDA

#endif // NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_CUDA_CUDAKERNELCOMPILER_HPP_
