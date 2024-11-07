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

#ifndef NES_PLUGINS_CUDA_INCLUDE_NAUTILUS_BACKENDS_CPP_VECTORIZATION_KERNELCOMPILER_HPP_
#define NES_PLUGINS_CUDA_INCLUDE_NAUTILUS_BACKENDS_CPP_VECTORIZATION_KERNELCOMPILER_HPP_

#include <Nautilus/Backends/CPP/Vectorization/KernelExecutable.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Util/CompilationOptions.hpp>

namespace NES::Runtime::Execution::Operators {
class Operator;
}// namespace NES::Runtime::Execution::Operators

namespace NES::Nautilus::Tracing {
class ExecutionTrace;
}// namespace NES::Nautilus::Tracing

namespace NES::Nautilus::CodeGen {
class CodeGenerator;
}// namespace NES::Nautilus::CodeGen

namespace NES::Nautilus::IR {
class IRGraph;
}// namespace NES::Nautilus::IR

namespace NES::Nautilus::Backends::CPP::Vectorization {

/**
 * @brief The `KernelCompiler` class is responsible for creating the kernel executable of a vectorized Nautilus operator pipeline.
 */
class KernelCompiler {
  public:
    std::unique_ptr<KernelExecutable> compile(const std::shared_ptr<Runtime::Execution::Operators::Operator>& nautilusOperator,
                                              const CompilationOptions& options);

    virtual ~KernelCompiler() = default;

  private:
    virtual std::shared_ptr<Nautilus::Tracing::ExecutionTrace>
    createTrace(const std::shared_ptr<Runtime::Execution::Operators::Operator>& nautilusOperator) = 0;

    /**
     * @brief Creates a graph of intermediate representation (IR) from a given trace.
     * @param trace Nautilus trace of the code/operator.
     * @return A shared pointer of IRGraph, which represents the given trace.
     */
    virtual std::shared_ptr<IR::IRGraph> createIR(const std::shared_ptr<Nautilus::Tracing::ExecutionTrace>& trace) = 0;

    /**
     * @brief Creates a code generator instance from a given IR graph containing necessary variable and includes.
     * @param irGraph An intermediate representation, e.g., created by createIR() method.
     * @return A unique pointer of the code generator instance.
     */
    virtual std::unique_ptr<CodeGen::CodeGenerator> createCodeGenerator(const std::shared_ptr<IR::IRGraph>& irGraph) = 0;

    /**
     * @brief Dumps generated code to file, compiles it, and gets its function pointer.
     * @param codeGenerator an instance of code generator to get the code to compile.
     * @param options compilation options to be applied when compiling the generated code.
     * @return A function pointer to the compiled code.
     */
    virtual std::unique_ptr<KernelExecutable> createExecutable(std::unique_ptr<CodeGen::CodeGenerator> codeGenerator,
                                                               const CompilationOptions& options) = 0;
};

}// namespace NES::Nautilus::Backends::CPP::Vectorization

#endif// NES_PLUGINS_CUDA_INCLUDE_NAUTILUS_BACKENDS_CPP_VECTORIZATION_KERNELCOMPILER_HPP_
