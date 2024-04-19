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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_EXPERIMENTAL_VECTORIZATION_KERNEL_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_EXPERIMENTAL_VECTORIZATION_KERNEL_HPP_

#include <Execution/Operators/Experimental/Vectorization/VectorizableOperator.hpp>

#include <Nautilus/Interface/DataTypes/BuiltIns/BuiltInVariable.hpp>
#include <Nautilus/Backends/Experimental/Vectorization/KernelExecutable.hpp>
#include <Nautilus/Util/CompilationOptions.hpp>

namespace NES::Runtime::Execution::Operators {

class NonKeyedSlicePreAggregationHandler;

/**
 * @brief The Kernel operator is a Nautilus operator that upon calling its `execute` method executes a kernel function.
 * The kernel function is compiled into a shared library object and exposed by a wrapper function.
 */
class Kernel : public VectorizableOperator {
public:
    struct Descriptor {
        std::shared_ptr<Operator> pipeline;
        Nautilus::CompilationOptions compileOptions;
        uint64_t inputSchemaSize;
        uint32_t threadsPerBlock;
        std::shared_ptr<Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler> handler;
    };

    /**
     * @brief Constructor.
     * @param descriptor the kernel descriptor
     */
    explicit Kernel(Descriptor descriptor);

    void setup(ExecutionContext& ctx) const override;
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

    static Value<> getCompilerBuiltInVariable(const std::shared_ptr<NES::Nautilus::BuiltInVariable>& builtInVariable);

  private:
    Descriptor descriptor;
    mutable std::unique_ptr<Backends::KernelExecutable> kernelExecutable;
};

} // namespace NES::Runtime::Execution::Operators

#endif // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_EXPERIMENTAL_VECTORIZATION_KERNEL_HPP_
