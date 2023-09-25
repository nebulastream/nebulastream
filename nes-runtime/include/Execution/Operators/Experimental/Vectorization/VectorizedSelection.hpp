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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_VECTORIZEDSELECTION_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_VECTORIZEDSELECTION_HPP_

#include <Execution/Operators/Experimental/Vectorization/VectorizableOperator.hpp>

#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Relational/Selection.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief The vectorized selection operator that applies the kernel programming model to a single-record selection operator.
 */
class VectorizedSelection : public VectorizableOperator {
public:
    /**
     * @brief Constructor.
     * @param expression the predicate expression
     * @param memoryProvider the memory provider
     * @param projections the projection vector
     */
    explicit VectorizedSelection(const Expressions::ExpressionPtr& expression,
        std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider,
        std::vector<Nautilus::Record::RecordFieldIdentifier> projections = {});

    void execute(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

private:
    Expressions::ExpressionPtr expression;
    std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider;
    std::vector<Nautilus::Record::RecordFieldIdentifier> projections;
};

} // namespace NES::Runtime::Execution::Operators

#endif // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_VECTORIZEDSELECTION_HPP_
