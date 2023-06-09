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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_VECTORIZE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_VECTORIZE_HPP_

#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Vectorization/VectorizableOperator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Vectorize operator that materializes incoming tuples until the stage buffer capacity is met.
 * Once the buffer is full, the child operator is invoked.
 */
class Vectorize : public ExecutableOperator {
public:
    /**
     * @brief Constructor.
     * @param operatorHandlerIndex the index of the handler of this operator in the pipeline execution context.
     * @param child the child operator which is derived from VectorizableOperator.
     */
    Vectorize(uint64_t operatorHandlerIndex, VectorizableOperatorPtr child);

    void execute(ExecutionContext& ctx, Record& record) const override;

private:
    uint64_t operatorHandlerIndex;
    VectorizableOperatorPtr child_;
};

} // namespace NES::Runtime::Execution::Operators

#endif // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_VECTORIZATION_VECTORIZE_HPP_
