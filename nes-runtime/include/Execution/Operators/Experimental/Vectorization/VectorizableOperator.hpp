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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_EXPERIMENTAL_VECTORIZATION_VECTORIZABLEOPERATOR_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_EXPERIMENTAL_VECTORIZATION_VECTORIZABLEOPERATOR_HPP_

#include <Execution/Operators/ExecutableOperator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Base class of executable operators which work on fixed-size blocks (vectors) of tuples.
 */
class VectorizableOperator : public ExecutableOperator {
public:
    /**
     * @brief This method is called by the upstream operator (parent) and passes a tuple buffer for execution.
     * @param ctx the execution context that allows accesses to local and global state.
     * @param records the records that should be processed.
     */
    virtual void execute(ExecutionContext& ctx, RecordBuffer& records) const = 0;

    virtual ~VectorizableOperator() = default;

private:
    virtual void execute(ExecutionContext& ctx, Record& record) const override;
};

using VectorizableOperatorPtr = std::shared_ptr<const VectorizableOperator>;

} // namespace NES::Runtime::Execution::Operators

#endif // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_EXPERIMENTAL_VECTORIZATION_VECTORIZABLEOPERATOR_HPP_
