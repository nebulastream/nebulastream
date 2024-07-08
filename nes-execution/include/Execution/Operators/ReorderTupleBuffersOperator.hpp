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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_REORDERTUPLEBUFFERSOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_REORDERTUPLEBUFFERSOPERATOR_HPP_

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Operator.hpp>

class RecordBuffer;

namespace NES::Runtime::Execution::Operators {
/**
 * @brief This class is the phase of migration on a destination node to write tuples to the file in order
 */
class ReorderTupleBuffersOperator : public Operator {
  public:
    /**
    * @brief Constructor for a ReorderTupleBuffersOperator
    */
    ReorderTupleBuffersOperator(uint64_t operatorHandlerIndex);

    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  protected:
    const uint64_t operatorHandlerIndex;
};
}// namespace NES::Runtime::Execution::Operators
#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_REORDERTUPLEBUFFERSOPERATOR_HPP_
