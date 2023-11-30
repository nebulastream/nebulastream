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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_LIMIT_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_LIMIT_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Limit operator handler to manage the global state of a limit operator
 */
class LimitOperatorHandler : public Runtime::Execution::OperatorHandler,
                             public ::NES::detail::virtual_enable_shared_from_this<LimitOperatorHandler, false> {
  public:
    /**
     * @brief Creates the operator handler.
     */
    explicit LimitOperatorHandler(const uint64_t limit) : limit(limit){};
    void start(Runtime::Execution::PipelineExecutionContextPtr, uint32_t) { NES_DEBUG("start LimitOperatorHandler"); }

    void stop(Runtime::QueryTerminationType queryTerminationType, Runtime::Execution::PipelineExecutionContextPtr) {
        NES_DEBUG("shutdown LimitOperatorHandler: {}", queryTerminationType);
    }
    const uint64_t limit;
    std::atomic<uint64_t> counter = 0;
};

/**
 * @brief Limit operator that limits the number of records returned by the query.
 */
class Limit : public ExecutableOperator {
  public:
    /**
     * @brief Creates a limit operator
     * @param limitRecords number of records to limit
     */
    explicit Limit(const uint64_t operatorHandlerIndex) : operatorHandlerIndex(operatorHandlerIndex){};
    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const uint64_t operatorHandlerIndex;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_LIMIT_HPP_
