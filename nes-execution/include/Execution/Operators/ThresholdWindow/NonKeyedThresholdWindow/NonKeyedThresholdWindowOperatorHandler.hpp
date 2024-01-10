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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_THRESHOLDWINDOW_NONKEYEDTHRESHOLDWINDOW_NONKEYEDTHRESHOLDWINDOWOPERATORHANDLER_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_THRESHOLDWINDOW_NONKEYEDTHRESHOLDWINDOW_NONKEYEDTHRESHOLDWINDOWOPERATORHANDLER_HPP_

#include <Execution/Aggregation/AggregationValue.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <mutex>
#include <utility>

namespace NES::Runtime::Execution::Operators {
/**
 * @brief This handler stores states of an unkeyed threshold window during its execution
 */
class NonKeyedThresholdWindowOperatorHandler : public OperatorHandler {
  public:
    explicit NonKeyedThresholdWindowOperatorHandler(std::vector<std::unique_ptr<Aggregation::AggregationValue>> AggregationValues)
        : AggregationValues(std::move(AggregationValues)) {}

    void start(PipelineExecutionContextPtr, uint32_t) override {}

    void stop(QueryTerminationType, PipelineExecutionContextPtr) override {}

    uint64_t recordCount = 0;// counts the records contributing to the aggregate,
    bool isWindowOpen = false;
    std::mutex mutex;
    std::vector<std::unique_ptr<Aggregation::AggregationValue>> AggregationValues;
    uint8_t aggregationType;
};
}// namespace NES::Runtime::Execution::Operators

#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_THRESHOLDWINDOW_NONKEYEDTHRESHOLDWINDOW_NONKEYEDTHRESHOLDWINDOWOPERATORHANDLER_HPP_
