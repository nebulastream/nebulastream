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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_THRESHOLDWINDOW_KEYEDTHRESHOLDWINDOW_KEYEDTHRESHOLDWINDOWOPERATORHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_THRESHOLDWINDOW_KEYEDTHRESHOLDWINDOW_KEYEDTHRESHOLDWINDOWOPERATORHANDLER_HPP_

#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Operators/ThresholdWindow/KeyedThresholdWindow/KeyedThresholdWindowState.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <mutex>
#include <utility>

namespace NES::Runtime::Execution::Operators {
/**
 * @brief This handler stores states of a keyed threshold window during its execution
 */
class KeyedThresholdWindowOperatorHandler : public OperatorHandler {
  public:
    explicit KeyedThresholdWindowOperatorHandler() = default;

    void start(PipelineExecutionContextPtr, uint32_t) override {}

    void stop(QueryTerminationType, PipelineExecutionContextPtr) override {}

    std::unordered_map<uint32_t, KeyedThresholdWindowState> keyedAggregationStates{};
};
}// namespace NES::Runtime::Execution::Operators

#endif  // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_THRESHOLDWINDOW_KEYEDTHRESHOLDWINDOW_KEYEDTHRESHOLDWINDOWOPERATORHANDLER_HPP_
