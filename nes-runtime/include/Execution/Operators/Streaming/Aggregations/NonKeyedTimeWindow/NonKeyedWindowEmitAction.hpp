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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_NONKEYEDTIMEWINDOW_NONKEYEDWINDOWEMITACTION_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_NONKEYEDTIMEWINDOW_NONKEYEDWINDOWEMITACTION_HPP_
#include <Execution/Operators/Streaming/Aggregations/SliceMergingAction.hpp>
namespace NES::Runtime::Execution::Operators {

/**
 * @brief The KeyedWindowEmitAction emits non-keyed slices as individual windows.
 */
class NonKeyedWindowEmitAction : public SliceMergingAction {
  public:
    NonKeyedWindowEmitAction(const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
                             const std::string& startTsFieldName,
                             const std::string& endTsFieldName,
                             uint64_t resultOriginId);

    void emitSlice(ExecutionContext& ctx,
                   ExecuteOperatorPtr& child,
                   Value<UInt64>& windowStart,
                   Value<UInt64>& windowEnd,
                   Value<UInt64>& sequenceNumber,
                   Value<MemRef>& globalSlice) const override;

  private:
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions;
    const std::string startTsFieldName;
    const std::string endTsFieldName;
    const uint64_t resultOriginId;
};
}// namespace NES::Runtime::Execution::Operators

#endif  // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_NONKEYEDTIMEWINDOW_NONKEYEDWINDOWEMITACTION_HPP_
