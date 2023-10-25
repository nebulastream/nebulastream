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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_KEYEDTIMEWINDOW_KEYEDWINDOWEMITACTION_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_KEYEDTIMEWINDOW_KEYEDWINDOWEMITACTION_HPP_
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Operators/Streaming/Aggregations/SliceMergingAction.hpp>
namespace NES::Runtime::Execution::Operators {

/**
 * @brief The KeyedWindowEmitAction emits keyed slices as individual windows.
 * Each key will result in an independent tuple in the result data stream.
 */
class KeyedWindowEmitAction : public SliceMergingAction {
  public:
    KeyedWindowEmitAction(const std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions,
                          const std::string startTsFieldName,
                          const std::string endTsFieldName,
                          const uint64_t keySize,
                          const uint64_t valueSize,
                          const std::vector<std::string> resultKeyFields,
                          const std::vector<PhysicalTypePtr> keyDataTypes,
                          const uint64_t resultOriginId);

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
    const uint64_t keySize;
    const uint64_t valueSize;
    const std::vector<std::string> resultKeyFields;
    const std::vector<PhysicalTypePtr> keyDataTypes;
    const uint64_t resultOriginId;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_KEYEDTIMEWINDOW_KEYEDWINDOWEMITACTION_HPP_
