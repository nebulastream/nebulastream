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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_APPENDTOSLICESTOREACTION_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_APPENDTOSLICESTOREACTION_HPP_
#include <Execution/Operators/Streaming/Aggregations/SliceMergingAction.hpp>
#include <Execution/Operators/Streaming/Aggregations/SlidingWindowSliceStore.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <atomic>
namespace NES::Runtime::Execution::Operators {
class MultiOriginWatermarkProcessor;
/**
 * @brief The AppendToSliceStoreAction appends slices to the slice store for sliding windows.
 * @tparam Slice
 */
template<class Slice>
class AppendToSliceStoreAction : public SliceMergingAction {
  public:
    AppendToSliceStoreAction(const uint64_t operatorHandlerIndex);

    void emitSlice(ExecutionContext& ctx,
                   ExecuteOperatorPtr& child,
                   Value<UInt64>& windowStart,
                   Value<UInt64>& windowEnd,
                   Value<UInt64>& sequenceNumber,
                   Value<MemRef>& globalSlice) const override;

  private:
    const uint64_t operatorHandlerIndex;
};
class NonKeyedSlice;
using NonKeyedAppendToSliceStoreAction = AppendToSliceStoreAction<NonKeyedSlice>;
class KeyedSlice;
using KeyedAppendToSliceStoreAction = AppendToSliceStoreAction<KeyedSlice>;
}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_APPENDTOSLICESTOREACTION_HPP_
