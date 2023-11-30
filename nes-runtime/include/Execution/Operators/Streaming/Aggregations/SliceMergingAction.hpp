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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_SLICEMERGINGACTION_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_SLICEMERGINGACTION_HPP_
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
namespace NES::Runtime::Execution::Operators {
using namespace Nautilus;
class ExecutableOperator;
using ExecuteOperatorPtr = std::shared_ptr<const ExecutableOperator>;
/**
 * @brief A slice merge action that is called if two slices are merged.
 */
class SliceMergingAction {
  public:
    /**
     * @brief Emits a slice after slice merging.
     * @param ctx ExecutionContext
     * @param child ExecuteOperatorPtr
     * @param windowStart
     * @param windowEnd
     * @param sequenceNumber
     * @param globalSlice
     */
    virtual void emitSlice(ExecutionContext& ctx,
                           ExecuteOperatorPtr& child,
                           Value<UInt64>& windowStart,
                           Value<UInt64>& windowEnd,
                           Value<UInt64>& sequenceNumber,
                           Value<MemRef>& globalSlice) const = 0;

    virtual ~SliceMergingAction() = default;
};

}// namespace NES::Runtime::Execution::Operators

#endif  // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_SLICEMERGINGACTION_HPP_
