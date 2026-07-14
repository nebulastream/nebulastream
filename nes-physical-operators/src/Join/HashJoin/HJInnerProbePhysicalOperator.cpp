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
#include <Join/HashJoin/HJInnerProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <DataTypes/DataTypesUtil.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/HashJoin/HJProbePhysicalOperatorBase.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/SlicedWindowStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>
#include <HashMapOptions.hpp>
#include <val_ptr.hpp>

namespace NES
{

HJInnerProbePhysicalOperator::HJInnerProbePhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    JoinSchema joinSchema,
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
    HashMapOptions leftHashMapBasedOptions,
    HashMapOptions rightHashMapBasedOptions)
    : HJProbePhysicalOperatorBase(
          operatorHandlerId,
          std::move(joinFunction),
          std::move(windowMetaData),
          std::move(joinSchema),
          std::move(leftTupleLayout),
          std::move(rightTupleLayout),
          std::move(leftHashMapBasedOptions),
          std::move(rightHashMapBasedOptions))
{
}

void HJInnerProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    StreamJoinProbePhysicalOperator::open(executionCtx, recordBuffer);

    /// Getting number of hash maps
    const auto hashJoinWindowRef = static_cast<nautilus::val<EmittedHJWindowTrigger*>>(recordBuffer.getMemArea());
    const auto leftNumberOfHashMaps
        = readValueFromMemRef<uint64_t>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::leftNumberOfHashMaps));
    const auto rightNumberOfHashMaps
        = readValueFromMemRef<uint64_t>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::rightNumberOfHashMaps));

    /// Getting necessary values from the record buffer
    const auto windowInfoRef = getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::windowInfo);
    const nautilus::val<Timestamp> windowStart{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const nautilus::val<Timestamp> windowEnd{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};
    const auto leftHashMapRefs = readValueFromMemRef<HashMap**>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::leftHashMaps));
    const auto rightHashMapRefs = readValueFromMemRef<HashMap**>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::rightHashMaps));

    performMatchPairsProbe(
        leftHashMapRefs, leftNumberOfHashMaps, rightHashMapRefs, rightNumberOfHashMaps, executionCtx, windowStart, windowEnd);
}
}
