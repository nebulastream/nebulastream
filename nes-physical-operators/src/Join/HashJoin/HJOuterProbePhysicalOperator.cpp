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
#include <Join/HashJoin/HJOuterProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/HashJoin/HJProbePhysicalOperatorBase.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <HashMapOptions.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_enum.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
/// Loads the PagedVector child buffer referenced by a chained hash map entry's value area (which stores a child-buffer index
/// into the hash map's own buffer).
PagedVectorRef
loadEntryPagedVector(const ChainedHashMapRef::ChainedEntryRef& entryRef, const std::shared_ptr<PagedVectorTupleLayout>& tupleLayout)
{
    auto valueMemArea = entryRef.getValueMemArea();
    OwnedNautilusBuffer pagedVectorBuffer;
    nautilus::invoke(
        +[](TupleBuffer* hashMapBuf, TupleBuffer* out, const uint32_t* indexPtr)
        { *out = hashMapBuf->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
        entryRef.hashMapBuffer,
        pagedVectorBuffer.asArg(),
        static_cast<nautilus::val<uint32_t*>>(valueMemArea));
    /// Must move the owned buffer into the PagedVectorRef rather than wrap a BorrowedNautilusBuffer around it: a
    /// borrowed view only stores a pointer back to `pagedVectorBuffer`, which would dangle once this function returns.
    return PagedVectorRef{std::move(pagedVectorBuffer), tupleLayout};
}
}

HJOuterProbePhysicalOperator::HJOuterProbePhysicalOperator(
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

void HJOuterProbePhysicalOperator::performNullFillProbe(
    const nautilus::val<TupleBuffer*>& recordBufferRef,
    nautilus::val<uint64_t> outerOffset,
    nautilus::val<uint64_t> outerNumberOfHashMaps,
    nautilus::val<uint64_t> innerOffset,
    nautilus::val<uint64_t> innerNumberOfHashMaps,
    const HashMapOptions& outerHashMapOptions,
    const HashMapOptions& innerHashMapOptions,
    const std::shared_ptr<PagedVectorTupleLayout>& outerTupleLayout,
    const Schema<QualifiedUnboundField, Ordered>& nullSideSchema,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    const auto outerFields = getOrderedFieldNames(outerTupleLayout->getSchema());

    /// Iterate all outer (preserved-side) hash maps
    for (nautilus::val<uint64_t> outerIdx = 0; outerIdx < outerNumberOfHashMaps; ++outerIdx)
    {
        auto outerHashMapBuffer = pinHashMapBuffer(recordBufferRef, outerOffset + outerIdx);
        const ChainedHashMapRef outerHashMap = makeChainedHashMapRef(outerHashMapBuffer.asArg(), outerHashMapOptions);

        for (const auto outerEntry : outerHashMap)
        {
            const ChainedHashMapRef::ChainedEntryRef outerEntryRef{
                outerEntry, outerHashMapBuffer.asArg(), outerHashMapOptions.fieldKeys, outerHashMapOptions.fieldValues};
            const PagedVectorRef outerPagedVector = loadEntryPagedVector(outerEntryRef, outerTupleLayout);

            /// Check all inner hash maps for a matching key
            nautilus::val<bool> matched(false);
            for (nautilus::val<uint64_t> innerIdx = 0; innerIdx < innerNumberOfHashMaps; ++innerIdx)
            {
                auto innerHashMapBuffer = pinHashMapBuffer(recordBufferRef, innerOffset + innerIdx);
                ChainedHashMapRef innerHashMap = makeChainedHashMapRef(innerHashMapBuffer.asArg(), innerHashMapOptions);

                if (innerHashMap.findEntry(outerEntryRef.entryRef) != nullptr)
                {
                    matched = nautilus::val<bool>(true);
                    break; /// Only need to know a match exists, no need to check remaining hash maps
                }
            }
            if (!matched)
            {
                /// Emit unmatched outer tuples with NULL-filled inner fields
                for (auto outerIt = outerPagedVector.begin(); outerIt != outerPagedVector.end(); ++outerIt)
                {
                    const auto outerRecord = *outerIt;
                    auto nullRecord = createNullFilledJoinedRecord(outerRecord, windowStart, windowEnd, outerFields, nullSideSchema);
                    executeChild(executionCtx, nullRecord);
                }
            }
        }
    }
}

/// NOLINTNEXTLINE(readability-function-cognitive-complexity) outer join dispatches by task type and delegates to inner/null-fill probes
void HJOuterProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    StreamJoinProbePhysicalOperator::open(executionCtx, recordBuffer);

    /// Parse the trigger buffer
    const auto hashJoinWindowRef = static_cast<nautilus::val<EmittedHJWindowTrigger*>>(recordBuffer.getMemArea());
    const auto leftNumberOfHashMaps
        = readValueFromMemRef<uint64_t>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::leftNumberOfHashMaps));
    const auto rightNumberOfHashMaps
        = readValueFromMemRef<uint64_t>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::rightNumberOfHashMaps));

    const auto windowInfoRef = getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::windowInfo);
    const nautilus::val<Timestamp> windowStart{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const nautilus::val<Timestamp> windowEnd{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};

    /// The hash map buffers themselves are stored as child buffers of the record buffer: left ones first, right ones after
    const auto& recordBufferRef = recordBuffer.getReference();
    const nautilus::val<uint64_t> leftOffset{0ULL};
    /// Right-side hash maps are stored immediately after the left ones
    const nautilus::val<uint64_t>& rightOffset = leftNumberOfHashMaps;

    /// Read the probe task type to determine what work this task should perform
    const nautilus::val<ProbeTaskType> probeTaskType
        = readValueFromMemRef<uint64_t>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::probeTaskType));
    if (probeTaskType == ProbeTaskType::LEFT_NULL_FILL)
    {
        if (leftNumberOfHashMaps > 0)
        {
            performNullFillProbe(
                recordBufferRef,
                leftOffset,
                leftNumberOfHashMaps,
                rightOffset,
                rightNumberOfHashMaps,
                leftHashMapOptions,
                rightHashMapOptions,
                leftTupleLayout,
                joinSchema.rightSchema,
                executionCtx,
                windowStart,
                windowEnd);
        }
    }
    else if (probeTaskType == ProbeTaskType::RIGHT_NULL_FILL)
    {
        if (rightNumberOfHashMaps > 0)
        {
            performNullFillProbe(
                recordBufferRef,
                rightOffset,
                rightNumberOfHashMaps,
                leftOffset,
                leftNumberOfHashMaps,
                rightHashMapOptions,
                leftHashMapOptions,
                rightTupleLayout,
                joinSchema.leftSchema,
                executionCtx,
                windowStart,
                windowEnd);
        }
    }
    else if (probeTaskType == ProbeTaskType::MATCH_PAIRS)
    {
        performMatchPairsProbe(recordBufferRef, leftNumberOfHashMaps, rightNumberOfHashMaps, executionCtx, windowStart, windowEnd);
    }
    else
    {
        nautilus::invoke(
            +[](const ProbeTaskType unknownProbeTaskType)
            { throw NotImplemented("Using unknown probeTaskType {}", magic_enum::enum_name(unknownProbeTaskType)); },
            probeTaskType);
    }
}
}
