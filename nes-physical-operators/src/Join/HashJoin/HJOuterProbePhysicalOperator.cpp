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
#include <Functions/PhysicalFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/ChainedHashMap/EntryRecordsRef.hpp>
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
#include <Schema/Schema.hpp>
#include <SliceStore/SlicedWindowStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_enum.hpp>
#include <val_ptr.hpp>

namespace NES
{


HJOuterProbePhysicalOperator::HJOuterProbePhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    JoinSchema joinSchema,
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
    ChainedHashMapConfig leftHashMapBasedOptions,
    ChainedHashMapConfig rightHashMapBasedOptions,
    std::optional<HJResidual> residual)
    : HJProbePhysicalOperatorBase(
          operatorHandlerId,
          std::move(joinFunction),
          std::move(windowMetaData),
          std::move(joinSchema),
          std::move(leftTupleLayout),
          std::move(rightTupleLayout),
          std::move(leftHashMapBasedOptions),
          std::move(rightHashMapBasedOptions),
          std::move(residual))
{
}

void HJOuterProbePhysicalOperator::performNullFillProbe(
    const BorrowedNautilusBuffer& recordBufferRef,
    nautilus::val<uint64_t> outerOffset,
    nautilus::val<uint64_t> outerNumberOfHashMaps,
    nautilus::val<uint64_t> innerOffset,
    nautilus::val<uint64_t> innerNumberOfHashMaps,
    const ChainedHashMapConfig& outerHashMapConfig,
    const ChainedHashMapConfig& innerHashMapConfig,
    const std::shared_ptr<PagedVectorTupleLayout>& outerTupleLayout,
    const std::shared_ptr<PagedVectorTupleLayout>& innerTupleLayout,
    const bool outerIsLeft,
    const Schema<QualifiedUnboundField, Ordered>& nullSideSchema,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    const auto outerFields = getOrderedFieldNames(outerTupleLayout->getSchema());

    /// Iterate all outer (preserved-side) hash maps
    for (nautilus::val<uint64_t> outerIdx = 0; outerIdx < outerNumberOfHashMaps; ++outerIdx)
    {
        auto outerHashMapBuffer = recordBufferRef.getChild(outerOffset + outerIdx);
        const ChainedHashMapRef outerHashMap{outerHashMapBuffer, outerHashMapConfig};

        for (const auto outerEntry : outerHashMap)
        {
            const ChainedHashMapRef::ChainedEntryRef outerEntryRef{
                outerEntry, outerHashMapBuffer, outerHashMapConfig.fieldKeys, outerHashMapConfig.fieldValues};
            const EntryRecordsRef outerRecords{
                outerHashMapBuffer,
                outerEntryRef.getValueMemArea(),
                outerTupleLayout,
                outerHashMapConfig.inlineRecords};
            const auto outerEnd = outerRecords.end();

            if (not residual.has_value())
            {
                /// Without a residual, every tuple of an entry whose key exists on the inner side is matched
                nautilus::val<bool> matched{false};
                for (nautilus::val<uint64_t> innerIdx = 0; innerIdx < innerNumberOfHashMaps; ++innerIdx)
                {
                    auto innerHashMapBuffer = recordBufferRef.getChild(innerOffset + innerIdx);
                    ChainedHashMapRef innerHashMap{innerHashMapBuffer, innerHashMapConfig};

                    matched = matched or (innerHashMap.findEntry(outerEntryRef.entryRef) != nullptr);
                }
                if (!matched)
                {
                    /// Emit unmatched outer tuples with NULL-filled inner fields
                    for (auto outerIt = outerRecords.begin(); outerIt != outerEnd; ++outerIt)
                    {
                        const auto outerRecord = *outerIt;
                        auto nullRecord = createNullFilledJoinedRecord(outerRecord, windowStart, windowEnd, outerFields, nullSideSchema);
                        executeChild(executionCtx, nullRecord);
                    }
                }
            }
            else
            {
                /// With a residual, an outer tuple is matched only if an inner tuple with the same key also satisfies the residual,
                /// so the decision is per tuple instead of per key.
                for (auto outerIt = outerRecords.begin(); outerIt != outerEnd; ++outerIt)
                {
                    const auto outerRecord = *outerIt;
                    /// Stops at the first partner for which the residual holds. The early exit is part of the loop conditions instead of a
                    /// `break`: the loop body owns a child buffer, whose destructor is a traced call. A `break` shares that cleanup with the
                    /// fall-through path, so nautilus sees the same trace tag on both paths, merges them, and the fall-through path then
                    /// leaves the loop after the first inner hash map.
                    nautilus::val<bool> matched{false};
                    for (nautilus::val<uint64_t> innerIdx = 0; innerIdx < innerNumberOfHashMaps and not matched; ++innerIdx)
                    {
                        auto innerHashMapBuffer = recordBufferRef.getChild(innerOffset + innerIdx);
                        ChainedHashMapRef innerHashMap{innerHashMapBuffer, innerHashMapConfig};
                        if (const auto innerEntry = innerHashMap.findEntry(outerEntryRef.entryRef); innerEntry != nullptr)
                        {
                            const ChainedHashMapRef::ChainedEntryRef innerEntryRef{
                                static_cast<nautilus::val<ChainedHashMapEntry*>>(innerEntry),
                                innerHashMapBuffer,
                                innerHashMapConfig.fieldKeys,
                                innerHashMapConfig.fieldValues};
                            const EntryRecordsRef innerRecords{
                                innerHashMapBuffer,
                                innerEntryRef.getValueMemArea(),
                                innerTupleLayout,
                                innerHashMapConfig.inlineRecords};
                            const auto innerEnd = innerRecords.end();
                            for (auto innerIt = innerRecords.begin(); innerIt != innerEnd and not matched; ++innerIt)
                            {
                                const auto innerRecord = *innerIt;
                                matched = outerIsLeft ? residualHolds(outerRecord, innerRecord, executionCtx, windowStart, windowEnd)
                                                      : residualHolds(innerRecord, outerRecord, executionCtx, windowStart, windowEnd);
                            }
                        }
                    }
                    if (!matched)
                    {
                        auto nullRecord = createNullFilledJoinedRecord(outerRecord, windowStart, windowEnd, outerFields, nullSideSchema);
                        executeChild(executionCtx, nullRecord);
                    }
                }
            }
        }
    }
}

/// NOLINTNEXTLINE(readability-function-cognitive-complexity) outer join dispatches by task type and delegates to inner/null-fill probes
void HJOuterProbePhysicalOperator::probeWindow(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// Parse the trigger buffer
    const auto hashJoinWindowRef = static_cast<nautilus::val<EmittedHJWindowTrigger*>>(recordBuffer.getMemArea());
    const auto leftNumberOfHashMaps
        = readValueFromMemRef<uint64_t>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::leftNumberOfBuffers));
    const auto rightNumberOfHashMaps
        = readValueFromMemRef<uint64_t>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::rightNumberOfBuffers));

    const auto windowInfoRef = getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::windowInfo);
    const nautilus::val<Timestamp> windowStart{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const nautilus::val<Timestamp> windowEnd{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};

    /// The hash map buffers themselves are stored as child buffers of the record buffer: left ones first, right ones after
    const auto recordBufferRef = BorrowedNautilusBuffer::from(recordBuffer.getReference());
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
                leftHashMapConfig,
                rightHashMapConfig,
                leftTupleLayout,
                rightTupleLayout,
                true,
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
                rightHashMapConfig,
                leftHashMapConfig,
                rightTupleLayout,
                leftTupleLayout,
                false,
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
