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

#include <Join/HashJoin/HJBuildPhysicalOperator.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <DataTypes/DataTypesUtil.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/SliceDataBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <HashMapSlice.hpp>
#include <WindowBuildPhysicalOperator.hpp>
#include <function.hpp>
#include <options.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_enum.hpp>
#include <val_ptr.hpp>

namespace NES
{

void HJBuildPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    StreamJoinBuildPhysicalOperator::setup(executionCtx, compilationContext);
}

void HJBuildPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the operator handler from the local state
    auto* localState = dynamic_cast<WindowOperatorBuildLocalState*>(ctx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    /// Get the current slice / hash map that we have to insert the tuple into
    const auto timestamp = timeFunction->getTs(ctx, record);
    HashMapSliceBuffer hashMapSlice{
        sliceStoreRef->getDataStructureRef(timestamp, ctx.workerThreadId, operatorHandler, ctx.pipelineMemoryProvider.bufferProvider)};

    ChainedHashMapRef hashMap = hashMapSlice.asHashMap(hashMapOptions);

    /// Calling the key functions to add/update the keys to the record
    nautilus::val<bool> containsNullInKey{false};
    for (nautilus::static_val<uint64_t> i = 0; i < hashMapOptions.fieldKeys.size(); ++i)
    {
        const auto& [fieldIdentifier, type, fieldOffset] = hashMapOptions.fieldKeys[i];
        const auto& function = hashMapOptions.keyFunctions[i];
        const auto value = function.execute(record, ctx.pipelineMemoryProvider.arena);
        containsNullInKey = containsNullInKey or (value.isNullable() and value.isNull());
        record.write(fieldIdentifier, value);
    }

    /// If any key field is null, we need to skip it from inserting the tuple in the hash table, as the tuple will never be included
    /// in the result set. This is the case as an inner join requires all join conditions to be TRUE (i.e., no NULL values in the join fields).
    if (not containsNullInKey)
    {
        /// Finding or creating the entry for the provided record
        const auto hashMapEntry = hashMap.findOrCreateEntry(
            record,
            *hashMapOptions.hashFunction,
            [&](const nautilus::val<AbstractHashMapEntry*>& entry)
            {
                /// If the entry for the provided keys does not exist, we need to create a new one and initialize the underlying paged vector,
                /// storing it as a child of the hash map buffer and keeping its index in the entry's value area.
                const auto entryRefReset = hashMap.getEntryRef(entry);
                auto valueMemArea = static_cast<nautilus::val<uint32_t*>>(entryRefReset.getValueMemArea());
                auto pagedVector = PagedVectorRef::createBuffer(ctx.pipelineMemoryProvider.bufferProvider, tupleLayout->getTupleSize());
                *valueMemArea = static_cast<nautilus::val<uint32_t>>(hashMapSlice.getBuffer().storeChild(std::move(pagedVector)));
            },
            ctx.pipelineMemoryProvider.bufferProvider);

        /// Inserting the tuple into the corresponding hash entry's paged vector (loaded from the hash map buffer via the stored index)
        const auto entryRef = hashMap.getEntryRef(hashMapEntry);
        auto pagedVecBuffer = hashMapSlice.getBuffer().getChild(
            static_cast<nautilus::val<size_t>>(readValueFromMemRef<uint32_t>(entryRef.getValueMemArea())));
        PagedVectorRef pagedVectorRef{std::move(pagedVecBuffer), tupleLayout};
        pagedVectorRef.pushBack(record, ctx.pipelineMemoryProvider.bufferProvider);
    }
}

HJBuildPhysicalOperator::HJBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout,
    HashMapOptions hashMapOptions,
    std::unique_ptr<SliceStoreRef> sliceStoreRef)
    : StreamJoinBuildPhysicalOperator{operatorHandlerId, joinBuildSide, std::move(timeFunction), std::move(tupleLayout), std::move(sliceStoreRef)}
    , hashMapOptions(std::move(hashMapOptions))
{
}

}
