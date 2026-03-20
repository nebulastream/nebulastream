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
#include <Identifiers/Identifiers.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Time/Timestamp.hpp>
#include <std/cstring.h>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <HashMapSlice.hpp>
#include <WindowBuildPhysicalOperator.hpp>
#include <function.hpp>
#include <options.hpp>
#include <static.hpp>
#include <val_enum.hpp>
#include <val_ptr.hpp>

namespace NES
{
TupleBuffer* getHashJoinHashMapProxy(
    AbstractBufferProvider* bufferProvider,
    PipelineExecutionContext* pec,
    const HJOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const JoinBuildSideType buildSide,
    const HJBuildPhysicalOperator* buildOperator)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");
    PRECONDITION(buildOperator != nullptr, "The build operator should not be null");

    const CreateNewHashMapSliceArgs hashMapSliceArgs{
        operatorHandler->getNautilusCleanupExec(),
        buildOperator->hashMapOptions.keySize,
        buildOperator->hashMapOptions.valueSize,
        buildOperator->hashMapOptions.pageSize,
        buildOperator->hashMapOptions.numberOfBuckets};
    const auto hashMap = operatorHandler->getSliceAndWindowStore().getSlicesOrCreate(
        timestamp, operatorHandler->getCreateNewSlicesFunction(bufferProvider, hashMapSliceArgs));
    INVARIANT(
        hashMap.size() == 1,
        "We expect exactly one slice for the given timestamp during the HashJoinBuild, as we currently solely support "
        "slicing, but got {}",
        hashMap.size());

    /// Converting the slice to an HJSlice and returning the pointer to the hashmap
    const auto hjSlice = std::dynamic_pointer_cast<HJSlice>(hashMap[0]);
    INVARIANT(hjSlice != nullptr, "The slice should be an HJSlice in an HJBuildPhysicalOperator");
    auto hashMapChildIndex = hjSlice->getWorkerHashMapIndex(workerThreadId, buildSide);
    auto hashMapTupleBuffer = hjSlice->loadHashMapBuffer(hashMapChildIndex);
    return std::addressof(pec->pinBuffer(std::move(hashMapTupleBuffer)));
}

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
    const auto hashMapTupleBuffer = invoke(
        getHashJoinHashMapProxy,
        ctx.pipelineMemoryProvider.bufferProvider,
        ctx.pipelineContext,
        operatorHandler,
        timestamp,
        ctx.workerThreadId,
        nautilus::val<JoinBuildSideType>(joinBuildSide),
        nautilus::val<const HJBuildPhysicalOperator*>(this));

    ChainedHashMapRef hashMap{
        hashMapTupleBuffer, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize};

    /// Calling the key functions to add/update the keys to the record
    for (nautilus::static_val<uint64_t> i = 0; i < hashMapOptions.fieldKeys.size(); ++i)
    {
        const auto& [fieldIdentifier, type, fieldOffset] = hashMapOptions.fieldKeys[i];
        const auto& function = hashMapOptions.keyFunctions[i];
        const auto value = function.execute(record, ctx.pipelineMemoryProvider.arena);
        record.write(fieldIdentifier, value);
    }

    /// Finding or creating the entry for the provided record
    const auto hashMapEntry = hashMap.findOrCreateEntry(
        record,
        *hashMapOptions.hashFunction,
        [&](const nautilus::val<AbstractHashMapEntry*>& entry)
        {
            const ChainedHashMapRef::ChainedEntryRef entryRefReset{
                entry, hashMapTupleBuffer, hashMapOptions.fieldKeys, hashMapOptions.fieldValues};
            auto childBufferIndexVal = nautilus::invoke(
                +[](AbstractBufferProvider* bufferProvider, TupleBuffer* hashMapTupleBuffer) -> uint64_t
                {
                    /// get a new buffer for the paged vector for the new entry
                    if (auto pagedVectorBuffer = bufferProvider->getUnpooledBuffer(PagedVector::getBufferSize()))
                    {
                        /// initialize paged vector buffer and attach it to the hash map
                        PagedVector pagedVector = PagedVector::init(pagedVectorBuffer.value());
                        auto childBufferIdx = hashMapTupleBuffer->storeChildBuffer(pagedVectorBuffer.value());
                        return childBufferIdx.getRawIndex();
                    }
                    throw BufferAllocationFailure("No unpooled TupleBuffer available for chained hash map entry's paged vector!");
                },
                ctx.pipelineMemoryProvider.bufferProvider,
                hashMapTupleBuffer);
            /// store the child buffer index the entry's memory area
            const auto memArea = entryRefReset.getValueMemArea();
            nautilus::invoke(
                +[](int8_t* memArea, uint32_t idx) -> void { *reinterpret_cast<uint32_t*>(memArea) = idx; }, memArea, childBufferIndexVal);
        },
        ctx.pipelineMemoryProvider.bufferProvider);

    /// Inserting the tuple into the corresponding hash entry
    const ChainedHashMapRef::ChainedEntryRef entryRef{
        hashMapEntry, hashMapTupleBuffer, hashMapOptions.fieldKeys, hashMapOptions.fieldValues};

    auto entryMemArea = entryRef.getValueMemArea();
    const auto childBufferIndexVal = readValueFromMemRef<uint32_t>(entryMemArea);
    const auto pagedVectorBuffer = invoke(
        +[](TupleBuffer* hashMapBuffer, const uint32_t childBufferIndexVal)
        {
            VariableSizedAccess::Index childBufferIndex{childBufferIndexVal};
            return std::addressof(hashMapBuffer->getChildRef(childBufferIndex));
        },
        hashMapTupleBuffer,
        childBufferIndexVal);
    const PagedVectorRef pagedVectorRef(pagedVectorBuffer, bufferRef);
    pagedVectorRef.writeRecord(record, ctx.pipelineMemoryProvider.bufferProvider);
}

HJBuildPhysicalOperator::HJBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    const std::shared_ptr<TupleBufferRef>& bufferRef,
    HashMapOptions hashMapOptions)
    : StreamJoinBuildPhysicalOperator(operatorHandlerId, joinBuildSide, std::move(timeFunction), bufferRef)
    , hashMapOptions(std::move(hashMapOptions))
{
}

}
