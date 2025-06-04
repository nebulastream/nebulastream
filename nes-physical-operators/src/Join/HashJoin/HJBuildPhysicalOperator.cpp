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

#include <ranges>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <SliceStore/Slice.hpp>
#include <magic_enum/magic_enum.hpp>
#include <Engine.hpp>
#include <val_enum.hpp>


namespace NES
{
Interface::HashMap* getHashJoinHashMapProxy(
    const HJOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const JoinBuildSideType buildSide,
    const HJBuildPhysicalOperator* buildOperator)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");
    PRECONDITION(buildOperator != nullptr, "The build operator should not be null");

    const auto hashMap
        = operatorHandler->getSliceAndWindowStore().getSlicesOrCreate(timestamp, operatorHandler->getCreateNewSlicesFunction());
    INVARIANT(
        hashMap.size() == 1,
        "We expect exactly one slice for the given timestamp during the HashJoinBuild, as we currently solely support "
        "slicing, but got {}",
        hashMap.size());

    /// Converting the slice to an HJSlice and returning the pointer to the hashmap
    const auto hjSlice = std::dynamic_pointer_cast<HJSlice>(hashMap[0]);
    INVARIANT(hjSlice != nullptr, "The slice should be an HJSlice in an HJBuildPhysicalOperator");
    hjSlice->setCleanupFunction(buildOperator->getSliceCleanupFunction(operatorHandler->getNautilusCleanupExec(buildSide)), buildSide);
    const CreateNewHashMapSliceArgs hashMapSliceArgs{
        buildOperator->keySize, buildOperator->valueSize, buildOperator->pageSize, buildOperator->numberOfBuckets};
    return hjSlice->getHashMapPtrOrCreate(workerThreadId, buildSide, hashMapSliceArgs);
}

void HJBuildPhysicalOperator::setup(ExecutionContext& executionCtx) const
{
    StreamJoinBuildPhysicalOperator::setup(executionCtx);

    /// Creating the cleanup function for the slice of current stream
    nautilus::invoke(
        +[](HJOperatorHandler* operatorHandler, const HJBuildPhysicalOperator* buildOperator, const JoinBuildSideType buildSide)
        {
            nautilus::engine::Options options;
            options.setOption("engine.Compilation", true);
            const nautilus::engine::NautilusEngine nautilusEngine(options);
            const auto cleanupStateNautilusFunction
                = std::make_shared<HJOperatorHandler::NautilusCleanupExec>(nautilusEngine.registerFunction(std::function(
                    [copyOfFieldKeys = buildOperator->fieldKeys,
                     copyOfFieldValues = buildOperator->fieldValues,
                     copyOfEntriesPerPage = buildOperator->entriesPerPage,
                     copyOfEntrySize = buildOperator->entrySize](nautilus::val<Nautilus::Interface::HashMap*> hashMap)
                    {
                        const Interface::ChainedHashMapRef hashMapRef(
                            hashMap, copyOfFieldKeys, copyOfFieldValues, copyOfEntriesPerPage, copyOfEntrySize);
                        for (const auto entry : hashMapRef)
                        {
                            const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(
                                entry, hashMap, copyOfFieldKeys, copyOfFieldValues);
                            const auto state = entryRefReset.getValueMemArea();
                            nautilus::invoke(
                                +[](int8_t* pagedVectorMemArea) -> void
                                {
                                    /// Calls the destructor of the PagedVector
                                    auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(
                                        pagedVectorMemArea); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                                    pagedVector->~PagedVector();
                                },
                                state);
                        }
                    })));
            operatorHandler->setNautilusCleanupExec(std::move(cleanupStateNautilusFunction), buildSide);
        },
        executionCtx.getGlobalOperatorHandler(operatorHandlerId),
        nautilus::val<const HJBuildPhysicalOperator*>(this),
        nautilus::val<JoinBuildSideType>(joinBuildSide));
}

void HJBuildPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the operator handler from the local state
    auto localState = dynamic_cast<WindowOperatorBuildLocalState*>(ctx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    /// Get the current slice / hash map that we have to insert the tuple into
    const auto timestamp = timeFunction->getTs(ctx, record);
    const auto hashMapPtr = invoke(
        getHashJoinHashMapProxy,
        operatorHandler,
        timestamp,
        ctx.workerThreadId,
        nautilus::val<JoinBuildSideType>(joinBuildSide),
        nautilus::val<const HJBuildPhysicalOperator*>(this));
    Interface::ChainedHashMapRef hashMap(hashMapPtr, fieldKeys, fieldValues, entriesPerPage, entrySize);

    /// Calling the key functions to add/update the keys to the record
    for (nautilus::static_val<uint64_t> i = 0; i < fieldKeys.size(); ++i)
    {
        const auto& [fieldIdentifier, type, fieldOffset] = fieldKeys[i];
        const auto& function = keyFunctions[i];
        const auto value = function.execute(record, ctx.pipelineMemoryProvider.arena);
        record.write(fieldIdentifier, value);
    }

    /// Finding or creating the entry for the provided record
    const auto hashMapEntry = hashMap.findOrCreateEntry(
        record,
        *hashFunction,
        [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
        {
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the underyling paged vector
            const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(entry, hashMapPtr, fieldKeys, fieldValues);
            const auto state = entryRefReset.getValueMemArea();
            nautilus::invoke(
                +[](int8_t* pagedVectorMemArea) -> void
                {
                    /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
                    auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
                    new (pagedVector) Nautilus::Interface::PagedVector();
                },
                state);
        },
        ctx.pipelineMemoryProvider.bufferProvider,
        ctx.workerThreadId);

    /// Inserting the tuple into the corresponding hash entry
    const Interface::ChainedHashMapRef::ChainedEntryRef entryRef(hashMapEntry, hashMapPtr, fieldKeys, fieldValues);
    auto entryMemArea = entryRef.getValueMemArea();
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(entryMemArea, memoryProvider);
    pagedVectorRef.writeRecord(record, ctx.pipelineMemoryProvider.bufferProvider, ctx.workerThreadId);
}

HJBuildPhysicalOperator::HJBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
    HashMapOptions hashMapOptions)
    : StreamJoinBuildPhysicalOperator(operatorHandlerId, joinBuildSide, std::move(timeFunction), memoryProvider)
    , HashMapOptions(std::move(hashMapOptions))
{
}

}
