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

#include <ranges>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJBuild.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJSlice.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <magic_enum/magic_enum.hpp>
#include <Engine.hpp>
#include <val_enum.hpp>


namespace NES::Runtime::Execution::Operators
{
Interface::HashMap* getHashJoinHashMapProxy(
    const HJOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const QueryCompilation::JoinBuildSideType buildSide,
    const HJBuild* buildOperator)
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
    INVARIANT(hjSlice != nullptr, "The slice should be an HJSlice in an HJBuild");
    hjSlice->setCleanupFunction(buildOperator->getSliceCleanupFunction(operatorHandler->getNautilusCleanupExec(buildSide)), buildSide);
    const CreateNewHashMapSliceArgs hashMapSliceArgs{
        buildOperator->keySize, buildOperator->valueSize, buildOperator->pageSize, buildOperator->numberOfBuckets};
    return hjSlice->getHashMapPtrOrCreate(workerThreadId, buildSide, hashMapSliceArgs);
}

void HJBuild::setup(ExecutionContext& executionCtx) const
{
    StreamJoinBuild::setup(executionCtx);

    /// Creating the cleanup function for the slice of current stream
    nautilus::invoke(
        +[](HJOperatorHandler* operatorHandler, const HJBuild* buildOperator, const QueryCompilation::JoinBuildSideType buildSide)
        {
            nautilus::engine::Options options;
            options.setOption("engine.Compilation", true);
            const nautilus::engine::NautilusEngine nautilusEngine(options);
            const auto cleanupStateNautilusFunction
                = std::make_shared<HJOperatorHandler::NautilusCleanupExec>(nautilusEngine.registerFunction(
                    std::function(
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
                                    entry, copyOfFieldKeys, copyOfFieldValues);
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
        executionCtx.getGlobalOperatorHandler(operatorHandlerIndex),
        nautilus::val<const HJBuild*>(this),
        nautilus::val<QueryCompilation::JoinBuildSideType>(joinBuildSide));
}

void HJBuild::execute(ExecutionContext& ctx, Record& record) const
{
    /// Get the current slice / hash map that we have to insert the tuple into
    const auto timestamp = timeFunction->getTs(ctx, record);
    const auto operatorHandlerRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto hashMapPtr = invoke(
        getHashJoinHashMapProxy,
        ctx.getGlobalOperatorHandler(operatorHandlerIndex),
        timestamp,
        ctx.getWorkerThreadId(),
        nautilus::val<QueryCompilation::JoinBuildSideType>(joinBuildSide),
        nautilus::val<const HJBuild*>(this));
    Interface::ChainedHashMapRef hashMap(hashMapPtr, fieldKeys, fieldValues, entriesPerPage, entrySize);

    /// Calling the key functions to add/update the keys to the record
    for (nautilus::static_val<uint64_t> i = 0; i < fieldKeys.size(); ++i)
    {
        const auto& [fieldIdentifier, type, fieldOffset] = fieldKeys[i];
        const auto& function = keyFunctions[i];
        const auto value = function->execute(record, ctx.pipelineMemoryProvider.arena);
        record.write(fieldIdentifier, value);
    }

    /// Finding or creating the entry for the provided record
    const auto hashMapEntry = hashMap.findOrCreateEntry(
        record,
        *hashFunction,
        [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
        {
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the underyling paged vector
            const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(entry, fieldKeys, fieldValues);
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
        ctx.pipelineMemoryProvider.bufferProvider);

    /// Inserting the tuple into the corresponding hash entry
    const Interface::ChainedHashMapRef::ChainedEntryRef entryRef(hashMapEntry, fieldKeys, fieldValues);
    auto entryMemArea = entryRef.getValueMemArea();
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(entryMemArea, memoryProvider);
    pagedVectorRef.writeRecord(record, ctx.pipelineMemoryProvider.bufferProvider);
}

HJBuild::HJBuild(
    const uint64_t operatorHandlerIndex,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
    HashMapOptions hashMapOptions)
    : StreamJoinBuild(operatorHandlerIndex, joinBuildSide, std::move(timeFunction), memoryProvider)
    , HashMapOptions(std::move(hashMapOptions))
{
}

}
