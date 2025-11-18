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
HashMap* getHashJoinHashMapProxy(
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
        timestamp, operatorHandler->getCreateNewSlicesFunction(hashMapSliceArgs));
    INVARIANT(
        hashMap.size() == 1,
        "We expect exactly one slice for the given timestamp during the HashJoinBuild, as we currently solely support "
        "slicing, but got {}",
        hashMap.size());

    /// Converting the slice to an HJSlice and returning the pointer to the hashmap
    const auto hjSlice = std::dynamic_pointer_cast<HJSlice>(hashMap[0]);
    INVARIANT(hjSlice != nullptr, "The slice should be an HJSlice in an HJBuildPhysicalOperator");
    return hjSlice->getHashMapPtrOrCreate(workerThreadId, buildSide);
}

void HJBuildPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    StreamJoinBuildPhysicalOperator::setup(executionCtx, compilationContext);

    /// Creating the cleanup function for the slice of current stream
    /// As the setup function does not get traced, we do not need to have any nautilus::invoke calls to jump to the C++ runtime
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    auto* operatorHandler = dynamic_cast<HJOperatorHandler*>(executionCtx.getGlobalOperatorHandler(operatorHandlerId).value);
    if (operatorHandler->wasSetupCalled(joinBuildSide))
    {
        return;
    }

    const auto cleanupStateNautilusFunction
        = std::make_shared<CreateNewHashMapSliceArgs::NautilusCleanupExec>(compilationContext.registerFunction(std::function(
            [copyOfHashMapOptions = hashMapOptions](nautilus::val<HashMap*> hashMap)
            {
                const ChainedHashMapRef hashMapRef{
                    hashMap,
                    copyOfHashMapOptions.fieldKeys,
                    copyOfHashMapOptions.fieldValues,
                    copyOfHashMapOptions.entriesPerPage,
                    copyOfHashMapOptions.entrySize};
                for (const auto entry : hashMapRef)
                {
                    const ChainedHashMapRef::ChainedEntryRef entryRefReset{
                        entry, hashMap, copyOfHashMapOptions.fieldKeys, copyOfHashMapOptions.fieldValues};
                    const auto state = entryRefReset.getValueMemArea();
                    nautilus::invoke(
                        +[](int8_t* pagedVectorMemArea) -> void
                        {
                            /// Calls the destructor of the PagedVector
                            /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                            auto* pagedVector = reinterpret_cast<PagedVector*>(pagedVectorMemArea);
                            pagedVector->~PagedVector();
                        },
                        state);
                }
            })));
    /// NOLINTEND(performance-unnecessary-value-param)
    operatorHandler->setNautilusCleanupExec(cleanupStateNautilusFunction, joinBuildSide);
}

void HJBuildPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the operator handler from the local state
    auto* localState = dynamic_cast<WindowOperatorBuildLocalState*>(ctx.getLocalState(id));
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
    ChainedHashMapRef hashMap{
        hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize};

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
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the underyling paged vector
            const ChainedHashMapRef::ChainedEntryRef entryRefReset{entry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues};
            const auto state = entryRefReset.getValueMemArea();
            nautilus::invoke(
                +[](int8_t* pagedVectorMemArea) -> void
                {
                    /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
                    /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                    auto* pagedVector = reinterpret_cast<PagedVector*>(pagedVectorMemArea);
                    new (pagedVector) PagedVector();
                },
                state);
        },
        ctx.pipelineMemoryProvider.bufferProvider);

    /// Inserting the tuple into the corresponding hash entry
    const ChainedHashMapRef::ChainedEntryRef entryRef{hashMapEntry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues};
    auto entryMemArea = entryRef.getValueMemArea();
    const PagedVectorRef pagedVectorRef(entryMemArea, bufferRef);
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
