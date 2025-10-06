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
#include <Aggregation/AggregationBuildPhysicalOperator.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/SerializableAggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/SerializableAggregationSlice.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/OffsetHashMap/OffsetHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <HashMapSlice.hpp>
#include <WindowBuildPhysicalOperator.hpp>
#include <function.hpp>
#include <options.hpp>
#include <static.hpp>
#include <val_ptr.hpp>

namespace NES
{
Interface::HashMap* getAggHashMapProxy(
    const AggregationOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const AggregationBuildPhysicalOperator* buildOperator)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");
    PRECONDITION(buildOperator != nullptr, "The build operator should not be null");

    /// If a new hashmap slice is created, we need to set the cleanup function for the aggregation states
    const CreateNewHashMapSliceArgs hashMapSliceArgs{
        {buildOperator->cleanupStateNautilusFunction},
        buildOperator->hashMapOptions.keySize,
        buildOperator->hashMapOptions.valueSize,
        buildOperator->hashMapOptions.pageSize,
        buildOperator->hashMapOptions.numberOfBuckets};
    auto wrappedCreateFunction(
        [createFunction = operatorHandler->getCreateNewSlicesFunction(hashMapSliceArgs),
         cleanupStateNautilusFunction = buildOperator->cleanupStateNautilusFunction](const SliceStart sliceStart, const SliceEnd sliceEnd)
        {
            const auto createdSlices = createFunction(sliceStart, sliceEnd);
            return createdSlices;
        });

    const auto hashMap = operatorHandler->getSliceAndWindowStore().getSlicesOrCreate(timestamp, wrappedCreateFunction);
    INVARIANT(
        hashMap.size() == 1,
        "We expect exactly one slice for the given timestamp during the AggregationBuild, as we currently solely support "
        "slicing, but got {}",
        hashMap.size());

    /// Converting the slice to an AggregationSlice and returning the pointer to the hashmap
    const auto aggregationSlice = std::dynamic_pointer_cast<AggregationSlice>(hashMap[0]);
    INVARIANT(aggregationSlice != nullptr, "The slice should be an AggregationSlice in an AggregationBuild");
    return aggregationSlice->getHashMapPtrOrCreate(workerThreadId);
}

DataStructures::OffsetHashMapWrapper* getSerializableAggHashMapProxy(
    const SerializableAggregationOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const AggregationBuildPhysicalOperator* buildOperator)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");
    PRECONDITION(buildOperator != nullptr, "The build operator should not be null");

    printf("HASHMAP_PROXY: getSerializableAggHashMapProxy called - serializable aggregation active!\n");
    fflush(stdout);

    // Create serializable slice arguments similar to regular aggregation
    const CreateNewHashMapSliceArgs hashMapSliceArgs{
        {buildOperator->cleanupStateNautilusFunction},
        buildOperator->hashMapOptions.keySize,
        buildOperator->hashMapOptions.valueSize,
        buildOperator->hashMapOptions.pageSize,
        buildOperator->hashMapOptions.numberOfBuckets};
    
    printf("HASHMAP_PROXY: Creating with keySize=%zu, valueSize=%zu, buckets=%zu\n", 
           hashMapSliceArgs.keySize, hashMapSliceArgs.valueSize, hashMapSliceArgs.numberOfBuckets);
    fflush(stdout);
    
    auto wrappedCreateFunction(
        [createFunction = operatorHandler->getCreateNewSlicesFunction(hashMapSliceArgs)](const SliceStart sliceStart, const SliceEnd sliceEnd)
        {
            NES_TRACE("Creating slices for range [{}, {}]", sliceStart, sliceEnd);
            const auto createdSlices = createFunction(sliceStart, sliceEnd);
            NES_TRACE("Created {} slices", createdSlices.size());
            return createdSlices;
        });

    const auto slices = operatorHandler->getSliceAndWindowStore().getSlicesOrCreate(timestamp, wrappedCreateFunction);
    printf("HASHMAP_PROXY: Got %zu slices from getSlicesOrCreate\n", slices.size());
    INVARIANT(
        slices.size() == 1,
        "We expect exactly one slice for the given timestamp during the SerializableAggregationBuild, as we currently solely support "
        "slicing, but got {}",
        slices.size());

    // Converting the slice to a SerializableAggregationSlice and returning the pointer to the hashmap
    const auto serializableAggregationSlice = std::dynamic_pointer_cast<SerializableAggregationSlice>(slices[0]);
    INVARIANT(serializableAggregationSlice != nullptr, "The slice should be a SerializableAggregationSlice in a SerializableAggregationBuild");
    
    printf("HASHMAP_PROXY: Successfully cast to SerializableAggregationSlice\n");
    
    // Get the offset-based hashmap from the slice and return it directly
    auto* offsetHashMap = serializableAggregationSlice->getHashMapPtrOrCreate(workerThreadId);
    printf("HASHMAP_PROXY: Got hashmap pointer from slice: %p\n", static_cast<void*>(offsetHashMap));
    
    // Cast to OffsetHashMapWrapper since getHashMapPtrOrCreate returns Interface::HashMap*
    auto* wrapper = static_cast<DataStructures::OffsetHashMapWrapper*>(offsetHashMap);
    printf("HASHMAP_PROXY: Returning wrapper pointer: %p\n", static_cast<void*>(wrapper));
    fflush(stdout);
    return wrapper;
}

// Helper to check if operator handler is serializable
bool isSerializableAggHandler(OperatorHandler* handler)
{
    return handler->getIsSerializable();
}

// Helper to convert from ChainedHashMap FieldOffsets to OffsetHashMap OffsetFieldOffsets
std::vector<NES::Nautilus::Interface::MemoryProvider::OffsetFieldOffsets> convertToOffsetFieldOffsets(
    const std::vector<NES::Nautilus::Interface::MemoryProvider::FieldOffsets>& chainedFields)
{
    std::vector<NES::Nautilus::Interface::MemoryProvider::OffsetFieldOffsets> offsetFields;
    offsetFields.reserve(chainedFields.size());
    
    for (const auto& field : chainedFields) {
        offsetFields.emplace_back(NES::Nautilus::Interface::MemoryProvider::OffsetFieldOffsets{
            field.fieldIdentifier,
            field.type, 
            field.fieldOffset
        });
    }
    
    return offsetFields;
}

void AggregationBuildPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the corresponding slice so that we can update the aggregation states
    const auto timestamp = timeFunction->getTs(ctx, record);
    auto operatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerId);

    // Avoid EXEC logging in compiled mode to prevent tracing crashes

    // Use compile-time baked choice from rewrite rule to avoid JIT-time ambiguity
    bool isSerializable = useSerializableAggregation;

    // no printf logging in compiled mode

    /// Calling the key functions to add/update the keys to the record
    for (nautilus::static_val<uint64_t> i = 0; i < hashMapOptions.fieldKeys.size(); ++i)
    {
        const auto& [fieldIdentifier, type, fieldOffset] = hashMapOptions.fieldKeys[i];
        const auto& function = hashMapOptions.keyFunctions[i];
        const auto value = function.execute(record, ctx.pipelineMemoryProvider.arena);
        record.write(fieldIdentifier, value);
    }

    /// Use conditional execution based on handler type
    if (isSerializable)
    {
        /// Use OffsetHashMap for serializable aggregation
        const auto offsetHashMapWrapperPtr = nautilus::invoke(
            +[](OperatorHandler* handler, Timestamp ts, WorkerThreadId threadId, const AggregationBuildPhysicalOperator* buildOp) -> DataStructures::OffsetHashMapWrapper*
            {
                auto* serializableHandler = dynamic_cast<const SerializableAggregationOperatorHandler*>(handler);
                PRECONDITION(serializableHandler != nullptr, "Handler must be SerializableAggregationOperatorHandler");
                return getSerializableAggHashMapProxy(serializableHandler, ts, threadId, buildOp);
            },
            operatorHandler, timestamp, ctx.workerThreadId, nautilus::val<const AggregationBuildPhysicalOperator*>(this));

        auto convertedKeys = convertToOffsetFieldOffsets(hashMapOptions.fieldKeys);
        auto convertedValues = convertToOffsetFieldOffsets(hashMapOptions.fieldValues);
        Interface::OffsetHashMapRef hashMap(offsetHashMapWrapperPtr, convertedKeys, convertedValues);

        /// Finding or creating the entry for the provided record
        const auto hashMapEntry = hashMap.findOrCreateEntry(
            record,
            *hashMapOptions.hashFunction,
            [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
            {
                /// Initialize the aggregation states for new entries
                const Interface::OffsetHashMapRef::OffsetEntryRef entryRefReset(
                    static_cast<nautilus::val<DataStructures::OffsetEntry*>>(static_cast<nautilus::val<void*>>(entry)), 
                    offsetHashMapWrapperPtr, 
                    convertedKeys, 
                    convertedValues);
                auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
                nautilus::invoke(+[](AggregationState* ptr, size_t funcCount) {
                    printf("BUILD_RESET: Entry created, FuncCount=%zu, InitialStatePtr=%p\n", funcCount, ptr);
                }, state, nautilus::val<size_t>(aggregationPhysicalFunctions.size()));
                for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                {
                    auto stateSize = nautilus::val<size_t>(aggFunction->getSizeOfStateInBytes());
                    nautilus::invoke(+[](AggregationState* ptr, size_t size) {
                        printf("BUILD_RESET: StatePtr=%p, Size=%zu\n", ptr, size);
                    }, state, nautilus::val<size_t>(aggFunction->getSizeOfStateInBytes()));
                    aggFunction->reset(state, ctx.pipelineMemoryProvider);
                    auto stateData = nautilus::invoke(+[](AggregationState* ptr) -> uint64_t {
                        return *reinterpret_cast<uint64_t*>(ptr);
                    }, state);
                    NES_ERROR_EXEC("BUILD_RESET: After reset, State[0]=" << stateData);
                    state = state + aggFunction->getSizeOfStateInBytes();
                }
                fflush(stdout);
            },
            ctx.pipelineMemoryProvider.bufferProvider);

        /// Updating the aggregation states
        const Interface::OffsetHashMapRef::OffsetEntryRef entryRef(
            static_cast<nautilus::val<DataStructures::OffsetEntry*>>(static_cast<nautilus::val<void*>>(hashMapEntry)), 
            offsetHashMapWrapperPtr, 
            convertedKeys, 
            convertedValues);
        auto state = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
        nautilus::invoke(+[](AggregationState* ptr) {
            printf("BUILD_LIFT: Starting lift operations, StatePtr=%p\n", ptr);
        }, state);
        for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
        {
            auto stateBefore = nautilus::invoke(+[](AggregationState* ptr) -> uint64_t {
                return *reinterpret_cast<uint64_t*>(ptr);
            }, state);
            NES_ERROR_EXEC("BUILD_LIFT: Before=" << stateBefore);
            NES_DEBUG_EXEC("AGG_BUILD_STATE: before=" << stateBefore);
            aggFunction->lift(state, ctx.pipelineMemoryProvider, record);
            auto stateAfter = nautilus::invoke(+[](AggregationState* ptr) -> uint64_t {
                return *reinterpret_cast<uint64_t*>(ptr);
            }, state);
            NES_ERROR_EXEC("BUILD_LIFT: After=" << stateAfter);
            NES_DEBUG_EXEC("AGG_BUILD_STATE: after=" << stateAfter);
            state = state + aggFunction->getSizeOfStateInBytes();
        }
        fflush(stdout);
    }
    else
    {
        /// Use traditional ChainedHashMap for regular aggregation
        const auto hashMapPtr = invoke(getAggHashMapProxy, operatorHandler, timestamp, ctx.workerThreadId, nautilus::val<const AggregationBuildPhysicalOperator*>(this));
        Interface::ChainedHashMapRef hashMap(hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize);

        /// Finding or creating the entry for the provided record
        const auto hashMapEntry = hashMap.findOrCreateEntry(
            record,
            *hashMapOptions.hashFunction,
            [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
            {
                /// Initialize aggregation states for new entries
                const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(
                    static_cast<nautilus::val<Interface::ChainedHashMapEntry*>>(static_cast<nautilus::val<void*>>(entry)),
                    hashMapPtr,
                    hashMapOptions.fieldKeys,
                    hashMapOptions.fieldValues);
                auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
                for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                {
                    aggFunction->reset(state, ctx.pipelineMemoryProvider);
                    state = state + aggFunction->getSizeOfStateInBytes();
                }
            },
            ctx.pipelineMemoryProvider.bufferProvider);

        /// Updating the aggregation states
        const Interface::ChainedHashMapRef::ChainedEntryRef entryRef(
            static_cast<nautilus::val<Interface::ChainedHashMapEntry*>>(static_cast<nautilus::val<void*>>(hashMapEntry)),
            hashMapPtr,
            hashMapOptions.fieldKeys,
            hashMapOptions.fieldValues);
        auto state = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
        for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
        {
            aggFunction->lift(state, ctx.pipelineMemoryProvider, record);
            state = state + aggFunction->getSizeOfStateInBytes();
        }
    }
}

AggregationBuildPhysicalOperator::AggregationBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    std::unique_ptr<TimeFunction> timeFunction,
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationFunctions,
    HashMapOptions hashMapOptions,
    bool useSerializableAggregation)
    : WindowBuildPhysicalOperator(operatorHandlerId, std::move(timeFunction))
    , aggregationPhysicalFunctions(std::move(aggregationFunctions))
    , hashMapOptions(std::move(hashMapOptions))
    , useSerializableAggregation(useSerializableAggregation)
{
    nautilus::engine::Options options;
    options.setOption("engine.Compilation", false);
    const nautilus::engine::NautilusEngine nautilusEngine(options);

    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    cleanupStateNautilusFunction = std::make_shared<NautilusCleanupExec>(nautilusEngine.registerFunction(std::function(
        [copyOfFieldKeys = this->hashMapOptions.fieldKeys,
         copyOfFieldValues = this->hashMapOptions.fieldValues,
         copyOfEntriesPerPage = this->hashMapOptions.entriesPerPage,
         copyOfEntrySize = this->hashMapOptions.entrySize,
         copyOfAggregationFunctions = aggregationPhysicalFunctions](nautilus::val<Nautilus::Interface::HashMap*> hashMap)
        {
            const Interface::ChainedHashMapRef hashMapRef(
                hashMap, copyOfFieldKeys, copyOfFieldValues, copyOfEntriesPerPage, copyOfEntrySize);
            for (const auto entry : hashMapRef)
            {
                const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(entry, hashMap, copyOfFieldKeys, copyOfFieldValues);
                auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
                for (const auto& aggFunction : nautilus::static_iterable(copyOfAggregationFunctions))
                {
                    aggFunction->cleanup(state);
                    state = state + aggFunction->getSizeOfStateInBytes();
                }
            }
        })));
    ///NOLINTEND(performance-unnecessary-value-param)
}

}
