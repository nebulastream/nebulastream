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

#include <Aggregation/AggregationHashMapMerge.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

void createAggregationHashMap(ExecutionContext& executionCtx, const ChainedHashMapConfig& hashMapConfig, OwnedNautilusBuffer& hashMapBuffer)
{
    /// The proxy takes the numbers individually: a non-capturing lambda cannot receive the config itself.
    nautilus::invoke(
        +[](AbstractBufferProvider* bufferProvider,
            TupleBuffer* finalHashMapBuffer IF_PRECONDITION(, const uint64_t entrySize),
            const uint64_t numberOfBuckets,
            const uint64_t bloomBytes)
        {
            *finalHashMapBuffer = bufferProvider->getBufferBlocking();
            ChainedHashMap::init(*finalHashMapBuffer IF_PRECONDITION(, entrySize), numberOfBuckets, bloomBytes);
        },
        executionCtx.pipelineMemoryProvider.bufferProvider,
        hashMapBuffer.asArg() IF_PRECONDITION(, nautilus::val<uint64_t>{hashMapConfig.entrySize}),
        nautilus::val<uint64_t>{hashMapConfig.numberOfBuckets},
        nautilus::val<uint64_t>{hashMapConfig.bloomFilterMemAreaSize()});
}

namespace
{
/// Combines the aggregation states of one entry of source into target, inserting the key with reset states if target does not have it.
void combineEntry(
    ExecutionContext& executionCtx,
    const ChainedHashMapConfig& hashMapConfig,
    const std::vector<std::shared_ptr<AggregationPhysicalFunction>>& aggregationFunctions,
    ChainedHashMapRef& targetMap,
    const BorrowedNautilusBuffer& target,
    const BorrowedNautilusBuffer& source,
    const nautilus::val<ChainedHashMapEntry*>& entry)
{
    const ChainedHashMapRef::ChainedEntryRef entryRef{entry, source, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
    const auto tmpRecordKey = entryRef.getKey();

    /// Inserting the record key into the final/global hash map. If an entry for the key already exists, we have to combine the aggregation states
    /// We do this by iterating over the aggregation functions and combining all aggregation states into a global state.
    targetMap.insertOrUpdateEntry(
        entryRef.entryRef,
        [fieldKeys = hashMapConfig.fieldKeys,
         fieldValues = hashMapConfig.fieldValues,
         &executionCtx,
         &entryRef,
         &aggregationPhysicalFunctions = aggregationFunctions,
         pinnedFinalBuffer = target,
         source](const nautilus::val<AbstractHashMapEntry*>& entryOnUpdate)
        {
            /// Combining the aggregation states of the current entry with the aggregation states of the final hash map
            const ChainedHashMapRef::ChainedEntryRef entryRefOnInsert{entryOnUpdate, pinnedFinalBuffer, fieldKeys, fieldValues};
            auto globalState = static_cast<nautilus::val<AggregationState*>>(entryRefOnInsert.getValueMemArea());
            auto entryRefState = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
            for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
            {
                aggFunction->combine(
                    globalState, pinnedFinalBuffer, entryRefState, source, executionCtx.pipelineMemoryProvider);
                globalState = globalState + aggFunction->getSizeOfStateInBytes();
                entryRefState = entryRefState + aggFunction->getSizeOfStateInBytes();
            }
        },
        [fieldKeys = hashMapConfig.fieldKeys,
         fieldValues = hashMapConfig.fieldValues,
         &executionCtx,
         &entryRef,
         &aggregationPhysicalFunctions = aggregationFunctions,
         pinnedFinalBuffer = target,
         source](const nautilus::val<AbstractHashMapEntry*>& entryOnInsert)
        {
            /// If the entry for the provided key has not been seen by this hash map / worker thread, we need
            /// to create a new one and initialize the aggregation states. After that, we can combine the aggregation states.
            const ChainedHashMapRef::ChainedEntryRef entryRefOnInsert{entryOnInsert, pinnedFinalBuffer, fieldKeys, fieldValues};
            auto globalState = static_cast<nautilus::val<AggregationState*>>(entryRefOnInsert.getValueMemArea());
            auto entryRefStatePtr = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
            for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
            {
                /// In contrast to the lambda method above, we have to reset the aggregation state before combining it with the other state
                aggFunction->reset(globalState, pinnedFinalBuffer, executionCtx.pipelineMemoryProvider);
                aggFunction->combine(
                    globalState, pinnedFinalBuffer, entryRefStatePtr, source, executionCtx.pipelineMemoryProvider);
                globalState = globalState + aggFunction->getSizeOfStateInBytes();
                entryRefStatePtr = entryRefStatePtr + aggFunction->getSizeOfStateInBytes();
            }
        },
        executionCtx.pipelineMemoryProvider.bufferProvider);
}
}

void combineAggregationHashMap(
    ExecutionContext& executionCtx,
    const ChainedHashMapConfig& hashMapConfig,
    const std::vector<std::shared_ptr<AggregationPhysicalFunction>>& aggregationFunctions,
    const BorrowedNautilusBuffer& target,
    const BorrowedNautilusBuffer& source)
{
    ChainedHashMapRef targetMap{target, hashMapConfig};
    const ChainedHashMapRef currentMap{source, hashMapConfig};
    for (const auto entry : currentMap)
    {
        combineEntry(executionCtx, hashMapConfig, aggregationFunctions, targetMap, target, source, entry);
    }
}

void combineAggregationHashMapChains(
    ExecutionContext& executionCtx,
    const ChainedHashMapConfig& hashMapConfig,
    const std::vector<std::shared_ptr<AggregationPhysicalFunction>>& aggregationFunctions,
    const BorrowedNautilusBuffer& target,
    const BorrowedNautilusBuffer& source,
    const nautilus::val<uint64_t>& firstChain,
    const nautilus::val<uint64_t>& endChain)
{
    ChainedHashMapRef targetMap{target, hashMapConfig};
    for (nautilus::val<uint64_t> chain = firstChain; chain < endChain; chain = chain + nautilus::val<uint64_t>{1})
    {
        auto entry = nautilus::invoke(
            +[](const TupleBuffer* hashMapBuffer, const uint64_t chainIndex) -> ChainedHashMapEntry*
            {
                auto hashMap = ChainedHashMap::load(*hashMapBuffer);
                if (hashMap.getTotalNumberOfRecords() == 0)
                {
                    return nullptr;
                }
                return hashMap.getChain(chainIndex);
            },
            source.asArg(),
            chain);
        while (entry != nullptr)
        {
            combineEntry(executionCtx, hashMapConfig, aggregationFunctions, targetMap, target, source, entry);
            const ChainedHashMapRef::ChainedEntryRef entryRef{entry, source, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
            entry = entryRef.getNext();
        }
    }
}

}
