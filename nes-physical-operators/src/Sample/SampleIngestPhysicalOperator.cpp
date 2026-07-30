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

#include <Sample/SampleIngestPhysicalOperator.hpp>

#include <memory>
#include <utility>
#include <Identifiers/Identifier.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/Record.hpp>
#include <Sample/SampleFields.hpp>
#include <Sample/SampleOperatorHandler.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <function.hpp>

namespace NES
{

SampleIngestPhysicalOperator::SampleIngestPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    std::unique_ptr<TimeFunction> timeFunction,
    std::unique_ptr<SliceStoreRef> sliceStoreRef,
    std::shared_ptr<SampleAggregationPhysicalFunction> function,
    HashMapOptions hashMapOptions)
    : WindowBuildPhysicalOperator(operatorHandlerId, std::move(timeFunction), std::move(sliceStoreRef))
    , function(std::move(function))
    , hashMapOptions(std::move(hashMapOptions))
{
}

void SampleIngestPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    WindowBuildPhysicalOperator::setup(executionCtx, compilationContext);

    auto* const operatorHandler = dynamic_cast<SampleOperatorHandler*>(
        nautilus::details::RawValueResolver<OperatorHandler*>::getRawValue(executionCtx.getGlobalOperatorHandler(operatorHandlerId)));
    if (bool expectedValue = false; operatorHandler->setupAlreadyCalled.compare_exchange_strong(expectedValue, true))
    {
        operatorHandler->cleanupStateNautilusFunction
            = std::make_shared<CreateNewHashMapSliceArgs::NautilusCleanupExec>(compilationContext.registerFunction(
                std::function(
                    [copyOfHashMapOptions = hashMapOptions, copyOfFunction = function](nautilus::val<HashMap*> hashMap)
                    {
                        const ChainedHashMapRef hashMapRef(
                            hashMap,
                            copyOfHashMapOptions.fieldKeys,
                            copyOfHashMapOptions.fieldValues,
                            copyOfHashMapOptions.entriesPerPage,
                            copyOfHashMapOptions.entrySize);
                        for (const auto entry : hashMapRef)
                        {
                            const ChainedHashMapRef::ChainedEntryRef entryRefReset(
                                entry, hashMap, copyOfHashMapOptions.fieldKeys, copyOfHashMapOptions.fieldValues);
                            const auto state = static_cast<nautilus::val<SampleAggregationState*>>(entryRefReset.getValueMemArea());
                            copyOfFunction->cleanup(state);
                        }
                    }),
                "cleanupSampleState"));
    }
}

void SampleIngestPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    static const auto procTsField = Identifier::parse(SAMPLE_PROC_TS_FIELD);
    record.write(procTsField, ctx.currentTs.convertToValue());

    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(ctx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();
    const auto timestamp = timeFunction->getTs(ctx, record);
    const auto hashMapPtr
        = sliceStoreRef->getDataStructureRef(timestamp, ctx.workerThreadId, operatorHandler, ctx.pipelineMemoryProvider.bufferProvider);

    ChainedHashMapRef hashMap(
        hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize);

    const auto hashMapEntry = hashMap.findOrCreateEntry(
        record,
        *hashMapOptions.hashFunction,
        [&](const nautilus::val<AbstractHashMapEntry*>& entry)
        {
            const ChainedHashMapRef::ChainedEntryRef entryRefReset(entry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
            const auto state = static_cast<nautilus::val<SampleAggregationState*>>(entryRefReset.getValueMemArea());
            function->reset(state, ctx.pipelineMemoryProvider);
        },
        ctx.pipelineMemoryProvider.bufferProvider);

    const ChainedHashMapRef::ChainedEntryRef entryRef(hashMapEntry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
    const auto state = static_cast<nautilus::val<SampleAggregationState*>>(entryRef.getValueMemArea());
    function->lift(state, ctx.pipelineMemoryProvider, record);
}

}
