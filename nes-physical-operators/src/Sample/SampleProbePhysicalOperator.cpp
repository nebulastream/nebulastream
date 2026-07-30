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

#include <Sample/SampleProbePhysicalOperator.hpp>

#include <cstdint>
#include <utility>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

SampleProbePhysicalOperator::SampleProbePhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    WindowMetaData windowMetaData,
    std::shared_ptr<SampleAggregationPhysicalFunction> function,
    std::vector<QualifiedUnboundField> outputFields,
    Record::RecordFieldIdentifier timestampFieldName,
    HashMapOptions hashMapOptions)
    : WindowProbePhysicalOperator(operatorHandlerId, std::move(windowMetaData))
    , function(std::move(function))
    , outputFields(std::move(outputFields))
    , timestampFieldName(std::move(timestampFieldName))
    , hashMapOptions(std::move(hashMapOptions))
{
}

void SampleProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    openChild(executionCtx, recordBuffer);

    const auto emittedWindowRef = static_cast<nautilus::val<EmittedAggregationWindow*>>(recordBuffer.getMemArea());
    const auto numberOfHashMaps
        = readValueFromMemRef<uint64_t>(getMemberRef(emittedWindowRef, &EmittedAggregationWindow::numberOfHashMaps));
    const auto windowInfoRef = getMemberRef(emittedWindowRef, &EmittedAggregationWindow::windowInfo);
    const nautilus::val<Timestamp> windowStart{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    auto hashMapRefs = readValueFromMemRef<HashMap**>(getMemberRef(emittedWindowRef, &EmittedAggregationWindow::hashMaps));
    auto finalHashMapPtr = readValueFromMemRef<HashMap*>(getMemberRef(emittedWindowRef, &EmittedAggregationWindow::finalHashMapPtr));

    ChainedHashMapRef finalHashMap(
        finalHashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize);
    const auto finalEntry = finalHashMap.findOrCreateEntry(
        Record{},
        *hashMapOptions.hashFunction,
        [&](const nautilus::val<AbstractHashMapEntry*>& entryOnInsert)
        {
            const ChainedHashMapRef::ChainedEntryRef entryRefOnInsert(
                entryOnInsert, finalHashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
            const auto state = static_cast<nautilus::val<SampleAggregationState*>>(entryRefOnInsert.getValueMemArea());
            function->reset(state, executionCtx.pipelineMemoryProvider);
        },
        executionCtx.pipelineMemoryProvider.bufferProvider);
    const ChainedHashMapRef::ChainedEntryRef finalEntryRef(
        finalEntry, finalHashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
    const auto combinedState = static_cast<nautilus::val<SampleAggregationState*>>(finalEntryRef.getValueMemArea());

    for (nautilus::val<uint64_t> curHashMap = 0; curHashMap < numberOfHashMaps; ++curHashMap)
    {
        const nautilus::val<HashMap*> hashMapPtr = hashMapRefs[curHashMap];
        const ChainedHashMapRef currentMap(
            hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize);
        for (const auto entry : currentMap)
        {
            const ChainedHashMapRef::ChainedEntryRef entryRef(entry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
            const auto entryState = static_cast<nautilus::val<SampleAggregationState*>>(entryRef.getValueMemArea());
            function->combine(combinedState, entryState, executionCtx.pipelineMemoryProvider);
        }
    }

    const auto empty = function->isEmpty(combinedState);
    const auto loweredRecord = function->lower(combinedState, executionCtx.pipelineMemoryProvider);

    Record outputRecord;
    for (const auto& field : nautilus::static_iterable(outputFields))
    {
        const auto fieldIdentifier = field.getFullyQualifiedName();
        const auto type = field.getDataType();
        const auto realValue = loweredRecord.read(fieldIdentifier);
        if (fieldIdentifier == timestampFieldName)
        {
            outputRecord.write(
                fieldIdentifier, VarVal::select(empty, VarVal{windowStart.convertToValue()}.castToType(type.type), realValue));
        }
        else if (type.nullable)
        {
            const VarVal nullPlaceholder{nautilus::val<uint64_t>(0), true, true};
            outputRecord.write(fieldIdentifier, VarVal::select(empty, nullPlaceholder.castToType(type.type), realValue));
        }
        else
        {
            outputRecord.write(fieldIdentifier, realValue);
        }
    }
    function->cleanup(combinedState);
    executeChild(executionCtx, outputRecord);

    nautilus::invoke(
        +[](EmittedAggregationWindow* emittedAggregationWindow) { emittedAggregationWindow->finalHashMap.reset(); }, emittedWindowRef);
}

}
