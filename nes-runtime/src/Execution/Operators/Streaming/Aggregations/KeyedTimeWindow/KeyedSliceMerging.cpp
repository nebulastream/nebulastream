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

#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceStaging.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/StdInt.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void* createKeyedState(void* op, void* sliceMergeTaskPtr) {
    auto handler = static_cast<KeyedSliceMergingHandler*>(op);
    auto sliceMergeTask = static_cast<SliceMergeTask*>(sliceMergeTaskPtr);
    auto globalState = handler->createGlobalSlice(sliceMergeTask);
    // we give nautilus the ownership, thus deletePartition must be called.
    return globalState.release();
}

void* getKeyedSliceState(void* gs) {
    auto globalSlice = static_cast<KeyedSlice*>(gs);
    return globalSlice->getState().get();
}

void deleteSlice(void* gs) {
    auto globalSlice = static_cast<KeyedSlice*>(gs);
    delete globalSlice;
}

void* eraseKeyedPartition(void* op, uint64_t ts) {
    NES_DEBUG("eraseKeyedPartition {}", ts);
    auto handler = static_cast<KeyedSliceMergingHandler*>(op);
    auto partition = handler->getSliceStaging().erasePartition(ts);
    // we give nautilus the ownership, thus deletePartition must be called.
    return partition.release();
}

uint64_t getNumberOfKeyedPartition(void* p) {
    auto partition = static_cast<KeyedSliceStaging::Partition*>(p);
    return partition->partialStates.size();
}

void* getKeyedPartitionState(void* p, uint64_t index) {
    auto partition = static_cast<KeyedSliceStaging::Partition*>(p);
    return partition->partialStates[index].get();
}

void deleteKeyedPartition(void* p) {
    auto partition = static_cast<KeyedSliceStaging::Partition*>(p);
    delete partition;
}

void setupKeyedSliceMergingHandler(void* ss, void* ctx, uint64_t keySize, uint64_t valueSize) {
    auto handler = static_cast<KeyedSliceMergingHandler*>(ss);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext, keySize, valueSize);
}

KeyedSliceMerging::KeyedSliceMerging(uint64_t operatorHandlerIndex,
                                     const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
                                     const std::vector<PhysicalTypePtr>& keyDataTypes,
                                     const std::vector<std::string>& resultKeyFields,
                                     std::string startTsFieldName,
                                     std::string endTsFieldName,
                                     uint64_t resultOriginId)
    : operatorHandlerIndex(operatorHandlerIndex), aggregationFunctions(aggregationFunctions), resultKeyFields(resultKeyFields),
      keyDataTypes(keyDataTypes), startTsFieldName(std::move(startTsFieldName)), endTsFieldName(std::move(endTsFieldName)),
      keySize(0), valueSize(0), resultOriginId(resultOriginId) {
    for (auto& keyType : keyDataTypes) {
        keySize = keySize + keyType->size();
    }
    for (auto& function : aggregationFunctions) {
        valueSize = valueSize + function->getSize();
    }
}

void KeyedSliceMerging::setup(ExecutionContext& executionCtx) const {
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("setupSliceMergingHandler",
                           setupKeyedSliceMergingHandler,
                           globalOperatorHandler,
                           executionCtx.getPipelineContext(),
                           Value<UInt64>(keySize),
                           Value<UInt64>(valueSize));
    this->child->setup(executionCtx);
}

void KeyedSliceMerging::open(ExecutionContext& ctx, RecordBuffer& buffer) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local slice store and store the pointer/memref to it in the execution context as the local slice store state.
    this->child->open(ctx, buffer);

    // 1. get the operator handler and extract the slice information that should be combined.
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto sliceMergeTask = buffer.getBuffer();
    Value<> startSliceTs = getMember(sliceMergeTask, SliceMergeTask, startSlice).load<UInt64>();
    Value<> endSliceTs = getMember(sliceMergeTask, SliceMergeTask, endSlice).load<UInt64>();
    Value<> sequenceNumber = getMember(sliceMergeTask, SliceMergeTask, sequenceNumber).load<UInt64>();

    // 2. initialize global slice state, which is represented by a chained hashtable
    auto globalSlice = Nautilus::FunctionCall("createKeyedState", createKeyedState, globalOperatorHandler, sliceMergeTask);
    auto globalSliceState = Nautilus::FunctionCall("getKeyedSliceState", getKeyedSliceState, globalSlice);
    auto globalHashTable = Interface::ChainedHashMapRef(globalSliceState, keyDataTypes, keySize, valueSize);

    // 3. combine thread local slices and append them to the global slice store
    combineThreadLocalSlices(globalOperatorHandler, globalHashTable, endSliceTs);

    // 4. emit global slice when we have a tumbling window.
    emitWindow(ctx, startSliceTs, endSliceTs, sequenceNumber, globalHashTable);

    // 5. clean slice
    Nautilus::FunctionCall("deleteSlice", deleteSlice, globalSlice);
}

void KeyedSliceMerging::combineThreadLocalSlices(Value<MemRef>& globalOperatorHandler,
                                                 Interface::ChainedHashMapRef& globalHashTable,
                                                 Value<>& endSliceTs) const {
    // combine all thread local partitions into the global slice hash map
    auto partition =
        Nautilus::FunctionCall("eraseKeyedPartition", eraseKeyedPartition, globalOperatorHandler, endSliceTs.as<UInt64>());
    auto numberOfPartitions = Nautilus::FunctionCall("getNumberOfKeyedPartition", getNumberOfKeyedPartition, partition);
    NES_DEBUG("combine {} slice stores for slice {}", numberOfPartitions->toString(), endSliceTs->toString());

    for (Value<UInt64> i = 0_u64; i < numberOfPartitions; i = i + 1_u64) {
        auto partitionState = Nautilus::FunctionCall("getKeyedPartitionState", getKeyedPartitionState, partition, i);
        auto partitionStateHashTable = Interface::ChainedHashMapRef(partitionState, keyDataTypes, keySize, valueSize);
        mergeHashTable(globalHashTable, partitionStateHashTable);
    }
    Nautilus::FunctionCall("deleteKeyedPartition", deleteKeyedPartition, partition);
}

void KeyedSliceMerging::mergeHashTable(Interface::ChainedHashMapRef& globalSliceHashMap,
                                       Interface::ChainedHashMapRef& threadLocalSliceHashMap) const {
    // inserts all entries from the thread local hash map into the global hash map.
    // 1. iterate over all entries in thread local hash map.
    for (const auto& threadLocalEntry : threadLocalSliceHashMap) {
        // 2. insert entry or update existing one with same key.
        globalSliceHashMap.insertEntryOrUpdate(threadLocalEntry, [&](auto& globalEntry) {
            // 2b. update aggregation if the entry was already existing in the global hash map
            auto key = threadLocalEntry.getKeyPtr();
            auto threadLocalValue = threadLocalEntry.getValuePtr();
            Value<MemRef> globalValue = globalEntry.getValuePtr();
            NES_TRACE("merge key {} th {} gb {}", key.load<UInt64>()->toString(),
                      threadLocalValue.load<UInt64>()->toString(),
                      globalValue.load<UInt64>()->toString())
            // 2c. apply aggregation functions and combine the values
            for (const auto& function : aggregationFunctions) {
                function->combine(globalValue, threadLocalValue);
                threadLocalValue = threadLocalValue + function->getSize();
                NES_TRACE("result value {}", globalValue.load<UInt64>()->toString());
                globalValue = globalValue + function->getSize();
            }

        });
    }
}

void KeyedSliceMerging::emitWindow(ExecutionContext& ctx,
                                   Value<>& windowStart,
                                   Value<>& windowEnd,
                                   Value<>& sequenceNumber,
                                   Interface::ChainedHashMapRef& globalSliceHashMap) const {
    NES_DEBUG("Emit window: {}-{}-{}", windowStart.as<UInt64>()->toString(), windowEnd.as<UInt64>()->toString(), sequenceNumber.as<UInt64>()->toString());
    ctx.setWatermarkTs(windowEnd.as<UInt64>());
    ctx.setOrigin(resultOriginId);
    ctx.setSequenceNumber(sequenceNumber.as<UInt64>());
    // create the final window content and emit it to the downstream operator
    for (const auto& globalEntry : globalSliceHashMap) {
        Record resultWindow;
        // write window start and end to result record
        resultWindow.write(startTsFieldName, windowStart);
        resultWindow.write(endTsFieldName, windowEnd);
        // load keys and write them to result record
        auto sliceKeys = globalEntry.getKeyPtr();
        for (size_t i = 0; i < resultKeyFields.size(); ++i) {
            Value<> value = sliceKeys.load<UInt64>();
            resultWindow.write(resultKeyFields[i], value);
            sliceKeys = sliceKeys + keyDataTypes[i]->size();
        }
        // load values and write them to result record
        auto sliceValue = globalEntry.getValuePtr();
        for (const auto& aggregationFunction : aggregationFunctions) {
            aggregationFunction->lower(sliceValue, resultWindow);
            sliceValue = sliceValue + aggregationFunction->getSize();
        }
        child->execute(ctx, resultWindow);
    }
}

}// namespace NES::Runtime::Execution::Operators