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
#include <Nautilus/Interface/DataTypes/Utils.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
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

void* eraseKeyedPartition(void* op, uint64_t ts) {
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
                                     const std::vector<std::string>& aggregationResultExpressions,
                                     const std::vector<PhysicalTypePtr>& keyDataTypes,
                                     const std::vector<std::string>& resultKeyFields,
                                     std::string startTsFieldName,
                                     std::string endTsFieldName)
    : operatorHandlerIndex(operatorHandlerIndex), aggregationFunctions(aggregationFunctions),
      aggregationResultExpressions(aggregationResultExpressions), resultKeyFields(resultKeyFields), keyDataTypes(keyDataTypes),
      startTsFieldName(std::move(startTsFieldName)), endTsFieldName(std::move(endTsFieldName)), keySize(0), valueSize(0) {
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

    // 2. initialize global slice state, which is represented by a chained hashtable
    auto globalSlice = Nautilus::FunctionCall("createKeyedState", createKeyedState, globalOperatorHandler, sliceMergeTask);
    auto globalSliceState = Nautilus::FunctionCall("getKeyedSliceState", getKeyedSliceState, globalSlice);
    auto globalHashTable = Interface::ChainedHashMapRef(globalSliceState, keyDataTypes, keySize, valueSize);

    // 3. combine thread local slices and append them to the global slice store
    combineThreadLocalSlices(globalOperatorHandler, globalHashTable, endSliceTs);

    // 4. emit global slice when we have a tumbling window.
    emitWindow(ctx, startSliceTs, endSliceTs, globalHashTable);
}

void KeyedSliceMerging::combineThreadLocalSlices(Value<MemRef>& globalOperatorHandler,
                                                 Interface::ChainedHashMapRef& globalHashTable,
                                                 Value<>& endSliceTs) const {
    // combine all thread local partitions into the global slice hash map
    auto partition = Nautilus::FunctionCall("eraseKeyedPartition", eraseKeyedPartition, globalOperatorHandler, endSliceTs.as<UInt64>());
    auto numberOfPartitions = Nautilus::FunctionCall("getNumberOfKeyedPartition", getNumberOfKeyedPartition, partition);
    for (Value<UInt64> i = (uint64_t) 0; i < numberOfPartitions; i = i + (uint64_t) 1) {
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
    for (const auto& thEntry : threadLocalSliceHashMap) {
        // 2. insert entry or update existing one with same key.
        globalSliceHashMap.insertEntryOrUpdate(thEntry, [&](auto& gEntry) {
            // 2b. update aggregation if the entry was already existing in the global hash map
            auto thValuePtr = thEntry.getValuePtr();
            auto gValuePtr = gEntry.getValuePtr();
            for (const auto& function : aggregationFunctions) {
                function->combine(thValuePtr, gValuePtr);
                thValuePtr = thValuePtr + function->getSize();
                gValuePtr = gValuePtr + function->getSize();
            }
        });
    }
}

void KeyedSliceMerging::emitWindow(ExecutionContext& ctx,
                                   Value<>& windowStart,
                                   Value<>& windowEnd,
                                   Interface::ChainedHashMapRef& globalSliceHashMap) const {
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
        for (size_t i = 0; i < aggregationFunctions.size(); ++i) {
            auto finalAggregationValue = aggregationFunctions[i]->lower(sliceValue);
            resultWindow.write(aggregationResultExpressions[i], finalAggregationValue);
            sliceValue = sliceValue + aggregationFunctions[i]->getSize();
        }
        child->execute(ctx, resultWindow);
    }
}

}// namespace NES::Runtime::Execution::Operators