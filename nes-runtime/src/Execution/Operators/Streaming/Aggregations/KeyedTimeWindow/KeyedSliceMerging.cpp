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
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void* createGlobalState2(void* op, void* sliceMergeTaskPtr) {
    auto handler = static_cast<KeyedSliceMergingHandler*>(op);
    auto sliceMergeTask = static_cast<SliceMergeTask*>(sliceMergeTaskPtr);
    auto globalState = handler->createGlobalSlice(sliceMergeTask);
    // we give nautilus the ownership, thus deletePartition must be called.
    return globalState.release();
}

void* getGlobalSliceState2(void* gs) {
    auto globalSlice = static_cast<KeyedSlice*>(gs);
    return globalSlice->getState().get();
}

void* erasePartition2(void* op, uint64_t ts) {
    auto handler = static_cast<KeyedSliceMergingHandler*>(op);
    auto partition = handler->getSliceStaging().erasePartition(ts);
    // we give nautilus the ownership, thus deletePartition must be called.
    return partition.release();
}

uint64_t getNumberOfPartition2(void* p) {
    auto partition = static_cast<KeyedSliceStaging::Partition*>(p);
    return partition->partialStates.size();
}

void* getPartitionState2(void* p, uint64_t index) {
    auto partition = static_cast<KeyedSliceStaging::Partition*>(p);
    return partition->partialStates[index].get();
}

void deletePartition2(void* p) {
    auto partition = static_cast<KeyedSliceStaging::Partition*>(p);
    delete partition;
}

void setupSliceMergingHandler2(void* ss, void* ctx, uint64_t keySize, uint64_t valueSize) {
    auto handler = static_cast<KeyedSliceMergingHandler*>(ss);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext, keySize, valueSize);
}

KeyedSliceMerging::KeyedSliceMerging(uint64_t operatorHandlerIndex,
                                     const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
                                     const std::vector<std::string>& aggregationResultExpressions,
                                     const std::vector<PhysicalTypePtr>& keyDataTypes,
                                     const std::vector<std::string>& resultKeyFields,
                                     const std::string& startTsFieldName,
                                     const std::string& endTsFieldName)
    : operatorHandlerIndex(operatorHandlerIndex), aggregationFunctions(aggregationFunctions),
      aggregationResultExpressions(aggregationResultExpressions), resultKeyFields(resultKeyFields), keyDataTypes(keyDataTypes),
      startTsFieldName(startTsFieldName), endTsFieldName(endTsFieldName), keySize(0), valueSize(0) {
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
                           setupSliceMergingHandler2,
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
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto sliceMergeTask = buffer.getBuffer();
    Value<> startSliceTs = (sliceMergeTask + (uint64_t) offsetof(SliceMergeTask, startSlice)).as<MemRef>().load<UInt64>();
    Value<> endSliceTs = (sliceMergeTask + (uint64_t) offsetof(SliceMergeTask, endSlice)).as<MemRef>().load<UInt64>();
    // 2. load the thread local slice store according to the worker id.
    auto globalSliceState = combineThreadLocalSlices(globalOperatorHandler, sliceMergeTask, endSliceTs);
    // emit global slice when we have a tumbling window.
    emitWindow(ctx, startSliceTs, endSliceTs, globalSliceState);
}

Value<MemRef> KeyedSliceMerging::combineThreadLocalSlices(Value<MemRef>& globalOperatorHandler,
                                                          Value<MemRef>& sliceMergeTask,
                                                          Value<>& endSliceTs) const {
    auto globalSlice = Nautilus::FunctionCall("createGlobalState", createGlobalState2, globalOperatorHandler, sliceMergeTask);
    auto globalSliceState = Nautilus::FunctionCall("getGlobalSliceState", getGlobalSliceState2, globalSlice);
    auto globalHashTable = Interface::ChainedHashMapRef(globalSliceState, keySize, valueSize);
    auto partition = Nautilus::FunctionCall("erasePartition", erasePartition2, globalOperatorHandler, endSliceTs.as<UInt64>());
    auto numberOfPartitions = Nautilus::FunctionCall("getNumberOfPartitions", getNumberOfPartition2, partition);
    for (Value<UInt64> i = (uint64_t) 0; i < numberOfPartitions; i = i + (uint64_t) 1) {
        auto partitionState = Nautilus::FunctionCall("getPartitionState", getPartitionState2, partition, i);
        auto partitionStateHashTable = Interface::ChainedHashMapRef(partitionState, keySize, valueSize);
        mergeHashTable(globalHashTable, partitionStateHashTable);
    }
    Nautilus::FunctionCall("deletePartition", deletePartition2, partition);
    return globalSlice;
}

void KeyedSliceMerging::mergeHashTable(Interface::ChainedHashMapRef& globalHashTable,
                                       Interface::ChainedHashMapRef& threadLocalHashTable) const {
    for (const auto& thEntry : threadLocalHashTable) {
        globalHashTable.insertEntryOrUpdate(thEntry, [&](auto& gEntry) {
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
                                   Value<MemRef>& globalSlice) const {
    auto globalSliceState = Nautilus::FunctionCall("getGlobalSliceState", getGlobalSliceState2, globalSlice);
    auto globalHashTable = Interface::ChainedHashMapRef(globalSliceState, keySize, valueSize);

    for (const auto& globalEntry : globalHashTable) {
        Record resultWindow;
        // write window start and end to result record
        resultWindow.write(startTsFieldName, windowStart);
        resultWindow.write(endTsFieldName, windowEnd);
        // load keys and write them to result record
        auto sliceKeys = globalEntry.getKeyPtr();
        for (size_t i = 0; i < resultKeyFields.size(); ++i) {
            Value<> value = sliceKeys.load<UInt64>();
            resultWindow.write(resultKeyFields[i], value);
            sliceKeys  = sliceKeys + keyDataTypes[i]->size();
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