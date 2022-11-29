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

#include <atomic>

#include <Execution/RecordBuffer.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinBuild.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinOperatorHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>


namespace NES::Runtime::Execution::Operators {

void* getLocalHashTableFunctionCall(void* ptrOpHandler, size_t index, bool isLeftSide) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    LazyJoinOperatorHandler* opHandler = static_cast<LazyJoinOperatorHandler*>(ptrOpHandler);

    return static_cast<void*>(&opHandler->getCurrentWindow().getLocalHashTable(index, isLeftSide));
}


void* insertFunctionCall(void* ptrLocalHashTable, uint64_t key) {
    NES_ASSERT2_FMT(ptrLocalHashTable != nullptr, "ptrLocalHashTable should not be null");

    LocalHashTable* localHashTable = static_cast<LocalHashTable*>(ptrLocalHashTable);
    return localHashTable->insert(key);
}


void triggerJoinSink(void* ptrOpHandler, void* ptrPipelineCtx, void* ptrWorkerCtx, size_t workerIdIndex, bool isLeftSide) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    NES_ASSERT2_FMT(ptrWorkerCtx != nullptr, "worker context should not be null");


    auto opHandler = static_cast<LazyJoinOperatorHandler*>(ptrOpHandler);
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    auto workerCtx = static_cast<WorkerContext*>(ptrWorkerCtx);

    auto& sharedJoinHashTable = opHandler->getCurrentWindow().getSharedJoinHashTable(isLeftSide);
    auto& localHashTable = opHandler->getCurrentWindow().getLocalHashTable(workerIdIndex, isLeftSide);


    for (auto a = 0; a < NUM_PARTITIONS; ++a) {
        sharedJoinHashTable.insertBucket(a, localHashTable.getBucketLinkedList(a));
    }

    if (opHandler->getCurrentWindow().fetchSubBuild(1) == 1) {
        // If the last thread/worker is done with building, then start the second phase (comparing buckets)
        for (auto i = 0; i < NUM_PARTITIONS; ++i) {
            auto partitionId = i + 1;
            auto buffer = Runtime::TupleBuffer::wrapMemory(reinterpret_cast<uint8_t*>(partitionId), 1, opHandler);
            pipelineCtx->emitBuffer(buffer, reinterpret_cast<WorkerContext&>(workerCtx));
        }

        opHandler->createNewLocalHashTables();
    }
}


void incrementLastTupleTimeStampRuntime(void* lazyJoinBuildPtr) {
    LazyJoinBuild* lazyJoinBuild = static_cast<LazyJoinBuild*>(lazyJoinBuildPtr);

    lazyJoinBuild->incLastTupleTimeStamp(lazyJoinBuild->getWindowSize());
}



void LazyJoinBuild::execute(ExecutionContext& ctx, Record& record) const {

    // Get the global state
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
    auto localHashTableMemRef = Nautilus::FunctionCall("getLocalHashTableFunctionCall",
                                                       getLocalHashTableFunctionCall,
                                                       operatorHandlerMemRef,
                                                       ctx.getWorkerId(),
                                                       Value<Boolean>(isLeftSide));


    if (record.read("timestamp") != lastTupleTimeStamp) {

        auto entryMemRef = Nautilus::FunctionCall("insertFunctionCall", insertFunctionCall,
                                                     localHashTableMemRef, record.read(joinFieldName).as<UInt64>());

        for (auto& field : record.getAllFields()) {
            entryMemRef.store(record.read(field));
            entryMemRef = entryMemRef + record.read(field);
        }

    } else {
        Nautilus::FunctionCall("triggerJoinSink", triggerJoinSink, operatorHandlerMemRef, ctx.getPipelineContext(),
                               ctx.getWorkerContext(), ctx.getWorkerId(), Value<Boolean>(isLeftSide));

        Nautilus::FunctionCall("incrementLastTupleTimeStampRuntime", incrementLastTupleTimeStampRuntime, Value<MemRef>(this));
    }
}

LazyJoinBuild::LazyJoinBuild(uint64_t handlerIndex, bool isLeftSide, const std::string& joinFieldName, size_t windowSize)
    : handlerIndex(handlerIndex), isLeftSide(isLeftSide), joinFieldName(joinFieldName), windowSize(windowSize) {
    lastTupleTimeStamp = windowSize;
}

size_t LazyJoinBuild::getWindowSize() const { return windowSize; }

void LazyJoinBuild::incLastTupleTimeStamp(uint64_t increment) {}

} // namespace NES::Runtime::Execution::Operators