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


namespace NES::Runtime::Execution::Operators {

void* getWorkerHashTableFunctionCall(void* ptr, size_t index) {
    LazyJoinOperatorHandler* opHandler = static_cast<LazyJoinOperatorHandler*>(ptr);

    return static_cast<void*>(&opHandler->getWorkerHashTable(index));
}

void* insertFunctionCall(void* ptr, uint64_t key) {
    LocalHashTable* localHashTable = static_cast<LocalHashTable*>(ptr);
    return localHashTable->insert(key);
}


void triggerJoinSink(void* ptrOpHandler, void* ptrPipelineCtx, void* ptrWorkerCtx, size_t workerIdIndex, bool isLeftSide) {
    LazyJoinOperatorHandler* opHandler = static_cast<LazyJoinOperatorHandler*>(ptrOpHandler);
    WorkerContext* wcCtx = static_cast<WorkerContext*>(ptrWorkerCtx);
    PipelineExecutionContext* pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);

    auto& sharedJoinHashTable = opHandler->getSharedJoinHashTable(isLeftSide);
    auto& localHashTable = opHandler->getWorkerHashTable(workerIdIndex);



    for (auto a = 0; a < NUM_PARTITIONS; ++a) {
        sharedJoinHashTable.insertBucket(a, localHashTable.getBucketLinkedList(a));
    }

    if (operatorHandler->fetch_sub(1) == 1) {
        // If the last thread/worker is done with building, then start the second phase (comparing buckets)
        for (auto i = 0; i < NUM_PARTITIONS; ++i) {
            auto partitionId = i + 1;
            auto buffer = wcCtx->allocateTupleBuffer();
            buffer.store(partitionId);
            pipelineCtxemitBuffer(buffer)
        }
    }
}


void LazyJoinBuild::execute(ExecutionContext& ctx, Record& record) const {

    // Get the global state
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
    auto localHashTableMemRef = Nautilus::FunctionCall("getWorkerHashTableFunctionCall",
                                                       getWorkerHashTableFunctionCall,
                                                       operatorHandlerMemRef, ctx.getWorkerId());



    // TODO check and see how we can differentiate, if the window is done and we can go to the merge part of the lazyjoin
    if (record.read("timestamp") < 0) {
//        localHashTable.insert(record, joinFieldName);
         auto entryMemRef = Nautilus::FunctionCall("insertFunctionCall", insertFunctionCall,
                                          localHashTableMemRef, record.read(joinFieldName).as<UInt64>());

         for (auto& fields : record.getAllField()) {
             entryMemRef.store(record.read(fields));
         }
    } else {
        Nautilus::FunctionCall("triggerJoinSink", triggerJoinSink, operatorHandlerMemRef, ctx.getPipelineContext(),
                               ctx.getWorkerContext(), ctx.getWorkerId());
    }
}
LazyJoinBuild::LazyJoinBuild(const std::string& joinFieldName, uint64_t handlerIndex, bool isLeftSide)
    : joinFieldName(joinFieldName), handlerIndex(handlerIndex), isLeftSide(isLeftSide) {}

} // namespace NES::Runtime::Execution::Operators