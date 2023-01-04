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
#include <cstdint>

#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/DataStructure/LocalHashTable.hpp>
#include <Execution/Operators/Streaming/Join/JoinPhases/StreamJoinBuild.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime::Execution::Operators {

void* getLocalHashTableFunctionCall(void* ptrOpHandler, size_t index, bool isLeftSide) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    StreamJoinOperatorHandler* opHandler = static_cast<StreamJoinOperatorHandler*>(ptrOpHandler);

    return static_cast<void*>(opHandler->getWindowToBeFilled(isLeftSide).getLocalHashTable(index, isLeftSide));
}

void* insertFunctionCall(void* ptrLocalHashTable, uint64_t key) {
    NES_ASSERT2_FMT(ptrLocalHashTable != nullptr, "ptrLocalHashTable should not be null");

    LocalHashTable* localHashTable = static_cast<LocalHashTable*>(ptrLocalHashTable);

    return localHashTable->insert(key);
}

void triggerJoinSink(void* ptrOpHandler, void* ptrPipelineCtx, void* ptrWorkerCtx, bool isLeftSide) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    NES_ASSERT2_FMT(ptrWorkerCtx != nullptr, "worker context should not be null");

    auto opHandler = static_cast<StreamJoinOperatorHandler*>(ptrOpHandler);
    auto pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
    auto workerCtx = static_cast<WorkerContext*>(ptrWorkerCtx);

    auto& sharedJoinHashTable = opHandler->getWindowToBeFilled(isLeftSide).getSharedJoinHashTable(isLeftSide);
    auto localHashTable = opHandler->getWindowToBeFilled(isLeftSide).getLocalHashTable(workerCtx->getId(), isLeftSide);

    for (auto a = 0UL; a < opHandler->getNumPartitions(); ++a) {
        sharedJoinHashTable.insertBucket(a, localHashTable->getBucketLinkedList(a));
    }

    // If the last thread/worker is done with building, then start the second phase (comparing buckets)
    if (opHandler->getWindowToBeFilled(isLeftSide).fetchSubBuild(1) == 1) {
        for (auto i = 0UL; i < opHandler->getNumPartitions(); ++i) {

            auto buffer = workerCtx->allocateTupleBuffer();
            auto bufferAs = buffer.getBuffer<JoinPartitionIdTumpleStamp>();

            bufferAs->partitionId = i;
            bufferAs->lastTupleTimeStamp = opHandler->getWindowToBeFilled(isLeftSide).getLastTupleTimeStamp();

            pipelineCtx->emitBuffer(buffer, reinterpret_cast<WorkerContext&>(workerCtx));
        }
    }

    opHandler->incLastTupleTimeStamp(opHandler->getWindowSize(), isLeftSide);
    opHandler->createNewWindow(isLeftSide);
}

uint64_t getLastTupleWindow(void* ptrOpHandler, bool isLeftSide) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");

    auto opHandler = static_cast<StreamJoinOperatorHandler*>(ptrOpHandler);
    return opHandler->getLastTupleTimeStamp(isLeftSide);
}

void StreamJoinBuild::execute(ExecutionContext& ctx, Record& record) const {

    // Get the global state
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);
    auto lastTupleWindowRef =
        Nautilus::FunctionCall("getLastTupleWindow", getLastTupleWindow, operatorHandlerMemRef, Value<Boolean>(isLeftSide));

    if (record.read(timeStampField) > lastTupleWindowRef) {
        Nautilus::FunctionCall("triggerJoinSink",
                               triggerJoinSink,
                               operatorHandlerMemRef,
                               ctx.getPipelineContext(),
                               ctx.getWorkerContext(),
                               Value<Boolean>(isLeftSide));
    }

    auto localHashTableMemRef = Nautilus::FunctionCall("getLocalHashTableFunctionCall",
                                                       getLocalHashTableFunctionCall,
                                                       operatorHandlerMemRef,
                                                       ctx.getWorkerId(),
                                                       Value<Boolean>(isLeftSide));

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto entryMemRef = Nautilus::FunctionCall("insertFunctionCall",
                                              insertFunctionCall,
                                              localHashTableMemRef,
                                              record.read(joinFieldName).as<UInt64>());
    for (auto& field : schema->fields) {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

        entryMemRef.store(record.read(fieldName));
        entryMemRef = entryMemRef + fieldType->size();
    }
}

StreamJoinBuild::StreamJoinBuild(uint64_t handlerIndex,
                                 bool isLeftSide,
                                 const std::string& joinFieldName,
                                 const std::string& timeStampField,
                                 SchemaPtr schema)
    : handlerIndex(handlerIndex), isLeftSide(isLeftSide), joinFieldName(joinFieldName), timeStampField(timeStampField),
      schema(schema) {}

}// namespace NES::Runtime::Execution::Operators