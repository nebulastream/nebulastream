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
#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJSink.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/DataStructure/NLJWindow.hpp>

namespace NES::Runtime::Execution::Operators {

void* getNLJWindowRefProxy(void* ptrOpHandler, void* windowIdentifierPtr) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(windowIdentifierPtr != nullptr, "joinPartitionTimeStampPtr should not be null");

    auto opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    auto windowIdentifier = *static_cast<uint64_t*>(windowIdentifierPtr);
    auto window = opHandler->getWindowByWindowIdentifier(windowIdentifier);
    if (window.has_value()) {
        return window.value().get();
    } else {
        return nullptr;
    }
}

void deleteWindowProxyForNestedLoopJoin(void* ptrOpHandler, void* windowIdentifierPtr) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(windowIdentifierPtr != nullptr, "joinPartitionTimeStampPtr should not be null");

    auto opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    auto windowIdentifier = *static_cast<uint64_t*>(windowIdentifierPtr);
    opHandler->deleteWindow(windowIdentifier);
}

uint64_t getWindowStartProxyForNestedLoopJoin(void* ptrOpHandler, void* windowIdentifierPtr) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(windowIdentifierPtr != nullptr, "joinPartitionTimeStampPtr should not be null");

    auto opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    auto windowIdentifier = *static_cast<uint64_t*>(windowIdentifierPtr);

    return opHandler->getWindowStartEnd(windowIdentifier).first;
}

uint64_t getWindowEndProxyForNestedLoopJoin(void* ptrOpHandler, void* windowIdentifierPtr) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(windowIdentifierPtr != nullptr, "joinPartitionTimeStampPtr should not be null");

    auto opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    auto windowIdentifier = *static_cast<uint64_t*>(windowIdentifierPtr);

    return opHandler->getWindowStartEnd(windowIdentifier).second;
}

uint64_t getSequenceNumberProxyForNestedLoopJoin(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");

    auto opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    return opHandler->getNextSequenceNumber();
}
uint64_t getOriginIdProxyForNestedLoopJoin(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");

    auto opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    return opHandler->getOperatorId();
}

void NLJSink::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    if (hasChild()) {
        child->open(ctx, recordBuffer);
    }

    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto windowIdentifierMemRef = recordBuffer.getBuffer();
    auto windowReference = Nautilus::FunctionCall("getNLJWindowRefProxy", getNLJWindowRefProxy,
                                                  operatorHandlerMemRef, windowIdentifierMemRef);
    auto leftPagedVectorRef = Nautilus::FunctionCall("getNLJPagedVectorProxy", getNLJPagedVectorProxy,
                                                     windowReference, Nautilus::Value<UInt64>((uint64_t) 0),
                                                     Nautilus::Value<Nautilus::Boolean>(/*isLeftSide*/ true));
    auto rightPagedVectorRef = Nautilus::FunctionCall("getNLJPagedVectorProxy", getNLJPagedVectorProxy,
                                                      windowReference, Nautilus::Value<UInt64>((uint64_t) 0),
                                                      Nautilus::Value<Nautilus::Boolean>(/*isLeftSide*/ false));
    Nautilus::Value<UInt64> leftEntrySize = Nautilus::FunctionCall("getEntrySizePagedVector", getEntrySizePagedVector,
                                                                   leftPagedVectorRef);
    Nautilus::Value<UInt64> rightEntrySize = Nautilus::FunctionCall("getEntrySizePagedVector", getEntrySizePagedVector,
                                                                    rightPagedVectorRef);
    Nautilus::Value<UInt64> leftPageSize = Nautilus::FunctionCall("getPageSizePagedVector", getPageSizePagedVector,
                                                                  leftPagedVectorRef);
    Nautilus::Value<UInt64> rightPageSize = Nautilus::FunctionCall("getPageSizePagedVector", getPageSizePagedVector,
                                                                   rightPagedVectorRef);

    Nautilus::Interface::PagedVectorRef leftPagedVector(leftPagedVectorRef, leftEntrySize.getValue().getValue(),
                                                        leftPageSize.getValue().getValue());
    Nautilus::Interface::PagedVectorRef rightPagedVector(rightPagedVectorRef, rightEntrySize.getValue().getValue(),
                                                         rightPageSize.getValue().getValue());

    // TODO ask Philipp why do I need this here and can not use auto?
    Value<Any> windowStart = Nautilus::FunctionCall("getWindowStartProxyForNestedLoopJoin",
                                                    getWindowStartProxyForNestedLoopJoin,
                                                    operatorHandlerMemRef,
                                                    windowIdentifierMemRef);
    Value<Any> windowEnd = Nautilus::FunctionCall("getWindowEndProxyForNestedLoopJoin",
                                                  getWindowEndProxyForNestedLoopJoin,
                                                  operatorHandlerMemRef,
                                                  windowIdentifierMemRef);

    ctx.setWatermarkTs(windowEnd.as<UInt64>());
    auto sequenceNumber = Nautilus::FunctionCall("getSequenceNumberProxyForNestedLoopJoin",
                                                 getSequenceNumberProxyForNestedLoopJoin,
                                                 operatorHandlerMemRef);
    ctx.setSequenceNumber(sequenceNumber);
    auto originId =
        Nautilus::FunctionCall("getOriginIdProxyForNestedLoopJoin", getOriginIdProxyForNestedLoopJoin, operatorHandlerMemRef);
    ctx.setOrigin(originId);

    auto leftMemProvider = Execution::MemoryProvider::MemoryProvider::createMemoryProvider(/*bufferSize*/ 1, leftSchema);
    auto rightMemProvider = Execution::MemoryProvider::MemoryProvider::createMemoryProvider(/*bufferSize*/ 1, rightSchema);

    Nautilus::Value<UInt64> zeroVal((uint64_t) 0);
    for (auto leftIt = leftPagedVector.begin(); leftIt != leftPagedVector.end(); ++leftIt) {
        for (auto rightIt = rightPagedVector.begin(); rightIt != rightPagedVector.end(); ++rightIt) {
            auto leftRecordMemRef = *leftIt;
            auto rightRecordMemRef = *rightIt;
            auto leftRecord = leftMemProvider->read({}, leftRecordMemRef, zeroVal);
            auto rightRecord = rightMemProvider->read({}, rightRecordMemRef, zeroVal);

            /* This can be later replaced by an interface that returns bool and getys passed the
                 * two Nautilus::Records (left and right) #3691 */
            if (leftRecord.read(joinFieldNameLeft) == rightRecord.read(joinFieldNameRight)) {
                Record joinedRecord;

                // TODO replace this with a more useful version
                joinedRecord.write(joinSchema->get(0)->getName(), windowStart);
                joinedRecord.write(joinSchema->get(1)->getName(), windowEnd);
                joinedRecord.write(joinSchema->get(2)->getName(), leftRecord.read(joinFieldNameLeft));

                /* Writing the leftSchema fields, expect the join schema to have the fields in the same order then
                     * the left schema */
                for (auto& field : leftSchema->fields) {
                    joinedRecord.write(field->getName(), leftRecord.read(field->getName()));
                }

                /* Writing the rightSchema fields, expect the join schema to have the fields in the same order then
                     * the right schema */
                for (auto& field : rightSchema->fields) {
                    joinedRecord.write(field->getName(), rightRecord.read(field->getName()));
                }

                if (hasChild()) {
                    // Calling the child operator for this joinedRecord
                    child->execute(ctx, joinedRecord);
                }
            }
        }
    }

    // Once we are done with this window, we can delete it to free up space
    Nautilus::FunctionCall("deleteWindowProxyForNestedLoopJoin",
                           deleteWindowProxyForNestedLoopJoin,
                           operatorHandlerMemRef,
                           windowIdentifierMemRef);
}

NLJSink::NLJSink(uint64_t operatorHandlerIndex,
                 SchemaPtr leftSchema,
                 SchemaPtr rightSchema,
                 SchemaPtr joinSchema,
                 std::string joinFieldNameLeft,
                 std::string joinFieldNameRight)
    : operatorHandlerIndex(operatorHandlerIndex), leftSchema(std::move(leftSchema)), rightSchema(std::move(rightSchema)),
      joinSchema(std::move(joinSchema)), joinFieldNameLeft(joinFieldNameLeft), joinFieldNameRight(joinFieldNameRight){}

}// namespace NES::Runtime::Execution::Operators