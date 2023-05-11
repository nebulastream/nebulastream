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
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJSink.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>


namespace NES::Runtime::Execution::Operators {

    void* getFirstTupleProxy(void* ptrOpHandler, void* windowIdentifierPtr, bool isLeftSide) {
        NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
        NES_ASSERT2_FMT(windowIdentifierPtr != nullptr, "joinPartitionTimeStampPtr should not be null");

        auto opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
        auto windowIdentifier = *static_cast<uint64_t*>(windowIdentifierPtr);
        return opHandler->getFirstTuple(windowIdentifier, isLeftSide);
    }

    uint64_t getNumberOfTuplesProxy(void* ptrOpHandler, void* windowIdentifierPtr, bool isLeftSide) {
        NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
        NES_ASSERT2_FMT(windowIdentifierPtr != nullptr, "joinPartitionTimeStampPtr should not be null");

        auto opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
        auto windowIdentifier = *static_cast<uint64_t*>(windowIdentifierPtr);
        return opHandler->getNumberOfTuplesInWindow(windowIdentifier, isLeftSide);
    }

    void deleteWindowProxy(void* ptrOpHandler, void* windowIdentifierPtr) {
        NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
        NES_ASSERT2_FMT(windowIdentifierPtr != nullptr, "joinPartitionTimeStampPtr should not be null");

        auto opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
        auto windowIdentifier = *static_cast<uint64_t*>(windowIdentifierPtr);
        opHandler->deleteWindow(windowIdentifier);
    }

    uint64_t getWindowStartProxy(void* ptrOpHandler, void* windowIdentifierPtr) {
        NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
        NES_ASSERT2_FMT(windowIdentifierPtr != nullptr, "joinPartitionTimeStampPtr should not be null");

        auto opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
        auto windowIdentifier = *static_cast<uint64_t*>(windowIdentifierPtr);

        return opHandler->getWindowStartEnd(windowIdentifier).first;
    }

    uint64_t getWindowEndProxy(void* ptrOpHandler, void* windowIdentifierPtr) {
        NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
        NES_ASSERT2_FMT(windowIdentifierPtr != nullptr, "joinPartitionTimeStampPtr should not be null");

        auto opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
        auto windowIdentifier = *static_cast<uint64_t*>(windowIdentifierPtr);

        return opHandler->getWindowStartEnd(windowIdentifier).second + 1;
    }


    void NLJSink::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
        if (hasChild()) {
            child->open(ctx, recordBuffer);
        }

        auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
        auto windowIdentifierMemRef = recordBuffer.getBuffer();

        auto firstTupleLeft = Nautilus::FunctionCall("getFirstTupleProxy", getFirstTupleProxy,
                                                     operatorHandlerMemRef,
                                                     windowIdentifierMemRef,
                                                     Value<Boolean>(/*isLeftSide*/ true));
        auto firstTupleRight = Nautilus::FunctionCall("getFirstTupleProxy", getFirstTupleProxy,
                                                      operatorHandlerMemRef,
                                                      windowIdentifierMemRef,
                                                      Value<Boolean>(/*isLeftSide*/ false));

        auto numberOfTuplesLeft = Nautilus::FunctionCall("getNumberOfTuplesProxy", getNumberOfTuplesProxy,
                                                         operatorHandlerMemRef,
                                                         windowIdentifierMemRef,
                                                         Value<Boolean>(/*isLeftSide*/ true));
        auto numberOfTuplesRight = Nautilus::FunctionCall("getNumberOfTuplesProxy", getNumberOfTuplesProxy,
                                                          operatorHandlerMemRef,
                                                          windowIdentifierMemRef,
                                                          Value<Boolean>(/*isLeftSide*/ false));

        // TODO ask Philipp why do I need this here and can not use auto?
        Value<Any> windowStart = Nautilus::FunctionCall("getWindowStartProxy", getWindowStartProxy,
                                                  operatorHandlerMemRef, windowIdentifierMemRef);
        Value<Any> windowEnd = Nautilus::FunctionCall("getWindowEndProxy", getWindowEndProxy,
                                                  operatorHandlerMemRef, windowIdentifierMemRef);

        // As we know that the data is of type rowLayout, we do not have to provide a buffer size
        auto leftMemProvider = Execution::MemoryProvider::MemoryProvider::createMemoryProvider(/*bufferSize*/1, leftSchema);
        auto rightMemProvider = Execution::MemoryProvider::MemoryProvider::createMemoryProvider(/*bufferSize*/1, rightSchema);

        for (Value<UInt64> leftPos(0UL); leftPos < numberOfTuplesLeft; leftPos = leftPos + 1){
            for (Value<UInt64> rightPos(0UL); rightPos < numberOfTuplesRight; rightPos = rightPos + 1){
                auto leftRecord = leftMemProvider->read({}, firstTupleLeft, leftPos);
                auto rightRecord = rightMemProvider->read({}, firstTupleRight, rightPos);

                /* This can be later replaced by an interface that returns bool and gets passed the
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

                    // Calling the child operator for this joinedRecord
                    child->execute(ctx, joinedRecord);
                }
            }
        }

        // Once we are done with this window, we can delete it to free up space
        Nautilus::FunctionCall("deleteWindowProxy", deleteWindowProxy, operatorHandlerMemRef, windowIdentifierMemRef);
    }

    NLJSink::NLJSink(uint64_t operatorHandlerIndex, SchemaPtr leftSchema, SchemaPtr rightSchema, SchemaPtr joinSchema,
                     std::string joinFieldNameLeft, std::string joinFieldNameRight)
            : operatorHandlerIndex(operatorHandlerIndex), leftSchema(std::move(leftSchema)),
            rightSchema(std::move(rightSchema)), joinSchema(std::move(joinSchema)), joinFieldNameLeft(joinFieldNameLeft),
            joinFieldNameRight(joinFieldNameRight) {}

} // namespace NES::Runtime::Execution::Operators