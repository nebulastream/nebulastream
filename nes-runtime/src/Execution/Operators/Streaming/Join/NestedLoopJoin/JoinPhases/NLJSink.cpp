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
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
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

    std::string getJoinFieldNameProxy(void* ptrOpHandler, bool isLeftSide) {
        NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");

        auto opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
        return opHandler->getJoinFieldName(isLeftSide);
    }

    void NLJSink::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {

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

        auto joinFieldNameLeft = Nautilus::FunctionCall("getJoinFieldNameProxy", getJoinFieldNameProxy,
                                                        operatorHandlerMemRef,
                                                        Value<Boolean>(/*isLeftSide*/ true));
        auto joinFieldNameRight = Nautilus::FunctionCall("getJoinFieldNameProxy", getJoinFieldNameProxy,
                                                        operatorHandlerMemRef,
                                                        Value<Boolean>(/*isLeftSide*/ false));

//        then iterate over both tuplesmemref via rowMemoryProvider and create a new record
//            - left and right record via rowMemoryprovider read
//            - two additional fields windowstart and windowend
//            - get the windowStart and windowEnd from the NLJOpHandler
//

        // As we know that the data is of type rowlayout, we do not have to provide a buffer size
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
                    hier das Record zusammen bauen.
                    Philipp fragen:
                        - wie das mit dem underlying data layout aussieht, also ob es schlimm ist, dass die Daten nicht 1:1 neben einander liegen
                        - Kann ich einen string mit Nautilus::Functioncall zurück geben?
                        - Wie bekomme ich das hin, dass korrekte Joinlayout mit windowstart und windowend hinzubekommen?
                }
            }
        }

        // Once we are done with this window, we can delete it to free up space
        Nautilus::FunctionCall("deleteWindowProxy", deleteWindowProxy, operatorHandlerMemRef, windowIdentifierMemRef);

    }

    NLJSink::NLJSink(uint64_t operatorHandlerIndex, SchemaPtr leftSchema, SchemaPtr rightSchema, SchemaPtr joinSchema)
            : operatorHandlerIndex(operatorHandlerIndex), leftSchema(std::move(leftSchema)),
            rightSchema(std::move(rightSchema)), joinSchema(std::move(joinSchema)) {}

} // namespace NES::Runtime::Execution::Operators