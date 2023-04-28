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
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>


namespace NES::Runtime::Execution::Operators {

    struct NLJJoinParams {
        PipelineExecutionContext* pipelineCtx;
        WorkerContext* workerCtx;
        uint8_t* outerTuplesPtr;
        uint8_t* innerTuplesPtr;
        uint64_t numberOfTuplesOuter;
        uint64_t numberOfTuplesInner;
        uint64_t outerJoinKeyPosition;
        uint64_t innerJoinKeyPosition;
        uint64_t joinKeySize;
        SchemaPtr outerSchema;
        SchemaPtr innerSchema;
        SchemaPtr joinSchema;
        uint64_t windowStart;
        uint64_t windowEnd;
    };

    uint64_t performNLJJoin(const NLJJoinParams& nljJoinParams) {
        auto numberOfTuplesJoined = 0UL;
        auto outerKey = nljJoinParams.outerTuplesPtr + nljJoinParams.outerJoinKeyPosition;
        auto innerKey = nljJoinParams.innerTuplesPtr + nljJoinParams.innerJoinKeyPosition;
        const auto outerTupleSize = nljJoinParams.outerSchema->getSchemaSizeInBytes();
        const auto innerTupleSize = nljJoinParams.innerSchema->getSchemaSizeInBytes();
        const auto joinTupleSize = nljJoinParams.joinSchema->getSchemaSizeInBytes();
        const uint64_t sizeOfWindowStart = sizeof(nljJoinParams.windowStart);
        const uint64_t sizeOfWindowEnd = sizeof(nljJoinParams.windowEnd);

        auto tupleBuffer = nljJoinParams.workerCtx->allocateTupleBuffer();
        tupleBuffer.setNumberOfTuples(0);
        const auto numberOfTuplesPerBuffer = tupleBuffer.getBufferSize() / nljJoinParams.joinSchema->getSchemaSizeInBytes();

        for(auto outer = 0UL; outer < nljJoinParams.numberOfTuplesOuter; ++outer) {
            for (auto inner = 0UL; inner < nljJoinParams.numberOfTuplesInner; ++inner) {
                if (std::memcmp(innerKey, outerKey, nljJoinParams.joinKeySize) == 0) {
                    ++numberOfTuplesJoined;

                    auto outerTuple = nljJoinParams.outerTuplesPtr + outerTupleSize * outer;
                    auto innerTuple = nljJoinParams.innerTuplesPtr + innerTupleSize * inner;

                    auto numberOfTuplesInBuffer = tupleBuffer.getNumberOfTuples();
                    auto bufferPtr = tupleBuffer.getBuffer() + joinTupleSize * numberOfTuplesInBuffer;

                    std::memcpy(bufferPtr, &nljJoinParams.windowStart, sizeOfWindowStart);
                    std::memcpy(bufferPtr + sizeOfWindowStart, &nljJoinParams.windowEnd, sizeOfWindowEnd);
                    std::memcpy(bufferPtr + sizeOfWindowStart + sizeOfWindowEnd, innerKey, nljJoinParams.joinKeySize);

                        std::memcpy(bufferPtr + sizeOfWindowStart + sizeOfWindowEnd + nljJoinParams.joinKeySize,
                                    outerTuple, outerTupleSize);
                        std::memcpy(bufferPtr + sizeOfWindowStart + sizeOfWindowEnd + nljJoinParams.joinKeySize + outerTupleSize,
                                    innerTuple, innerTupleSize);
                    tupleBuffer.setNumberOfTuples(numberOfTuplesInBuffer);

                    if (numberOfTuplesInBuffer >= numberOfTuplesPerBuffer) {
                        nljJoinParams.pipelineCtx->emitBuffer(tupleBuffer, *nljJoinParams.workerCtx);
                        tupleBuffer = nljJoinParams.workerCtx->allocateTupleBuffer();
                        tupleBuffer.setNumberOfTuples(0);
                    }
                }

                outerKey += outerTupleSize;
                innerKey += innerTupleSize;
            }
        }

        if (tupleBuffer.getNumberOfTuples() > 0) {
            nljJoinParams.pipelineCtx->emitBuffer(tupleBuffer, *nljJoinParams.workerCtx);
        }

        return numberOfTuplesJoined;
    }

    /**
     * @brief For now, we join the left as the outer and the right as the inner join
     * @param ptrOpHandler
     * @param ptrPipelineCtx
     * @param ptrWorkerCtx
     * @param windowIdentifierPtr
     */
    void performNLJProxy(void* ptrOpHandler, void* ptrPipelineCtx, void* ptrWorkerCtx, void* windowIdentifierPtr) {
        NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
        NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
        NES_ASSERT2_FMT(ptrWorkerCtx != nullptr, "worker context should not be null");
        NES_ASSERT2_FMT(windowIdentifierPtr != nullptr, "joinPartitionTimeStampPtr should not be null");

        auto opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
        auto pipelineCtx = static_cast<PipelineExecutionContext*>(ptrPipelineCtx);
        auto workerCtx = static_cast<WorkerContext*>(ptrWorkerCtx);
        auto windowIdentifier = reinterpret_cast<uint64_t>(windowIdentifierPtr);

        auto numberOfTuplesLeft = opHandler->getNumberOfTuplesInWindow(windowIdentifier, /*isLeftSide*/ true);
        auto numberOfTuplesRight = opHandler->getNumberOfTuplesInWindow(windowIdentifier, /*isLeftSide*/ false);
        auto tuplesLeftPtr = opHandler->getFirstTuple(windowIdentifier, /*isLeftSide*/ true);
        auto tuplesRightPtr = opHandler->getFirstTuple(windowIdentifier, /*isLeftSide*/ false);
        auto leftKeyPosition = opHandler->getPositionOfJoinKey(/*isLeftSide*/ true);
        auto rightKeyPosition = opHandler->getPositionOfJoinKey(/*isLeftSide*/ true);
        auto leftSchema = opHandler->getSchema(/*isLeftSide*/ true);
        auto rightSchema = opHandler->getSchema(/*isLeftSide*/ false);
        auto [windowStart, windowEnd] = opHandler->getWindowStartEnd(windowIdentifier);

        DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
        auto joinKeySize = defaultPhysicalTypeFactory.getPhysicalType(leftSchema->get(opHandler->getJoinFieldNameLeft())->getDataType())->size();

        NLJJoinParams nljJoinParams;
        nljJoinParams.workerCtx = workerCtx;
        nljJoinParams.pipelineCtx = pipelineCtx;
        nljJoinParams.outerTuplesPtr = tuplesLeftPtr;
        nljJoinParams.innerTuplesPtr = tuplesRightPtr;
        nljJoinParams.numberOfTuplesOuter = numberOfTuplesLeft;
        nljJoinParams.numberOfTuplesInner = numberOfTuplesRight;
        nljJoinParams.outerJoinKeyPosition = leftKeyPosition;
        nljJoinParams.innerJoinKeyPosition = rightKeyPosition;
        nljJoinParams.joinKeySize = joinKeySize;
        nljJoinParams.outerSchema = leftSchema;
        nljJoinParams.innerSchema = rightSchema;
        nljJoinParams.joinSchema = Util::createJoinSchema(leftSchema, rightSchema, opHandler->getJoinFieldNameLeft());
        nljJoinParams.windowStart = windowStart;
        nljJoinParams.windowEnd = windowEnd;

        auto numberOfTuplesJoined = performNLJJoin(nljJoinParams);
        NES_DEBUG2("Joined a total of {} tuples!", numberOfTuplesJoined);
    }



    void NLJSink::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {

        auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
        auto windowIdentifierMemRef = recordBuffer.getBuffer();

        Nautilus::FunctionCall("performNLJProxy", performNLJProxy, operatorHandlerMemRef,
                               ctx.getPipelineContext(), ctx.getWorkerContext(), windowIdentifierMemRef);
    }

} // namespace NES::Runtime::Execution::Operators