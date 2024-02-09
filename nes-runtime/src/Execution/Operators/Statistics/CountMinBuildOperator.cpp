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

#include <API/Schema.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Statistics/CountMinBuildOperator.hpp>
#include <Execution/Operators/Statistics/CountMinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/H3Hash.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <StatisticFieldIdentifiers.hpp>
#include <Util/StatisticCollectorIdentifier.hpp>

namespace NES::Experimental::Statistics {

CountMinBuildOperator::CountMinBuildOperator(uint64_t operatorHandlerIndex,
                                             uint64_t width,
                                             uint64_t depth,
                                             const std::string& onField,
                                             uint64_t keySizeInBits,
                                             Runtime::Execution::Operators::TimeFunctionPtr timeFunction,
                                             SchemaPtr schema)
    : operatorHandlerIndex(operatorHandlerIndex), width(width), depth(depth), onField(onField), keySizeInBits(keySizeInBits),
      timeFunction(std::move(timeFunction)),
      memoryProvider(Runtime::Execution::MemoryProvider::MemoryProvider::createMemoryProvider(1, std::move(schema))) {}

void setUpOperatorHandlerProxy(void* opHandlerPtr, void* ptrPipelineCtx) {
    auto* opHandler = static_cast<CountMinOperatorHandler*>(opHandlerPtr);
    auto* pipelineCtx = static_cast<Runtime::Execution::PipelineExecutionContext*>(ptrPipelineCtx);
    auto bufferManager = pipelineCtx->getBufferManager();
    opHandler->setBufferManager(bufferManager);
}

void* getTupleBufferProxy(void* opHandlerPtr, const Nautilus::TextValue* nPhysicalSourceName, uint64_t ts) {
    auto* opHandler = static_cast<CountMinOperatorHandler*>(opHandlerPtr);
    auto physicalSourceName = std::string(nPhysicalSourceName->c_str(), nPhysicalSourceName->length());
    auto buffer = opHandler->getTupleBuffer(physicalSourceName, ts);
    return buffer.getBuffer();
}

void incrementCMProxy(Nautilus::TextValue* cmString, uint64_t rowId, uint64_t width, uint64_t columnId) {
    auto* rawData = static_cast<uint64_t*>(static_cast<void*>(cmString->str()));
    CountMin::incrementCounter(rawData, rowId, width, columnId);
}

void checkFinishedCountMinSketchesProxy(void* ptrOpHandler,
                                        void* ptrPipelineCtx,
                                        void* ptrWorkerCtx,
                                        uint64_t watermarkTs,
                                        uint64_t sequenceNumber,
                                        OriginId originId) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    NES_ASSERT2_FMT(ptrPipelineCtx != nullptr, "pipeline context should not be null");
    NES_ASSERT2_FMT(ptrWorkerCtx != nullptr, "worker context should not be null");

    auto* pipelineCtx = static_cast<Runtime::Execution::PipelineExecutionContext*>(ptrPipelineCtx);
    auto* opHandler = static_cast<CountMinOperatorHandler*>(ptrOpHandler);

    auto tupleBuffers = opHandler->getFinishedCountMinSketches(watermarkTs, sequenceNumber, originId);
    for (const auto& tupleBuffer : tupleBuffers) {
        pipelineCtx->dispatchBuffer(tupleBuffer);
    }
}

void* getH3SeedsProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<CountMinOperatorHandler*>(ptrOpHandler);
    return opHandler->getH3Seeds();
}

void CountMinBuildOperator::setup(Runtime::Execution::ExecutionContext& ctx) const {
    Nautilus::FunctionCall("setUpOperatorHandlerProxy",
                           setUpOperatorHandlerProxy,
                           ctx.getGlobalOperatorHandler(operatorHandlerIndex),
                           ctx.getPipelineContext());
}

void CountMinBuildOperator::execute(NES::Runtime::Execution::ExecutionContext& ctx, NES::Nautilus::Record& incomingRecord) const {
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    Nautilus::Value<Nautilus::UInt64> timestampVal = timeFunction->getTs(ctx, incomingRecord);
    Nautilus::Value<Nautilus::UInt64> nWidth(width);
    Nautilus::Value<Nautilus::UInt64> nDepth(depth);
    auto physicalSourceName = incomingRecord.read(PHYSICAL_SOURCE_NAME).as<Nautilus::Text>();

    // get TupleBuffer and convert it to a MemRef
    auto tupleBuffer = FunctionCall("getTupleBufferProxy",
                                    getTupleBufferProxy,
                                    globalOperatorHandler,
                                    physicalSourceName->getReference(),
                                    timestampVal);

    // read incomingRecord such that it can be changed
    Nautilus::Value<Nautilus::UInt64> zeroVal = 0_u64;
    Nautilus::Record cmRecord = memoryProvider->read({}, tupleBuffer, zeroVal);

    // increase the number of observed tuples
    auto obsTups = cmRecord.read(OBSERVED_TUPLES);
    cmRecord.write(OBSERVED_TUPLES, obsTups + 1);

    // get the memref of the seeds twice
    auto h3SeedsBaseMemRef = Nautilus::FunctionCall("getH3SeedsProxy", getH3SeedsProxy, globalOperatorHandler);
    auto h3SeedsMemRef = Nautilus::FunctionCall("getH3SeedsProxy", getH3SeedsProxy, globalOperatorHandler);

    // read the key
    auto key = incomingRecord.read(onField);

    // here we have to iterate over all rows and increment the corresponding columns
    for (Nautilus::Value<Nautilus::UInt64> rowId = 0_u64; rowId < nDepth; rowId = rowId + 1) {
        // calculate hash/columnId for each row
        h3SeedsMemRef = h3SeedsBaseMemRef + (sizeof(uint64_t) * width * rowId).as<Nautilus::MemRef>();
        Nautilus::Value<Nautilus::UInt64> columnId = 0_u64;
        Nautilus::Interface::H3Hash(keySizeInBits).calculateWithState(columnId, key, h3SeedsMemRef);
        columnId = columnId % width;

        auto cmString = cmRecord.read(DATA).as<Nautilus::Text>();
        FunctionCall("incrementCMProxy", incrementCMProxy, cmString->getReference(), rowId, nWidth, columnId);
    }

    cmRecord.write(WIDTH, width);

    // As a last step, we have to write the updated incomingRecord back to the tuple buffer
    memoryProvider->write(zeroVal, tupleBuffer, cmRecord);
}

void CountMinBuildOperator::close(Runtime::Execution::ExecutionContext& executionCtx,
                                  Runtime::Execution::RecordBuffer&) const {

    // Update the watermark for the countMinBuildOperator and trigger slices
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("checkFinishedCountMinSketchesProxy",
                           checkFinishedCountMinSketchesProxy,
                           operatorHandlerMemRef,
                           executionCtx.getPipelineContext(),
                           executionCtx.getWorkerContext(),
                           executionCtx.getWatermarkTs(),
                           executionCtx.getSequenceNumber(),
                           executionCtx.getOriginId());
}

}// namespace NES::Experimental::Statistics