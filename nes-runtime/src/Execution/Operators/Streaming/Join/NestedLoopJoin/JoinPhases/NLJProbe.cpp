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
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/DataStructure/NLJWindow.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJProbe.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Runtime::Execution::Operators {

void* getNLJWindowRef(void* ptrOpHandler, uint64_t windowIdentifier) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    NES_INFO("windowIdentifier: {}", windowIdentifier);
    const auto opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    auto window = opHandler->getWindowByWindowIdentifier(windowIdentifier);
    if (window.has_value()) {
        return window.value().get();
    }
    // For now this is fine. We should handle this as part of issue #4016
    return nullptr;
}

void deleteWindowProxyForNestedLoopJoin(void* ptrOpHandler, uint64_t windowIdentifier) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "op handler context should not be null");
    auto opHandler = static_cast<NLJOperatorHandler*>(ptrOpHandler);
    opHandler->deleteWindow(windowIdentifier);
}

void NLJProbe::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // As this operator functions as a scan, we have to set the execution context for this pipeline
    ctx.setWatermarkTs(recordBuffer.getWatermarkTs());
    ctx.setSequenceNumber(recordBuffer.getSequenceNr());
    ctx.setOrigin(recordBuffer.getOriginId());

    Operator::open(ctx, recordBuffer);

    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto windowIdentifier = recordBuffer.getBuffer().load<UInt64>();
    // During triggering the window, we append all pages of all local copies to a single PagedVector located at position 0
    Value<UInt64> workerIdForPagedVectors(0_u64);
    auto windowReference = Nautilus::FunctionCall("getNLJWindowRef", getNLJWindowRef, operatorHandlerMemRef, windowIdentifier);
    auto leftPagedVectorRef = Nautilus::FunctionCall("getNLJPagedVectorProxy",
                                                     getNLJPagedVectorProxy,
                                                     windowReference,
                                                     workerIdForPagedVectors,
                                                     Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Left)));
    auto rightPagedVectorRef = Nautilus::FunctionCall("getNLJPagedVectorProxy",
                                                      getNLJPagedVectorProxy,
                                                      windowReference,
                                                      workerIdForPagedVectors,
                                                      Value<UInt64>(to_underlying(QueryCompilation::JoinBuildSideType::Right)));

    Nautilus::Interface::PagedVectorRef leftPagedVector(leftPagedVectorRef, leftEntrySize);
    Nautilus::Interface::PagedVectorRef rightPagedVector(rightPagedVectorRef, rightEntrySize);

    Value<UInt64> windowStart = Nautilus::FunctionCall("getNLJWindowStartProxy", getNLJWindowStartProxy, windowReference);
    Value<UInt64> windowEnd = Nautilus::FunctionCall("getNLJWindowEndProxy", getNLJWindowEndProxy, windowReference);

    // As we know that the tuples are lying one after the other (row layout), we can ignore the buffer size
    auto leftMemProvider = Execution::MemoryProvider::MemoryProvider::createMemoryProvider(/*bufferSize*/ 1, leftSchema);
    auto rightMemProvider = Execution::MemoryProvider::MemoryProvider::createMemoryProvider(/*bufferSize*/ 1, rightSchema);

    Nautilus::Value<UInt64> zeroVal(0_u64);
    for (auto leftRecordMemRef : leftPagedVector) {
        for (auto rightRecordMemRef : rightPagedVector) {
            auto leftRecord = leftMemProvider->read({}, leftRecordMemRef, zeroVal);
            auto rightRecord = rightMemProvider->read({}, rightRecordMemRef, zeroVal);
            /* This can be later replaced by an interface that returns boolean and gets passed the
             * two Nautilus::Records (left and right) #3691 */
            if (leftRecord.read(joinFieldNameLeft) == rightRecord.read(joinFieldNameRight)) {
                Record joinedRecord;
                // Writing the window start, end, and key field
                joinedRecord.write(windowStartFieldName, windowStart);
                joinedRecord.write(windowEndFieldName, windowEnd);
                joinedRecord.write(windowKeyFieldName, leftRecord.read(joinFieldNameLeft));

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
    Nautilus::FunctionCall("deleteWindowProxyForNestedLoopJoin",
                           deleteWindowProxyForNestedLoopJoin,
                           operatorHandlerMemRef,
                           windowIdentifier);
}

NLJProbe::NLJProbe(const uint64_t operatorHandlerIndex,
                   const SchemaPtr& leftSchema,
                   const SchemaPtr& rightSchema,
                   const SchemaPtr& joinSchema,
                   const uint64_t leftEntrySize,
                   const uint64_t rightEntrySize,
                   const std::string& joinFieldNameLeft,
                   const std::string& joinFieldNameRight,
                   const std::string& windowStartFieldName,
                   const std::string& windowEndFieldName,
                   const std::string& windowKeyFieldName)
    : operatorHandlerIndex(operatorHandlerIndex), leftSchema(leftSchema), rightSchema(rightSchema), joinSchema(joinSchema),
      leftEntrySize(leftEntrySize), rightEntrySize(rightEntrySize), joinFieldNameLeft(joinFieldNameLeft),
      joinFieldNameRight(joinFieldNameRight), windowStartFieldName(windowStartFieldName), windowEndFieldName(windowEndFieldName),
      windowKeyFieldName(windowKeyFieldName) {}

}// namespace NES::Runtime::Execution::Operators