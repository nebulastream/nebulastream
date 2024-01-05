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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJBuildSlicing.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Runtime::Execution::Operators {

uint64_t getNLJSliceStartProxy(void* ptrNljSlice) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto* nljSlice = static_cast<NLJSlice*>(ptrNljSlice);
    return nljSlice->getSliceStart();
}

uint64_t getNLJSliceEndProxy(void* ptrNljSlice) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto* nljSlice = static_cast<NLJSlice*>(ptrNljSlice);
    return nljSlice->getSliceEnd();
}

void* getCurrentWindowProxy(void* ptrOpHandler) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<NLJOperatorHandlerSlicing*>(ptrOpHandler);
    return opHandler->getCurrentSliceOrCreate();
}

void* getNLJSliceRefProxy(void* ptrOpHandler, uint64_t timestamp) {
    NES_ASSERT2_FMT(ptrOpHandler != nullptr, "opHandler context should not be null!");
    auto* opHandler = static_cast<NLJOperatorHandlerSlicing*>(ptrOpHandler);
    return opHandler->getSliceByTimestampOrCreateIt(timestamp).get();
}

void NLJBuildSlicing::execute(ExecutionContext& ctx, Record& record) const {
    // Get the local state
    auto localJoinState = dynamic_cast<LocalNestedLoopJoinState*>(ctx.getLocalState(this));
    auto operatorHandlerMemRef = localJoinState->joinOperatorHandler;
    Value<UInt64> timestampVal = timeFunction->getTs(ctx, record);

    if (!(localJoinState->sliceStart <= timestampVal && timestampVal < localJoinState->sliceEnd)) {
        // We have to get the slice for the current timestamp
        auto workerId = ctx.getWorkerId();
        updateLocalJoinState(localJoinState, operatorHandlerMemRef, timestampVal, workerId);
    }

    // Get the memRef to the new entry
    auto entryMemRef = localJoinState->pagedVectorRef.allocateEntry();

    // Write Record at entryMemRef
    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : schema->fields) {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

        entryMemRef.store(record.read(fieldName));
        entryMemRef = entryMemRef + fieldType->size();
    }
}

void NLJBuildSlicing::updateLocalJoinState(LocalNestedLoopJoinState* localJoinState,
                                           Value<Nautilus::MemRef>& operatorHandlerMemRef,
                                           Value<Nautilus::UInt64>& timestamp,
                                           Value<Nautilus::UInt64>& workerId) const {
    NES_DEBUG("Updating LocalJoinState!");

    // Retrieving the slice of the current watermark, as we expect that more tuples will be inserted into this slice
    localJoinState->sliceReference =
        Nautilus::FunctionCall("getNLJSliceRefProxy", getNLJSliceRefProxy, operatorHandlerMemRef, timestamp);
    auto nljPagedVectorMemRef = Nautilus::FunctionCall("getNLJPagedVectorProxy",
                                                       getNLJPagedVectorProxy,
                                                       localJoinState->sliceReference,
                                                       workerId,
                                                       Value<UInt64>(to_underlying(joinBuildSide)));
    localJoinState->pagedVectorRef = Nautilus::Interface::PagedVectorRef(nljPagedVectorMemRef, entrySize);
    localJoinState->sliceStart =
        Nautilus::FunctionCall("getNLJSliceStartProxy", getNLJSliceStartProxy, localJoinState->sliceReference);
    localJoinState->sliceEnd = Nautilus::FunctionCall("getNLJSliceEndProxy", getNLJSliceEndProxy, localJoinState->sliceReference);
}

void NLJBuildSlicing::open(ExecutionContext& ctx, RecordBuffer&) const {
    auto opHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    auto workerId = ctx.getWorkerId();
    auto sliceReference = Nautilus::FunctionCall("getCurrentWindowProxy", getCurrentWindowProxy, opHandlerMemRef);
    auto nljPagedVectorMemRef = Nautilus::FunctionCall("getNLJPagedVectorProxy",
                                                       getNLJPagedVectorProxy,
                                                       sliceReference,
                                                       workerId,
                                                       Value<UInt64>(to_underlying(joinBuildSide)));
    auto pagedVectorRef = Nautilus::Interface::PagedVectorRef(nljPagedVectorMemRef, entrySize);
    auto localJoinState = std::make_unique<LocalNestedLoopJoinState>(opHandlerMemRef, sliceReference, pagedVectorRef);

    // Getting the current slice start and end
    localJoinState->sliceStart =
        Nautilus::FunctionCall("getNLJSliceStartProxy", getNLJSliceStartProxy, localJoinState->sliceReference);
    localJoinState->sliceEnd = Nautilus::FunctionCall("getNLJSliceEndProxy", getNLJSliceEndProxy, localJoinState->sliceReference);

    // Storing the local state
    ctx.setLocalOperatorState(this, std::move(localJoinState));
}

NLJBuildSlicing::NLJBuildSlicing(const uint64_t operatorHandlerIndex,
                                 const SchemaPtr& schema,
                                 const std::string& joinFieldName,
                                 const QueryCompilation::JoinBuildSideType joinBuildSide,
                                 const uint64_t entrySize,
                                 TimeFunctionPtr timeFunction,
                                 QueryCompilation::StreamJoinStrategy joinStrategy,
                                 QueryCompilation::WindowingStrategy windowingStrategy)
    : StreamJoinBuild(operatorHandlerIndex,
                      schema,
                      joinFieldName,
                      joinBuildSide,
                      entrySize,
                      std::move(timeFunction),
                      joinStrategy,
                      windowingStrategy) {}
}// namespace NES::Runtime::Execution::Operators