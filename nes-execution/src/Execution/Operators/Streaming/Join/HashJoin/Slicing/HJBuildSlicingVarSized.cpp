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

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJSliceVarSized.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJBuildSlicingVarSized.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Stores the reference to the pagedVector, slice start, slice end, and the slice reference
 */
class LocalHashJoinState : public OperatorState {
  public:
    LocalHashJoinState(Value<MemRef>& operatorHandler, Value<MemRef>& pagedVectorVarSizedRef, Value<MemRef>& sliceReference)
        : joinOperatorHandler(operatorHandler), pagedVectorVarSizedRef(pagedVectorVarSizedRef), sliceReference(sliceReference),
          sliceStart(0_u64), sliceEnd(0_u64){};
    Value<MemRef> joinOperatorHandler;
    Value<MemRef> pagedVectorVarSizedRef;
    Value<MemRef> sliceReference;
    Value<UInt64> sliceStart;
    Value<UInt64> sliceEnd;
};

void* getHJSliceVarSizedProxy(void* ptrOpHandler, uint64_t timeStamp) {
    NES_DEBUG("getHJSliceVarSizedProxy with ts={}", timeStamp);
    auto* opHandler = StreamJoinOperator::getSpecificOperatorHandler(
        ptrOpHandler,
        magic_enum::enum_integer(QueryCompilation::StreamJoinStrategy::HASH_JOIN_VAR_SIZED),
        magic_enum::enum_integer(QueryCompilation::WindowingStrategy::SLICING));
    auto currentSlice = dynamic_cast<HJOperatorHandlerSlicing*>(opHandler)->getSliceByTimestampOrCreateIt(timeStamp);
    NES_ASSERT2_FMT(currentSlice != nullptr, "invalid window");
    return currentSlice.get();
}

uint64_t getSliceStartVarSizedProxy(void* ptrHashSlice) {
    NES_ASSERT2_FMT(ptrHashSlice != nullptr, "hash window handler context should not be null");
    auto* hashSlice = static_cast<HJSliceVarSized*>(ptrHashSlice);
    return hashSlice->getSliceStart();
}

uint64_t getSliceEndVarSizedProxy(void* ptrHashSlice) {
    NES_ASSERT2_FMT(ptrHashSlice != nullptr, "hash window handler context should not be null");
    auto* hashSlice = static_cast<HJSliceVarSized*>(ptrHashSlice);
    return hashSlice->getSliceEnd();
}

void* getHJPagedVectorVarSizedProxy(void* ptrHashSlice, WorkerThreadId workerThreadId, uint64_t joinBuildSideInt, uint64_t key) {
    NES_ASSERT2_FMT(ptrHashSlice != nullptr, "hash window handler context should not be null");
    auto* hashSlice = static_cast<HJSliceVarSized*>(ptrHashSlice);
    auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    NES_DEBUG("Insert into HT for window={} is left={} workerThreadId={}",
              hashSlice->getSliceIdentifier(),
              magic_enum::enum_name(joinBuildSide),
              workerThreadId);
    auto ptr = hashSlice->getHashTable(joinBuildSide, workerThreadId)->insert(key);
    auto pagedVectorVarSizedMemRef = ptr.get();
    return pagedVectorVarSizedMemRef;
}

HJBuildSlicingVarSized::HJBuildSlicingVarSized(const uint64_t operatorHandlerIndex,
                                               const SchemaPtr& schema,
                                               const std::string& joinFieldName,
                                               const QueryCompilation::JoinBuildSideType joinBuildSide,
                                               const uint64_t entrySize,
                                               TimeFunctionPtr timeFunction,
                                               QueryCompilation::StreamJoinStrategy joinStrategy)
    : StreamJoinOperator(joinStrategy, QueryCompilation::WindowingStrategy::SLICING), StreamJoinBuild(operatorHandlerIndex,
                                                                                                      schema,
                                                                                                      joinBuildSide,
                                                                                                      entrySize,
                                                                                                      std::move(timeFunction),
                                                                                                      joinStrategy,
                                                                                                      windowingStrategy),
      joinFieldName(joinFieldName) {}

void HJBuildSlicingVarSized::execute(ExecutionContext& ctx, Record& record) const {
    auto joinState = static_cast<LocalHashJoinState*>(ctx.getLocalState(this));
    auto operatorHandlerMemRef = joinState->joinOperatorHandler;
    Value<UInt64> tsValue = timeFunction->getTs(ctx, record);

    //check if we can reuse window
    if (!(joinState->sliceStart <= tsValue && tsValue < joinState->sliceEnd)) {
        joinState->sliceReference =
            FunctionCall("getHJSliceVarSizedProxy", getHJSliceVarSizedProxy, operatorHandlerMemRef, Value<UInt64>(tsValue));
        joinState->sliceStart = FunctionCall("getSliceStartVarSizedProxy", getSliceStartVarSizedProxy, joinState->sliceReference);
        joinState->sliceEnd = FunctionCall("getSliceEndVarSizedProxy", getSliceEndVarSizedProxy, joinState->sliceReference);

        NES_DEBUG("reinit join state with start={} end={} for ts={} for isLeftSide={}",
                  joinState->sliceStart->toString(),
                  joinState->sliceEnd->toString(),
                  tsValue->toString(),
                  to_underlying(joinBuildSide));
    }

    // Write record to the pagedVector
    auto hjPagedVectorMemRef = FunctionCall("getHJPagedVectorVarSizedProxy",
                                            getHJPagedVectorVarSizedProxy,
                                            joinState->sliceReference,
                                            ctx.getWorkerThreadId(),
                                            Value<UInt64>(to_underlying(joinBuildSide)),
                                            record.read(joinFieldName).as<UInt64>());

    Interface::PagedVectorVarSizedRef pagedVectorVarSizedRef(hjPagedVectorMemRef, schema);
    pagedVectorVarSizedRef.writeRecord(record);
}

void* getDefaultMemRefVarSized() { return nullptr; }

void HJBuildSlicingVarSized::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // We override the Operator::open() and have to call it explicitly here, as we must set the statistic id
    Operator::open(ctx, recordBuffer);
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    Value<MemRef> dummyRef1 = FunctionCall("getDefaultMemRef", getDefaultMemRefVarSized);
    Value<MemRef> dummyRef2 = FunctionCall("getDefaultMemRef", getDefaultMemRefVarSized);
    auto joinState = std::make_unique<LocalHashJoinState>(operatorHandlerMemRef, dummyRef1, dummyRef2);
    ctx.setLocalOperatorState(this, std::move(joinState));
}
}// namespace NES::Runtime::Execution::Operators
