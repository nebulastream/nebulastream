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
#include <Nautilus/DataTypes/FixedSizeExecutableDataType.hpp>
#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJSlice.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJBuildSlicing.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Stores the reference to the hash table, slice start, slice end, and the slice reference
 */
class LocalJoinState : public Operators::OperatorState {
  public:
    LocalJoinState(MemRefVal& operatorHandler, MemRefVal& hashTableReference, MemRefVal& sliceReference)
        : joinOperatorHandler(operatorHandler), hashTableReference(hashTableReference), sliceReference(sliceReference),
          sliceStart(0_u64), sliceEnd(0_u64){};
    MemRefVal joinOperatorHandler;
    MemRefVal hashTableReference;
    MemRefVal sliceReference;
    UInt64Val sliceStart;
    UInt64Val sliceEnd;
};

void* getHJSliceProxy(void* ptrOpHandler, uint64_t timeStamp, uint64_t joinStrategyInt, uint64_t windowingStrategyInt) {
    NES_DEBUG("getHJSliceProxy with ts={}", timeStamp);
    auto* opHandler = StreamJoinOperator::getSpecificOperatorHandler(ptrOpHandler, joinStrategyInt, windowingStrategyInt);
    auto currentSlice = dynamic_cast<HJOperatorHandlerSlicing*>(opHandler)->getSliceByTimestampOrCreateIt(timeStamp);
    NES_ASSERT2_FMT(currentSlice != nullptr, "invalid window");
    return currentSlice.get();
}

uint64_t getSliceStartProxy(void* ptrHashSlice) {
    NES_ASSERT2_FMT(ptrHashSlice != nullptr, "hash window handler context should not be null");
    auto* hashSlice = static_cast<HJSlice*>(ptrHashSlice);
    return hashSlice->getSliceStart();
}

uint64_t getSliceEndProxy(void* ptrHashSlice) {
    NES_ASSERT2_FMT(ptrHashSlice != nullptr, "hash window handler context should not be null");
    auto* hashSlice = static_cast<HJSlice*>(ptrHashSlice);
    return hashSlice->getSliceEnd();
}

void* getLocalHashTableProxy(void* ptrHashSlice, uint32_t workerThreadId, uint64_t joinBuildSideInt) {
    NES_ASSERT2_FMT(ptrHashSlice != nullptr, "hash window handler context should not be null");
    auto* hashSlice = static_cast<HJSlice*>(ptrHashSlice);
    auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    NES_DEBUG("Insert into HT for window={} is left={} workerThreadId={}",
              hashSlice->getSliceIdentifier(),
              magic_enum::enum_name(joinBuildSide),
              workerThreadId);
    auto ptr = hashSlice->getHashTable(joinBuildSide, WorkerThreadId(workerThreadId));
    auto localHashTablePointer = static_cast<void*>(ptr);
    return localHashTablePointer;
}

HJBuildSlicing::HJBuildSlicing(const uint64_t operatorHandlerIndex,
                               const SchemaPtr& schema,
                               const std::string& joinFieldName,
                               const QueryCompilation::JoinBuildSideType joinBuildSide,
                               const uint64_t entrySize,
                               TimeFunctionPtr timeFunction,
                               QueryCompilation::StreamJoinStrategy joinStrategy)
    : StreamJoinOperator(joinStrategy, QueryCompilation::WindowingStrategy::SLICING), StreamJoinBuild(operatorHandlerIndex,
                                                                                                      schema,
                                                                                                      joinFieldName,
                                                                                                      joinBuildSide,
                                                                                                      entrySize,
                                                                                                      std::move(timeFunction),
                                                                                                      joinStrategy,
                                                                                                      windowingStrategy) {}

void HJBuildSlicing::execute(ExecutionContext& ctx, Record& record) const {
    auto joinState = static_cast<LocalJoinState*>(ctx.getLocalState(this));
    auto operatorHandlerMemRef = joinState->joinOperatorHandler;
    UInt64Val tsValue = timeFunction->getTs(ctx, record);

    //check if we can reuse window
    if (!(joinState->sliceStart <= tsValue && tsValue < joinState->sliceEnd)) {
        //we need a new slice
        joinState->sliceReference =
            nautilus::invoke(getHJSliceProxy,
                             operatorHandlerMemRef,
                             UInt64Val(tsValue),
                             UInt64Val(to_underlying<QueryCompilation::StreamJoinStrategy>(joinStrategy)),
                             UInt64Val(to_underlying<QueryCompilation::WindowingStrategy>(windowingStrategy)));

        joinState->hashTableReference = nautilus::invoke(getLocalHashTableProxy,
                                                         joinState->sliceReference,
                                                         ctx.getWorkerThreadId(),
                                                         UInt64Val(to_underlying(joinBuildSide)));

        joinState->sliceStart = nautilus::invoke(getSliceStartProxy, joinState->sliceReference);
        joinState->sliceEnd = nautilus::invoke(getSliceEndProxy, joinState->sliceReference);

        //        NES_DEBUG("reinit join state with start={} end={} for ts={} for isLeftSide={}",
        //                  joinState->sliceStart->toString(),
        //                  joinState->sliceEnd->toString(),
        //                  tsValue->toString(),
        //                  to_underlying(joinBuildSide));
    }

    //get position in the HT where to write to auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto entryMemRef = nautilus::invoke(insertFunctionProxy, joinState->hashTableReference, record.read(joinFieldName)->as<ExecDataUInt64>()->getRawValue());
    //write data
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (auto& field : nautilus::static_iterable(schema->fields)) {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
//        NES_TRACE("write key={} value={}", field->getName(), record.read(fieldName)->toString());
        writeFixedExecDataTypeToMemRef(entryMemRef, record.read(fieldName));
        entryMemRef = entryMemRef + UInt64Val(fieldType->size());
    }
}

void* getDefaultMemRef() { return nullptr; }

void HJBuildSlicing::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // We override the Operator::open() and have to call it explicitly here, as we must set the statistic id
    Operator::open(ctx, recordBuffer);
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    MemRefVal dummyRef1 = nautilus::invoke(getDefaultMemRef);
    MemRefVal dummyRef2 = nautilus::invoke(getDefaultMemRef);
    auto joinState = std::make_unique<LocalJoinState>(operatorHandlerMemRef, dummyRef1, dummyRef2);
    ctx.setLocalOperatorState(this, std::move(joinState));
}
}// namespace NES::Runtime::Execution::Operators
