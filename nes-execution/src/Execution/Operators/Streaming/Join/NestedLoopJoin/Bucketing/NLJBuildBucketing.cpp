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
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Bucketing/NLJBuildBucketing.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void* getPagedVectorRefProxy(void* ptrWindowVector, uint64_t index, WorkerThreadId workerThreadId, uint64_t joinBuildSideInt) {
    NES_ASSERT2_FMT(ptrWindowVector != nullptr, "ptrPagedVector should not be null!");
    auto allWindowVec = static_cast<std::vector<NLJSlice*>*>(ptrWindowVector);
    auto nljWindow = allWindowVec->operator[](index);
    auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    NES_INFO("allWindowVec->size(): {}", allWindowVec->size());
    NES_INFO("joinBuildSide: {}", magic_enum::enum_name(joinBuildSide));

    NES_INFO("getPagedVectorRefProxy for index {} workerThreadId {} nljWindow {}", index, workerThreadId, nljWindow->toString());

    switch (joinBuildSide) {
        case QueryCompilation::JoinBuildSideType::Left: return nljWindow->getPagedVectorRefLeft(workerThreadId);
        case QueryCompilation::JoinBuildSideType::Right: return nljWindow->getPagedVectorRefRight(workerThreadId);
    }
}

void NLJBuildBucketing::insertRecordForWindow(Value<MemRef>& allWindowsToFill,
                                              Value<UInt64>& curIndex,
                                              ValueId<WorkerThreadId>& workerThreadId,
                                              Record& record) const {
    auto curPagedVectorRef = Nautilus::FunctionCall("getPagedVectorRefProxy",
                                                    getPagedVectorRefProxy,
                                                    allWindowsToFill,
                                                    curIndex,
                                                    workerThreadId,
                                                    Value<UInt64>(to_underlying(joinBuildSide)));

    // Write record to the pagedVector
    auto pagedVectorVarSizedRef = Nautilus::Interface::PagedVectorVarSizedRef(curPagedVectorRef, schema);
    pagedVectorVarSizedRef.writeRecord(record);
}

NLJBuildBucketing::NLJBuildBucketing(const uint64_t operatorHandlerIndex,
                                     const SchemaPtr& schema,
                                     const QueryCompilation::JoinBuildSideType joinBuildSide,
                                     const uint64_t entrySize,
                                     TimeFunctionPtr timeFunction,
                                     QueryCompilation::StreamJoinStrategy joinStrategy,
                                     const uint64_t windowSize,
                                     const uint64_t windowSlide)
    : StreamJoinOperator(joinStrategy, QueryCompilation::WindowingStrategy::BUCKETING),
      StreamJoinBuildBucketing(operatorHandlerIndex,
                               schema,
                               joinBuildSide,
                               entrySize,
                               std::move(timeFunction),
                               joinStrategy,
                               windowingStrategy,
                               windowSize,
                               windowSlide) {}

}// namespace NES::Runtime::Execution::Operators
