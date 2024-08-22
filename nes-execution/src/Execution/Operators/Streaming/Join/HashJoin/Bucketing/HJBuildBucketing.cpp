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
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <API/AttributeField.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Bucketing/HJBuildBucketing.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJSlice.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <magic_enum.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

#include <API/Schema.hpp>
#include <Configurations/Enums/WindowingStrategy.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinBuildBucketing.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperator.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/FixedPage/FixedPage.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Util/CastUtils.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Runtime::Execution::Operators
{

void* getHashTableRefProxy(void* ptrWindowVector, uint64_t index, WorkerThreadId workerThreadId, uint64_t joinBuildSideInt)
{
    NES_ASSERT2_FMT(ptrWindowVector != nullptr, "ptrPagedVector should not be null!");
    auto allWindowVec = static_cast<std::vector<HJSlice*>*>(ptrWindowVector);
    auto nljWindow = allWindowVec->operator[](index);
    auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();

    NES_INFO("getHashTableRefProxy for index {} workerThreadId {} nljWindow {}", index, workerThreadId, nljWindow->toString());
    return nljWindow->getHashTable(joinBuildSide, workerThreadId);
}

void HJBuildBucketing::insertRecordForWindow(
    Value<MemRef>& allWindowsToFill, Value<UInt64>& curIndex, ValueId<WorkerThreadId>& workerThreadId, Record& record) const
{
    auto hashTableReference = Nautilus::FunctionCall(
        "getHashTableRefProxy",
        getHashTableRefProxy,
        allWindowsToFill,
        curIndex,
        workerThreadId,
        Value<UInt64>(to_underlying(joinBuildSide)));

    ///get position in the HT where to write to auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto entryMemRef
        = Nautilus::FunctionCall("insertFunctionProxy", insertFunctionProxy, hashTableReference, record.read(joinFieldName).as<UInt64>());
    ///write data
    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : schema->fields)
    {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        NES_TRACE("write key={} value={}", field->getName(), record.read(fieldName)->toString());
        entryMemRef.store(record.read(fieldName));
        entryMemRef = entryMemRef + fieldType->size();
    }
}

HJBuildBucketing::HJBuildBucketing(
    const uint64_t operatorHandlerIndex,
    const SchemaPtr& schema,
    const std::string& joinFieldName,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const uint64_t entrySize,
    TimeFunctionPtr timeFunction,
    QueryCompilation::StreamJoinStrategy joinStrategy,
    const uint64_t windowSize,
    const uint64_t windowSlide)
    : StreamJoinOperator(joinStrategy, QueryCompilation::WindowingStrategy::BUCKETING)
    , StreamJoinBuildBucketing(
          operatorHandlerIndex,
          schema,
          joinFieldName,
          joinBuildSide,
          entrySize,
          std::move(timeFunction),
          joinStrategy,
          windowingStrategy,
          windowSize,
          windowSlide)
{
}

} /// namespace NES::Runtime::Execution::Operators
