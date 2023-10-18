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
#include <Execution/Operators/Streaming/Join/HashJoin/Bucketing/HJBuildBucketing.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJSlice.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Runtime::Execution::Operators {

void* getHashTableRefProxy(void* ptrWindowVector, uint64_t index, uint64_t workerId, uint64_t joinBuildSideInt) {
    NES_ASSERT2_FMT(ptrWindowVector != nullptr, "ptrPagedVector should not be null!");
    auto allWindowVec = static_cast<std::vector<HJSlice*>*>(ptrWindowVector);
    auto nljWindow = allWindowVec->operator[](index);
    auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();

    NES_INFO("getHashTableRefProxy for index {} workerId {} nljWindow {}", index, workerId, nljWindow->toString());
    return nljWindow->getHashTable(joinBuildSide, workerId);
}

void HJBuildBucketing::insertRecordForWindow(Value<MemRef>& allWindowsToFill,
                                             Value<UInt64>& curIndex,
                                             Value<UInt64>& workerId,
                                             Record& record) const {

    auto hashTableReference = Nautilus::FunctionCall("getHashTableRefProxy",
                                                     getHashTableRefProxy,
                                                     allWindowsToFill,
                                                     curIndex,
                                                     workerId,
                                                     Value<UInt64>(to_underlying(joinBuildSide)));

    //get position in the HT where to write to auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    auto entryMemRef = Nautilus::FunctionCall("insertFunctionProxy",
                                              insertFunctionProxy,
                                              hashTableReference,
                                              record.read(joinFieldName).as<UInt64>());
    //write data
    DefaultPhysicalTypeFactory physicalDataTypeFactory;
    for (auto& field : schema->fields) {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        NES_TRACE("write key={} value={}", field->getName(), record.read(fieldName)->toString());
        entryMemRef.store(record.read(fieldName));
        entryMemRef = entryMemRef + fieldType->size();
    }
}

HJBuildBucketing::HJBuildBucketing(const uint64_t operatorHandlerIndex,
                                   const SchemaPtr& schema,
                                   const std::string& joinFieldName,
                                   const QueryCompilation::JoinBuildSideType joinBuildSide,
                                   const uint64_t entrySize,
                                   TimeFunctionPtr timeFunction,
                                   QueryCompilation::StreamJoinStrategy joinStrategy,
                                   QueryCompilation::WindowingStrategy windowingStrategy,
                                   const uint64_t windowSize,
                                   const uint64_t windowSlide)
    : StreamJoinBuildBucketing(operatorHandlerIndex,
                               schema,
                               joinFieldName,
                               joinBuildSide,
                               entrySize,
                               std::move(timeFunction),
                               joinStrategy,
                               windowingStrategy,
                               windowSize,
                               windowSlide) {}

}// namespace NES::Runtime::Execution::Operators