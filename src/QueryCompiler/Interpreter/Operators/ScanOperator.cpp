/*

     Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <API/AttributeField.hpp>
#include <Common/PhysicalTypes/PhysicalTypeFactory.hpp>
#include <QueryCompiler/Interpreter/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Interpreter/Operators/OperatorContext.hpp>

#include <QueryCompiler/Interpreter/Operators/ScanOperator.hpp>
#include <QueryCompiler/Interpreter/Record.hpp>
#include <QueryCompiler/Interpreter/RecordBuffer.hpp>
#include <QueryCompiler/Interpreter/Values/NesInt32.hpp>
#include <QueryCompiler/Interpreter/Values/NesInt64.hpp>
#include <QueryCompiler/Interpreter/Values/NesMemoryAddress.hpp>
#include <QueryCompiler/Interpreter/Values/NesValue.hpp>
namespace NES::QueryCompilation {

ScanOperator::ScanOperator(SchemaPtr inputSchema, ExecutableOperatorPtr next) : Operator(next), inputSchema(inputSchema) {}

RecordPtr ScanOperator::readRecord(RecordBuffer& buffer, NesValuePtr recordIndex) {
    auto values = std::vector<NesValuePtr>();
    auto recordSize = inputSchema->getSchemaSizeInBytes();
    auto physicalFieldFactory = DefaultPhysicalTypeFactory();

    int64_t fieldOffset = 0L;
    for (uint64_t i = 0; i < inputSchema->fields.size(); i++) {
        auto field = inputSchema->fields[i];
        auto physicalType = physicalFieldFactory.getPhysicalType(field->getDataType());
        auto offset = (recordIndex * recordSize) + fieldOffset;
        // TODO extract read
        auto buf = buffer.getBufferAddress();
        auto address = std::dynamic_pointer_cast<NesMemoryAddress>(buf + offset);
        int32_t value = *reinterpret_cast<int32_t*>(address->getValue());
        values.emplace_back(std::make_shared<NesInt32>(value));
        fieldOffset = fieldOffset + physicalType->size();
    }

    return std::make_shared<Record>(values);
}

void ScanOperator::execute(RecordBuffer& buffer, ExecutionContextPtr ctx) {
    for (NesValuePtr i = std::make_shared<NesInt32>(0); i < buffer.getNumberOfTuples(); i = i + 1) {
        auto record = readRecord(buffer, i);
        next->execute(record, ctx);
    }
}
}// namespace NES::QueryCompilation
