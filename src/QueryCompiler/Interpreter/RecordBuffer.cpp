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
#include <QueryCompiler/Interpreter/RecordBuffer.hpp>
#include <QueryCompiler/Interpreter/Record.hpp>
#include <QueryCompiler/Interpreter/Values/NesInt32.hpp>
#include <Runtime/MemoryLayout/DynamicMemoryLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
namespace NES::QueryCompilation {

RecordBuffer::RecordBuffer(Runtime::TupleBuffer& buffer, SchemaPtr schema) : buffer(buffer), schema(schema) {}

uint64_t RecordBuffer::getNumberOfTuples() { return buffer.getNumberOfTuples(); }

RecordPtr RecordBuffer::operator[](std::size_t index) const {
    auto values = std::vector<NesValuePtr>();
    //auto recordSize = schema->getSchemaSizeInBytes();
    for (auto field : schema->fields) {
      //  uint32_t value = recordSize * index + buffer.getBufferSize();
        values.emplace_back(std::make_shared<NesInt32>(index));
    }
    return std::make_shared<Record>(values);
}

}// namespace NES::QueryCompilation