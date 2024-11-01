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

#pragma once

#include <SourceParsers/SourceParser.hpp>

namespace NES::SourceParsers
{

class SourceParserCSV : public SourceParser
{
public:
    SourceParserCSV(SchemaPtr schema, std::vector<NES::PhysicalTypePtr> physicalTypes, std::string delimiter);

    /// takes csv string line as input, casts its values to the correct types and writes it to the TupleBuffer
    bool writeInputTupleToTupleBuffer(
        std::string_view inputString,
        uint64_t tupleCount,
        NES::Memory::TupleBuffer& tupleBuffer,
        NES::Memory::AbstractBufferProvider& bufferManager) const override;

    size_t getSizeOfSchemaInBytes() const;

private:
    SchemaPtr schema;
    uint64_t numberOfSchemaFields;
    std::vector<NES::PhysicalTypePtr> physicalTypes;
    std::string delimiter;
};

}
