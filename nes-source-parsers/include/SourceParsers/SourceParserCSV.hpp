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

#include <API/Schema.hpp>
#include <SourceParsers/SourceParser.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::SourceParsers
{

/// Defined in implementation of ParserCSV
class ProgressTracker;

class SourceParserCSV : public SourceParser
{
public:
    SourceParserCSV(SchemaPtr schema, const std::vector<NES::PhysicalTypePtr>& physicalTypes, std::string fieldDelimiter);
    ~SourceParserCSV() override;

    void parseTupleBufferRaw(
        const Memory::TupleBuffer& tbRaw,
        Memory::AbstractBufferProvider& bufferProvider,
        size_t numBytesInTBRaw,
        const std::function<void(Memory::TupleBuffer& buffer, bool addBufferMetaData)>& emitFunction) override;

private:
    SchemaPtr schema;
    std::string fieldDelimiter;
    std::vector<NES::PhysicalTypePtr> physicalTypes;
    std::unique_ptr<ProgressTracker> progressTracker;
    std::vector<size_t> fieldSizes;

    /// Splits the string-tuple into string-fields, parsing each string-field, converting it to the internal representation.
    /// Assumptions: input is a string that contains either:
    /// - one complete tuple
    /// - a string containing a partial tuple (potentially with a partial field) and another string that contains the rest of the tuple.
    void parseStringTupleToTBFormatted(std::string_view stringTuple, NES::Memory::AbstractBufferProvider& bufferProvider) const;
};

}
