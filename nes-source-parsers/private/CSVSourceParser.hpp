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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>
#include <API/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SourceParsers/SourceParser.hpp>

namespace NES::SourceParsers
{

/// Implementation detail of CSVSourceParser
class ProgressTracker;

class CSVSourceParser : public SourceParser
{
public:
    using CastFunctionSignature
        = std::function<void(std::string inputString, int8_t* fieldPointer, Memory::AbstractBufferProvider& bufferProvider)>;

    CSVSourceParser(const Schema& schema, std::string tupleDelimiter, std::string fieldDelimiter);
    ~CSVSourceParser() override;

    CSVSourceParser(const CSVSourceParser&) = delete;
    CSVSourceParser& operator=(const CSVSourceParser&) = delete;
    CSVSourceParser(CSVSourceParser&&) = delete;
    CSVSourceParser& operator=(CSVSourceParser&&) = delete;

    void parseTupleBufferRaw(
        const Memory::TupleBuffer& tbRaw,
        Memory::AbstractBufferProvider& bufferProvider,
        size_t numBytesInTBRaw,
        const std::function<void(Memory::TupleBuffer& buffer, bool addBufferMetaData)>& emitFunction) override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string fieldDelimiter;
    std::unique_ptr<ProgressTracker> progressTracker;
    std::vector<size_t> fieldSizes;
    std::vector<CastFunctionSignature> fieldParseFunctions;

    /// Splits the string-tuple into string-fields, parsing each string-field, converting it to the internal representation.
    /// Assumptions: input is a string that contains either:
    /// - one complete tuple
    /// - a string containing a partial tuple (potentially with a partial field) and another string that contains the rest of the tuple.
    void parseStringIntoTupleBuffer(std::string_view stringTuple, NES::Memory::AbstractBufferProvider& bufferProvider) const;
};

}
