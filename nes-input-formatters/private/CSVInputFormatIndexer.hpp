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
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <RawValueParser.hpp>
#include <static.hpp>

namespace NES
{


class CSVInputFormatIndexer : public InputFormatIndexer
{
    /// Passkey idiom (to enforce checks before calling the constructor)
    struct Private
    {
        explicit Private() = default;
    };

public:
    static constexpr size_t SIZE_OF_TUPLE_DELIMITER = 1;
    static constexpr size_t SIZE_OF_FIELD_DELIMITER = 1;

    explicit CSVInputFormatIndexer(
        Private, const char tupleDelimiter, const char fieldDelimiter, const bool allowCommasInStrings, const size_t numberOfFields)
        : tupleDelimiter(tupleDelimiter)
        , fieldDelimiter(fieldDelimiter)
        , allowCommasInStrings(allowCommasInStrings)
        , numberOfFields(numberOfFields)
        , nullValues({""})
    {
    }

    /// Delegate constructor that applies preconditions before safely calling the constructor
    static std::unique_ptr<CSVInputFormatIndexer> create(const ParserConfig& config, const TupleBufferRef& tupleBufferRef)
    {
        PRECONDITION(config.tupleDelimiter.size() == 1, "Size of tuple delimiter must be exactly one.");
        PRECONDITION(config.fieldDelimiter.size() == 1, "Size of field delimiter must be exactly one.");
        return std::make_unique<CSVInputFormatIndexer>(
            Private{},
            config.tupleDelimiter.front(),
            config.fieldDelimiter.front(),
            config.allowCommasInStrings,
            tupleBufferRef.getAllDataTypes().size());
    }

    ~CSVInputFormatIndexer() override = default;

    std::unique_ptr<RawBufferIndex> indexRawBuffer(const RawTupleBuffer& rawBuffer) const override;


    friend std::ostream& operator<<(std::ostream& os, const CSVInputFormatIndexer& obj);

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const override { return {&tupleDelimiter, 1}; }

    [[nodiscard]] std::string_view getFieldDelimitingBytes() const override { return {&fieldDelimiter, SIZE_OF_FIELD_DELIMITER}; }

    [[nodiscard]] const std::vector<std::string>& getNullValues() const override { return nullValues; }

private:
    char tupleDelimiter;
    char fieldDelimiter;
    bool allowCommasInStrings{};
    size_t numberOfFields;
    std::vector<std::string> nullValues{};
};

}
