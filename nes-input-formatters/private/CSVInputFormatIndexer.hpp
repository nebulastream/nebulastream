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
#include <memory>
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
#include <RawBufferIndex.hpp>
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
        PRECONDITION(
            config.tupleDelimiter.size() == 1,
            "Tuple delimiter must be 1 byte but was {:?} (size {})",
            config.tupleDelimiter,
            config.tupleDelimiter.size());
        PRECONDITION(
            config.fieldDelimiter.size() == 1,
            "Field delimiter must be 1 byte but was {:?} (size {})",
            config.fieldDelimiter,
            config.fieldDelimiter.size());
        return std::make_unique<CSVInputFormatIndexer>(
            Private{},
            config.tupleDelimiter.front(),
            config.fieldDelimiter.front(),
            config.allowCommasInStrings,
            tupleBufferRef.getAllDataTypes().size());
    }

    ~CSVInputFormatIndexer() override = default;

    [[nodiscard]] std::unique_ptr<RawBufferIndex> indexRawBuffer(std::string_view rawBuffer) const override;

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const override { return {&tupleDelimiter, SIZE_OF_TUPLE_DELIMITER}; }

    [[nodiscard]] std::string_view getFieldDelimitingBytes() const override { return {&fieldDelimiter, SIZE_OF_FIELD_DELIMITER}; }

    [[nodiscard]] QuotationType getQuotationType() const override { return QuotationType::NONE; }

    [[nodiscard]] const std::vector<std::string>& getNullValues() const override { return nullValues; }

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    char tupleDelimiter;
    char fieldDelimiter;
    bool allowCommasInStrings{};
    size_t numberOfFields;
    std::vector<std::string> nullValues;
};

}
