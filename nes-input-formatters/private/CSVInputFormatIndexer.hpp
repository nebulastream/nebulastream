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
#include <ostream>
#include <string_view>

#include <DataTypes/Schema.hpp>
#include <InputFormatters/InputFormatterTupleBufferRef.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexer.hpp>
#include <RawValueParser.hpp>

namespace NES
{

constexpr auto CSV_NUM_OFFSETS_PER_FIELD = NumRequiredOffsetsPerField::ONE;

struct CSVMetaData
{
    static constexpr size_t SIZE_OF_TUPLE_DELIMITER = 1;
    static constexpr size_t SIZE_OF_FIELD_DELIMITER = 1;

    explicit CSVMetaData(const ParserConfig& config, const MemoryLayout& memoryLayout)
        : tupleDelimiter(config.tupleDelimiter.front()), fieldDelimiter(config.fieldDelimiter.front()), schema(memoryLayout.getSchema())
    {
        PRECONDITION(
            config.tupleDelimiter.size() == SIZE_OF_TUPLE_DELIMITER,
            "Delimiters must be of size '1 byte', but the tuple delimiter was {} (size {})",
            config.tupleDelimiter,
            config.tupleDelimiter.size());
        PRECONDITION(
            config.fieldDelimiter.size() == SIZE_OF_FIELD_DELIMITER,
            "Delimiters must be of size '1 byte', but the field delimiter was {} (size {})",
            config.fieldDelimiter,
            config.fieldDelimiter.size());
    };

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const { return {&tupleDelimiter, SIZE_OF_TUPLE_DELIMITER}; }

    [[nodiscard]] std::string_view getFieldDelimitingBytes() const { return {&fieldDelimiter, SIZE_OF_FIELD_DELIMITER}; }

    [[nodiscard]] char getTupleDelimiter() const { return tupleDelimiter; }

    [[nodiscard]] char getFieldDelimiter() const { return fieldDelimiter; }

    static QuotationType getQuotationType() { return QuotationType::NONE; }

    [[nodiscard]] const Schema& getSchema() const { return this->schema; }

private:
    char tupleDelimiter;
    char fieldDelimiter;
    Schema schema;
};

class CSVInputFormatIndexer : public InputFormatIndexer<CSVInputFormatIndexer>
{
public:
    using IndexerMetaData = CSVMetaData;
    using FieldIndexFunctionType = FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>;

    CSVInputFormatIndexer() = default;
    ~CSVInputFormatIndexer() = default;

    void indexRawBuffer(
        FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const CSVMetaData& metaData) const;

    friend std::ostream& operator<<(std::ostream& os, const CSVInputFormatIndexer& obj);
};

}
