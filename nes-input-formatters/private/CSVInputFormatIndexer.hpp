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
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <InputFormatters/InputFormatterTupleBufferRef.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexer.hpp>
#include <RawValueParser.hpp>
#include <static.hpp>

namespace NES
{

constexpr auto CSV_NUM_OFFSETS_PER_FIELD = NumRequiredOffsetsPerField::ONE;

struct CSVMetaData
{
    static constexpr size_t SIZE_OF_TUPLE_DELIMITER = 1;
    static constexpr size_t SIZE_OF_FIELD_DELIMITER = 1;

    explicit CSVMetaData(const ParserConfig& config, const TupleBufferRef& tupleBufferRef)
        : tupleDelimiter(config.tupleDelimiter.front())
        , fieldDelimiter(config.fieldDelimiter.front())
        , fieldNames(tupleBufferRef.getAllFieldNames())
        , fieldDataTypes(tupleBufferRef.getAllDataTypes())
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

    static std::vector<std::string> getNullValues() { return {""}; }

    [[nodiscard]] const Record::RecordFieldIdentifier& getFieldNameAt(const nautilus::static_val<uint64_t>& i) const
    {
        PRECONDITION(i < fieldNames.size(), "Trying to access position, larger than the size of fieldNames {}", fieldNames.size());
        return fieldNames[i];
    }

    [[nodiscard]] const DataType& getFieldDataTypeAt(const nautilus::static_val<uint64_t>& i) const
    {
        PRECONDITION(
            i < fieldDataTypes.size(), "Trying to access position, larger than the size of fieldDataTypes {}", fieldDataTypes.size());
        return fieldDataTypes[i];
    }

    [[nodiscard]] uint64_t getNumberOfFields() const
    {
        INVARIANT(fieldNames.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
        return fieldNames.size();
    }

private:
    char tupleDelimiter;
    char fieldDelimiter;
    std::vector<Record::RecordFieldIdentifier> fieldNames;
    std::vector<DataType> fieldDataTypes;
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
