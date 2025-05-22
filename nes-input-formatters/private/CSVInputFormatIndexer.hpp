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
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexer.hpp>

namespace NES::InputFormatters
{

constexpr auto CSV_NUM_OFFSETS_PER_FIELD = NumRequiredOffsetsPerField::ONE;

struct CSVMetaData
{
    explicit CSVMetaData(const ParserConfig& config, const Schema&) : tupleDelimiter(config.tupleDelimiter) { };

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const { return this->tupleDelimiter; }

private:
    std::string tupleDelimiter;
};

class CSVInputFormatIndexer : public InputFormatIndexer<CSVInputFormatIndexer>
{
public:
    static constexpr bool IsFormattingRequired = true;
    static constexpr bool HasSpanningTuple = true;
    using IndexerMetaData = CSVMetaData;
    using FieldIndexFunctionType = FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>;

    explicit CSVInputFormatIndexer(ParserConfig config, size_t numberOfFieldsInSchema);
    ~CSVInputFormatIndexer() = default;

    void indexRawBuffer(FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const CSVMetaData&) const;

    friend std::ostream& operator<<(std::ostream& os, const CSVInputFormatIndexer& obj);

private:
    ParserConfig config;
    size_t numberOfFieldsInSchema;
};

}
