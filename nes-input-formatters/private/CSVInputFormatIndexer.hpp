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

#include <InputFormatters/InputFormatIndexer.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <FieldOffsets.hpp>
#include <InputFormatIndexerRegistry.hpp>

namespace NES::InputFormatters
{

constexpr auto CSV_NUM_OFFSETS_PER_FIELD = NumRequiredOffsetsPerField::ONE;

struct CSVMetaData
{
    explicit CSVMetaData(const ParserConfig& config, Schema schema) : tupleDelimiter(config.tupleDelimiter), schema(schema) { };

    std::string_view getTupleDelimitingBytes() const { return this->tupleDelimiter; }
    const Schema& getSchema() const { return this->schema; }

private:
    std::string tupleDelimiter;
    Schema schema;
};

class CSVInputFormatIndexer final
    : public InputFormatIndexer<FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>, CSVMetaData, /* IsFormattingRequired */ true>
{
public:
    explicit CSVInputFormatIndexer(ParserConfig config, size_t numberOfFieldsInSchema);
    ~CSVInputFormatIndexer() override = default;

    void indexRawBuffer(
        FieldOffsets<CSV_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const CSVMetaData&) const override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    ParserConfig config;
    size_t numberOfFieldsInSchema;
};

}
