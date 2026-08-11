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

#include <ostream>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <InputFormatIndexer.hpp>
#include <RawTupleBuffer.hpp>
#include <SchemaJSONFieldIndex.hpp>
#include <SchemaJSONInputFormatIndexer.hpp>

namespace NES
{

/// Sequential (single-thread, in-order) variant of the SchemaJSON indexer (see
/// SchemaJSONInputFormatIndexer.hpp for the format model), the SequentialCSV<->SIMDCSV split: indexing is
/// IDENTICAL (stage 1 over the region between the first and last tuple delimiter), only the spanning-tuple
/// resolution differs -- IsSequential = true selects the InputFormatter's single-threaded carry accumulator
/// (leading row completed from the carried tail, trailing partial carried to the next buffer) instead of the
/// SequenceShredder. The stage-1 slot pool is keyed by FIF address, so the sequential FIF roles get their own
/// persistent buffers exactly like the parallel ones.
class SequentialSchemaJSONInputFormatIndexer final : public InputFormatIndexer<SequentialSchemaJSONInputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "SequentialSchemaJSON";
    static constexpr bool IsSequential = true;
    static constexpr char TUPLE_DELIMITER = SchemaJSONInputFormatIndexer::TUPLE_DELIMITER;

    using IndexerMetaData = SchemaJSONMetaData;
    using FieldIndexFunctionType = SchemaJSONFieldIndex;

    SequentialSchemaJSONInputFormatIndexer() = default;
    ~SequentialSchemaJSONInputFormatIndexer() = default;

    void indexRawBuffer(SchemaJSONFieldIndex& fieldIndex, const RawTupleBuffer& rawBuffer, const SchemaJSONMetaData& metaData) const
    {
        inner.indexRawBuffer(fieldIndex, rawBuffer, metaData);
    }

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const SequentialSchemaJSONInputFormatIndexer& indexer);

private:
    SchemaJSONInputFormatIndexer inner;
};

}
