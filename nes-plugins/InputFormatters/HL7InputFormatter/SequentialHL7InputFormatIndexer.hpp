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
#include <HL7InputFormatIndexer.hpp>
#include <Hl7FieldIndex.hpp>
#include <InputFormatIndexer.hpp>
#include <RawTupleBuffer.hpp>
#include <SIMDHL7InputFormatIndexer.hpp>

namespace NES
{

/// Sequential (single-thread, in-order) variant of the SIMDHL7 indexer (see HL7InputFormatIndexer.hpp for the
/// format model), the SequentialCSV<->SIMDCSV split: indexing is IDENTICAL (the SIMD flatten into the in-place
/// event band, 1- or 2-byte message delimiter), only the spanning-tuple resolution differs -- IsSequential =
/// true selects the InputFormatter's single-threaded carry accumulator instead of the SequenceShredder. The
/// band slot pool is keyed by FIF address, so the sequential FIF roles get their own persistent buffers exactly
/// like the parallel ones. The packed-XML configuration (1-byte '\n' delimiter, structural class {<,>}) works
/// unchanged.
class SequentialHL7InputFormatIndexer final : public InputFormatIndexer<SequentialHL7InputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "SequentialHL7";
    static constexpr bool IsSequential = true;

    using IndexerMetaData = HL7MetaData;
    using FieldIndexFunctionType = Hl7FieldIndex;

    SequentialHL7InputFormatIndexer() = default;
    ~SequentialHL7InputFormatIndexer() = default;

    void indexRawBuffer(Hl7FieldIndex& fieldIndex, const RawTupleBuffer& rawBuffer, const HL7MetaData& metaData) const
    {
        inner.indexRawBuffer(fieldIndex, rawBuffer, metaData);
    }

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const SequentialHL7InputFormatIndexer& obj);

private:
    SIMDHL7InputFormatIndexer inner;
};

}
