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
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <HL7InputFormatIndexer.hpp>
#include <Hl7FieldIndex.hpp>
#include <Hl7SimdKernel.hpp>
#include <InputFormatIndexer.hpp>
#include <RawTupleBuffer.hpp>

namespace NES
{

/// SIMD variant of the HL7 indexer (see HL7InputFormatIndexer.hpp for the format model): a
/// 64-byte-block broadcast-compare kernel (Hl7SimdKernel) whose event bits are FLATTENED branchless
/// into one raw position band (Hl7Scan.hpp) that the custom Hl7FieldIndex reads in place with a
/// fixed stride -- no per-message group framing, no FieldOffsets emission (the SchemaJSON
/// single-band design). Shares HL7MetaData and the config surface with the scalar indexer.
/// Additional requirement: the message delimiter must be exactly the two-byte MLLP trailer shape
/// (first byte not in the structural class) -- streams with other delimiters use the scalar "HL7"
/// indexer.
class SIMDHL7InputFormatIndexer final : public InputFormatIndexer<SIMDHL7InputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "SIMDHL7";
    static constexpr bool IsSequential = false;

    using IndexerMetaData = HL7MetaData;
    using FieldIndexFunctionType = Hl7FieldIndex;

    SIMDHL7InputFormatIndexer() : computeBlocks(SimdHl7::selectComputeBlocks()) { }
    ~SIMDHL7InputFormatIndexer() = default;

    void indexRawBuffer(Hl7FieldIndex& fieldIndex, const RawTupleBuffer& rawBuffer, const HL7MetaData& metaData) const;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const SIMDHL7InputFormatIndexer& obj);

private:
    SimdHl7::ComputeBlocksFn computeBlocks;
};

}
