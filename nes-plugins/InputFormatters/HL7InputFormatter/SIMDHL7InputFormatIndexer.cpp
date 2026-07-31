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

#include <SIMDHL7InputFormatIndexer.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <Hl7FieldIndex.hpp>
#include <Hl7Scan.hpp>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <InputFormatterValidationRegistry.hpp>
#include <RawTupleBuffer.hpp>

namespace NES
{
namespace
{
/// The flatten emits uint32_t positions; Hl7FieldIndex reads them back as FieldIndex entries.
static_assert(std::is_same_v<FieldIndex, uint32_t>, "the HL7 flatten band assumes FieldIndex == uint32_t");

/// One persistent band per FIF ROLE per thread (the SchemaJSON stage-1 pool pattern): the flattened
/// event band must survive from the indexing phase into the read phase, and a single readBuffer pass
/// reads THREE distinct bands (rawBufferFIF + leading + trailing spanning FIFs) -- so they must not
/// share one buffer. Keying by the FIF's address gives each role its own persistent buffer: the
/// three FIFs are fixed members of the InputFormatter's static thread_local IndexPhaseResult, so
/// their addresses are stable across buffers even though the POD FIF state is reset each buffer.
/// The vectors only ever grow (to the largest buffer seen), so the steady state does no allocation.
struct BandSlot
{
    const void* owner = nullptr;
    std::vector<uint32_t> band;
    std::vector<uint32_t> pairBandIdx;
};

/// At most three distinct owners exist per (thread, SIMDHL7 formatter); 8 is comfortable headroom.
thread_local std::array<BandSlot, 8> tlBandPool;

BandSlot& acquireSlotFor(const void* const owner)
{
    for (auto& slot : tlBandPool)
    {
        if (slot.owner == owner)
        {
            return slot;
        }
    }
    for (auto& slot : tlBandPool)
    {
        if (slot.owner == nullptr)
        {
            slot.owner = owner;
            return slot;
        }
    }
    throw CannotFormatSourceData("SIMDHL7: band slot pool exhausted (more than {} concurrent FIF roles)", tlBandPool.size());
}
}

void SIMDHL7InputFormatIndexer::indexRawBuffer(
    Hl7FieldIndex& fieldIndex, const RawTupleBuffer& rawBuffer, const HL7MetaData& metaData) const
{
    const auto messageDelimiter = metaData.getTupleDelimitingBytes();
    PRECONDITION(
        messageDelimiter.size() == 1 || messageDelimiter.size() == 2,
        "SIMDHL7 requires a one-byte (e.g. packed-XML '\\n') or two-byte (the MLLP <FS><CR> trailer) message delimiter, "
        "but got {} bytes; use the scalar HL7 indexer for longer delimiters",
        messageDelimiter.size());
    PRECONDITION(
        metaData.getStructuralBytes().find(messageDelimiter[0]) == std::string_view::npos,
        "SIMDHL7 requires the message delimiter's first byte to not be a structural delimiter");

    const auto numFields = metaData.getNumberOfFields();
    fieldIndex.startSetup(static_cast<uint32_t>(numFields));

    const auto view = rawBuffer.getBufferView();
    auto& slot = acquireSlotFor(&fieldIndex);
    /// Worst-case capacity (every byte an event) + the flatten's fixed-batch overshoot slack.
    if (slot.band.size() < view.size() + 16)
    {
        slot.band.resize(view.size() + 16);
    }
    if (slot.pairBandIdx.size() < (view.size() / 2) + 2)
    {
        slot.pairBandIdx.resize((view.size() / 2) + 2);
    }

    /// 1-byte mode passes msgFirst twice: the flatten ignores msgSecond, the kernel's compare needs A byte.
    const auto flattened = messageDelimiter.size() == 2 ? SimdHl7::flattenHl7SimdInto<2>(
                                                              slot.band.data(),
                                                              slot.pairBandIdx.data(),
                                                              view,
                                                              metaData.getStructuralChars(),
                                                              messageDelimiter[0],
                                                              messageDelimiter[1],
                                                              computeBlocks)
                                                        : SimdHl7::flattenHl7SimdInto<1>(
                                                              slot.band.data(),
                                                              slot.pairBandIdx.data(),
                                                              view,
                                                              metaData.getStructuralChars(),
                                                              messageDelimiter[0],
                                                              messageDelimiter[0],
                                                              computeBlocks);

    /// No delimiter pair in the buffer: the whole buffer is a spanning-tuple fragment.
    if (flattened.numPairs == 0)
    {
        fieldIndex.markNoTupleDelimiters();
        return;
    }
    /// Complete messages live strictly between consecutive pairs; leading/trailing-fragment events
    /// sit in the band outside [firstPair, lastPair] and are never addressed by the read phase.
    SimdHl7::validateHl7MessageArity(slot.pairBandIdx.data(), flattened.numPairs, numFields);
    fieldIndex.setFlattenResult(slot.band.data(), slot.pairBandIdx[0], flattened.numPairs - 1);
    fieldIndex.markWithTupleDelimiters(slot.band[slot.pairBandIdx[0]], slot.band[slot.pairBandIdx[flattened.numPairs - 1]]);
}

DescriptorConfig::Config SIMDHL7InputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    /// Same config surface and escape handling as the scalar HL7 indexer.
    for (const auto* const key :
         {"message_delimiter", "segment_delimiter", "field_delimiter", "component_delimiter", "subcomponent_delimiter"})
    {
        if (const auto entry = config.find(key); entry != config.end())
        {
            entry->second = unescapeSpecialCharacters(entry->second);
        }
    }
    return DescriptorConfig::validateAndFormat<ConfigParametersHL7InputFormatIndexer>(std::move(config), NAME);
}

InputFormatIndexerRegistryReturnType
RegisterSIMDHL7InputFormatIndexer(InputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createInputFormatterWithIndexer(SIMDHL7InputFormatIndexer{});
}

InputFormatterValidationRegistryReturnType
InputFormatterValidationGeneratedRegistrar::RegisterSIMDHL7InputFormatterValidation(InputFormatterValidationRegistryArguments arguments)
{
    return SIMDHL7InputFormatIndexer::validateAndFormat(arguments.config);
}

std::ostream& operator<<(std::ostream& os, const SIMDHL7InputFormatIndexer&)
{
    return os << fmt::format("SIMDHL7InputFormatIndexer()");
}
}
