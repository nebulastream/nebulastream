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

#include <HL7InputFormatIndexer.hpp>

#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <FieldOffsets.hpp>
#include <Hl7Scan.hpp>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <InputFormatterValidationRegistry.hpp>
#include <RawTupleBuffer.hpp>

namespace NES
{

void HL7InputFormatIndexer::indexRawBuffer(
    FieldOffsets<HL7_NUM_OFFSETS_PER_FIELD>& fieldOffsets, const RawTupleBuffer& rawBuffer, const HL7MetaData& metaData) const
{
    SimdHl7::indexHl7ScalarInto(
        fieldOffsets,
        rawBuffer.getBufferView(),
        metaData.getTupleDelimitingBytes(),
        metaData.getStructuralBytes(),
        metaData.getNumberOfFields());
}

DescriptorConfig::Config HL7InputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    /// SQL string literals reach the config verbatim (the parser does no escape processing), so
    /// values carrying control bytes are written as escape sequences ('\r', '\x1C\r') and unescaped
    /// here. The C++-literal defaults never pass through this map.
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
RegisterHL7InputFormatIndexer(InputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createInputFormatterWithIndexer(HL7InputFormatIndexer{});
}

InputFormatterValidationRegistryReturnType
InputFormatterValidationGeneratedRegistrar::RegisterHL7InputFormatterValidation(InputFormatterValidationRegistryArguments arguments)
{
    return HL7InputFormatIndexer::validateAndFormat(arguments.config);
}

std::ostream& operator<<(std::ostream& os, const HL7InputFormatIndexer&)
{
    return os << fmt::format("HL7InputFormatIndexer()");
}
}
