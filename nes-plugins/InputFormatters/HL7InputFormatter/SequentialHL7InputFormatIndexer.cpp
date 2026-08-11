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

#include <SequentialHL7InputFormatIndexer.hpp>

#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <InputFormatIndexerRegistry.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <InputFormatterValidationRegistry.hpp>

namespace NES
{

DescriptorConfig::Config SequentialHL7InputFormatIndexer::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    /// Same config surface and escape handling as the scalar HL7 and SIMDHL7 indexers.
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
RegisterSequentialHL7InputFormatIndexer(InputFormatIndexerRegistryArguments arguments) ///NOLINT(performance-unnecessary-value-param)
{
    return arguments.createInputFormatterWithIndexer(SequentialHL7InputFormatIndexer{});
}

InputFormatterValidationRegistryReturnType InputFormatterValidationGeneratedRegistrar::RegisterSequentialHL7InputFormatterValidation(
    InputFormatterValidationRegistryArguments arguments)
{
    return SequentialHL7InputFormatIndexer::validateAndFormat(arguments.config);
}

std::ostream& operator<<(std::ostream& os, const SequentialHL7InputFormatIndexer&)
{
    return os << fmt::format("SequentialHL7InputFormatIndexer()");
}
}
