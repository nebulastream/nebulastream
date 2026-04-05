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

#include <ArrowOutputFormatter.hpp>

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <fmt/format.h>

#include <OutputFormatterRegistry.hpp>
#include <OutputFormatterValidationRegistry.hpp>

namespace NES
{

ArrowOutputFormatter::ArrowOutputFormatter(const std::vector<Record::RecordFieldIdentifier>& fieldNames)
    : OutputFormatter(fieldNames)
{
}

nautilus::val<uint64_t> ArrowOutputFormatter::writeFormattedValue(
    const VarVal&,
    const DataType&,
    const nautilus::static_val<uint64_t>&,
    const nautilus::val<int8_t*>&,
    const nautilus::val<uint64_t>&,
    const RecordBuffer&,
    const nautilus::val<AbstractBufferProvider*>&) const
{
    /// This method should never be called — Arrow format uses ArrowBufferRef, not OutputFormatterBufferRef.
    /// The LowerSchemaProvider routes Arrow format directly to ArrowBufferRef.
    throw std::logic_error("ArrowOutputFormatter::writeFormattedValue should not be called. "
                           "Arrow format uses ArrowBufferRef for buffer writing.");
}

DescriptorConfig::Config ArrowOutputFormatter::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<OutputFormatterConfig::ConfigParametersArrow>(std::move(config), "Arrow");
}

std::ostream& operator<<(std::ostream& out, const ArrowOutputFormatter&)
{
    return out << fmt::format("ArrowOutputFormatter()");
}

OutputFormatterValidationRegistryReturnType
OutputFormatterValidationGeneratedRegistrar::RegisterArrowOutputFormatterValidation(OutputFormatterValidationRegistryArguments args)
{
    return ArrowOutputFormatter::validateAndFormat(std::move(args.config));
}

OutputFormatterRegistryReturnType OutputFormatterGeneratedRegistrar::RegisterArrowOutputFormatter(OutputFormatterRegistryArguments args)
{
    return std::make_unique<ArrowOutputFormatter>(std::move(args.fieldNames));
}
}
