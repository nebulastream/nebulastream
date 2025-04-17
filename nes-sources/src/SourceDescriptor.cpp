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

#include <cstdint>
#include <string>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <fmt/format.h>
namespace NES::Sources
{
SourceDescriptor::SourceDescriptor(
    Configurations::DescriptorConfig::Config&& config,
    const uint64_t physicalSourceID,
    const WorkerId workerID,
    const LogicalSource& logicalSource,
    const std::string& sourceType,
    const ParserConfig& parserConfig)
    : Descriptor(std::move(config))
    , physicalSourceID(physicalSourceID)
    , logicalSource(logicalSource)
    , workerID(workerID)
    , sourceType(sourceType)
    , parserConfig(parserConfig)
{
}


bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs)
{
    return lhs.logicalSource == rhs.logicalSource && lhs.sourceType == rhs.sourceType;
}
}
fmt::context::iterator
fmt::formatter<NES::Sources::SourceDescriptor>::format(const NES::Sources::SourceDescriptor& sourceDescriptor, format_context& ctx) const
{
    return format_to(
        ctx.out(),
        "SourceDescriptor(sourceType: {}, schema: {}, parserConfig: {{type: {}, tupleDelimiter: {}, stringDelimiter: {} }})",
        sourceDescriptor.getSourceType(),
        *sourceDescriptor.getSchema(),
        sourceDescriptor.getParserConfig().parserType,
        sourceDescriptor.getParserConfig().tupleDelimiter,
        sourceDescriptor.getParserConfig().fieldDelimiter);
}
