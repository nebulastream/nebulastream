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

#include <compare>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>

namespace NES::Sources
{
SourceDescriptor::SourceDescriptor(
    Configurations::DescriptorConfig::Config&& config,
    const uint64_t physicalSourceID,
    const WorkerId workerID,
    const LogicalSource& logicalSource,
    std::string sourceType,
    ParserConfig parserConfig)
    : Descriptor(std::move(config))
    , physicalSourceID(physicalSourceID)
    , logicalSource(logicalSource)
    , workerID(workerID)
    , sourceType(std::move(sourceType))
    , parserConfig(std::move(parserConfig))
{
}

LogicalSource SourceDescriptor::getLogicalSource() const
{
    return logicalSource;
}

std::shared_ptr<const Schema> SourceDescriptor::getSchema() const
{
    return logicalSource.getSchema();
}

std::string SourceDescriptor::getSourceType() const
{
    return sourceType;
}

ParserConfig SourceDescriptor::getParserConfig() const
{
    return parserConfig;
}

WorkerId SourceDescriptor::getWorkerID() const
{
    return workerID;
}

uint64_t SourceDescriptor::getPhysicalSourceID() const
{
    return physicalSourceID;
}


bool operator==(const ParserConfig& lhs, const ParserConfig& rhs)
{
    return lhs.parserType == rhs.parserType && lhs.tupleDelimiter == rhs.tupleDelimiter && lhs.fieldDelimiter == rhs.fieldDelimiter;
}

bool operator!=(const ParserConfig& lhs, const ParserConfig& rhs)
{
    return !(lhs == rhs);
}

std::weak_ordering operator<=>(const SourceDescriptor& lhs, const SourceDescriptor& rhs)
{
    return lhs.physicalSourceID <=> rhs.physicalSourceID;
}

bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs)
{
    return lhs == static_cast<const Configurations::Descriptor&>(rhs) && lhs.physicalSourceID == rhs.physicalSourceID
        && lhs.logicalSource == rhs.logicalSource && lhs.workerID == rhs.workerID && lhs.sourceType == rhs.sourceType
        && lhs.parserConfig == rhs.parserConfig;
}
std::ostream& operator<<(std::ostream& out, const SourceDescriptor& descriptor)
{
    return out << fmt::format(
               "SourceDescriptor(sourceType: {}, schema: {}, parserConfig: {{type: {}, tupleDelimiter: {}, stringDelimiter: {} }})",
               descriptor.getSourceType(),
               *descriptor.getSchema(),
               descriptor.getParserConfig().parserType,
               NES::Util::escapeSpecialCharacters(descriptor.getParserConfig().tupleDelimiter),
               NES::Util::escapeSpecialCharacters(descriptor.getParserConfig().fieldDelimiter));
}
}
