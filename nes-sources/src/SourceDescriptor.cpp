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
#include <ostream>
#include <string>
#include <utility>
#include <API/Schema.hpp>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>

namespace NES::Sources
{
SourceDescriptor::SourceDescriptor(
    LogicalSource logicalSource,
    const uint64_t physicalSourceID,
    const WorkerId workerID,
    std::string sourceType,
    const int numberOfBuffersInSourceLocalBufferPool,
    Configurations::DescriptorConfig::Config&& config,
    ParserConfig parserConfig)
    : Descriptor(std::move(config))
    , physicalSourceID(physicalSourceID)
    , logicalSource(std::move(logicalSource))
    , workerID(workerID)
    , sourceType(std::move(sourceType))
    , parserConfig(std::move(parserConfig))
    , buffersInLocalPool(numberOfBuffersInSourceLocalBufferPool)
{
}

LogicalSource SourceDescriptor::getLogicalSource() const
{
    return logicalSource;
}

std::string SourceDescriptor::getSourceType() const
{
    return sourceType;
}

ParserConfig SourceDescriptor::getParserConfig() const
{
    return parserConfig;
}

WorkerId SourceDescriptor::getWorkerId() const
{
    return workerID;
}

uint64_t SourceDescriptor::getPhysicalSourceId() const
{
    return physicalSourceID;
}

int32_t SourceDescriptor::getBuffersInLocalPool() const
{
    return buffersInLocalPool;
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
               *descriptor.getLogicalSource().getSchema(),
               descriptor.getParserConfig().parserType,
               NES::Util::escapeSpecialCharacters(descriptor.getParserConfig().tupleDelimiter),
               NES::Util::escapeSpecialCharacters(descriptor.getParserConfig().fieldDelimiter));
}
}
