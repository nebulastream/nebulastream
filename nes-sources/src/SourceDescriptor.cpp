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

#include <Sources/SourceDescriptor.hpp>

#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Sources/LogicalSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <ProtobufHelper.hpp> /// NOLINT
#include <SerializableOperator.pb.h>

namespace NES
{

SourceDescriptor::SourceDescriptor(
    const PhysicalSourceId physicalSourceId,
    LogicalSource logicalSource,
    std::string_view sourceType,
    DescriptorConfig::Config config,
    BufferRefDescriptor bufferRefDescriptor)
    : Descriptor(std::move(config))
    , physicalSourceId(physicalSourceId)
    , logicalSource(std::move(logicalSource))
    , sourceType(std::move(sourceType))
    , bufferRefDescriptor(std::move(bufferRefDescriptor))
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

BufferRefDescriptor SourceDescriptor::getBufferRefDescriptor() const
{
    return bufferRefDescriptor;
}

PhysicalSourceId SourceDescriptor::getPhysicalSourceId() const
{
    return physicalSourceId;
}

std::weak_ordering operator<=>(const SourceDescriptor& lhs, const SourceDescriptor& rhs)
{
    return lhs.physicalSourceId <=> rhs.physicalSourceId;
}

std::string SourceDescriptor::explain(ExplainVerbosity verbosity) const
{
    std::stringstream stringstream;
    if (verbosity == ExplainVerbosity::Debug)
    {
        stringstream << *this;
    }
    else if (verbosity == ExplainVerbosity::Short)
    {
        stringstream << fmt::format("{}", logicalSource.getLogicalSourceName());
    }
    return stringstream.str();
}

std::ostream& operator<<(std::ostream& out, const SourceDescriptor& descriptor)
{
    return out << fmt::format(
               "SourceDescriptor(sourceId: {}, sourceType: {}, logicalSource:{}, bufferRefDescriptor: {})",
               descriptor.getPhysicalSourceId(),
               descriptor.getSourceType(),
               descriptor.getLogicalSource(),
               descriptor.getBufferRefDescriptor());
}

SerializableSourceDescriptor SourceDescriptor::serialize() const
{
    SerializableSourceDescriptor serializableSourceDescriptor;
    SchemaSerializationUtil::serializeSchema(*logicalSource.getSchema(), serializableSourceDescriptor.mutable_sourceschema());
    serializableSourceDescriptor.set_logicalsourcename(logicalSource.getLogicalSourceName());
    serializableSourceDescriptor.set_sourcetype(sourceType);

    serializableSourceDescriptor.set_physicalsourceid(physicalSourceId.getRawValue());

    /// Serialize parser config.
    auto* const serializedParserConfig = SerializableBufferRefDescriptor().New();
    serializedParserConfig->set_bufferreftype(bufferRefDescriptor.getBufferRefType());
    serializedParserConfig->set_allocated_config(bufferRefDescriptor.getConfig());

    /// Iterate over SourceDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : getConfig())
    {
        auto* kv = serializableSourceDescriptor.mutable_config();
        kv->emplace(key, descriptorConfigTypeToProto(value));
    }
    return serializableSourceDescriptor;
}
}
