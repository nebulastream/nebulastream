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

ParserConfig ParserConfig::create(std::unordered_map<std::string, std::string> configMap)
{
    ParserConfig created{};
    if (const auto parserType = configMap.find("type"); parserType != configMap.end())
    {
        created.parserType = parserType->second;
    }
    else
    {
        throw InvalidConfigParameter("Parser configuration must contain: type");
    }
    if (const auto tupleDelimiter = configMap.find("tuple_delimiter"); tupleDelimiter != configMap.end())
    {
        /// TODO #651: Add full support for tuple delimiters that are larger than one byte.
        PRECONDITION(tupleDelimiter->second.size() == 1, "We currently do not support tuple delimiters larger than one byte.");
        created.tupleDelimiter = tupleDelimiter->second;
    }
    else
    {
        NES_DEBUG("Parser configuration did not contain: tuple_delimiter, using default: \\n");
        created.tupleDelimiter = '\n';
    }
    if (const auto fieldDelimiter = configMap.find("field_delimiter"); fieldDelimiter != configMap.end())
    {
        created.fieldDelimiter = fieldDelimiter->second;
    }
    else
    {
        NES_DEBUG("Parser configuration did not contain: field_delimiter, using default: ,");
        created.fieldDelimiter = ",";
    }
    return created;
}

std::ostream& operator<<(std::ostream& os, const ParserConfig& obj)
{
    return os << fmt::format(
               "ParserConfig(type: {}, tupleDelimiter: {}, fieldDelimiter: {})", obj.parserType, obj.tupleDelimiter, obj.fieldDelimiter);
}

SourceDescriptor::SourceDescriptor(
    const PhysicalSourceId physicalSourceId,
    LogicalSource logicalSource,
    std::string_view sourceType,
    DescriptorConfig::Config config,
    ParserConfig parserConfig)
    : Descriptor(std::move(config))
    , physicalSourceId(physicalSourceId)
    , logicalSource(std::move(logicalSource))
    , sourceType(std::move(sourceType))
    , parserConfig(std::move(parserConfig))
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
               "SourceDescriptor(sourceId: {}, sourceType: {}, logicalSource:{}, parserConfig: {{type: {}, tupleDelimiter: {}, "
               "stringDelimiter: {} }})",
               descriptor.getPhysicalSourceId(),
               descriptor.getSourceType(),
               descriptor.getLogicalSource(),
               descriptor.getParserConfig().parserType,
               escapeSpecialCharacters(descriptor.getParserConfig().tupleDelimiter),
               escapeSpecialCharacters(descriptor.getParserConfig().fieldDelimiter));
}

SerializableSourceDescriptor SourceDescriptor::serialize() const
{
    SerializableSourceDescriptor serializableSourceDescriptor;
    SchemaSerializationUtil::serializeSchema(*logicalSource.getSchema(), serializableSourceDescriptor.mutable_sourceschema());
    serializableSourceDescriptor.set_logicalsourcename(logicalSource.getLogicalSourceName());
    serializableSourceDescriptor.set_sourcetype(sourceType);

    serializableSourceDescriptor.set_physicalsourceid(physicalSourceId.getRawValue());

    /// Serialize parser config.
    auto* const serializedParserConfig = SerializableParserConfig().New();
    serializedParserConfig->set_type(parserConfig.parserType);
    serializedParserConfig->set_tupledelimiter(parserConfig.tupleDelimiter);
    serializedParserConfig->set_fielddelimiter(parserConfig.fieldDelimiter);
    serializableSourceDescriptor.set_allocated_parserconfig(serializedParserConfig);

    /// Iterate over SourceDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : getConfig())
    {
        auto* kv = serializableSourceDescriptor.mutable_config();
        kv->emplace(key, descriptorConfigTypeToProto(value));
    }
    return serializableSourceDescriptor;
}
}
