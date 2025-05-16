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

#include <sstream>
#include <API/Schema.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ProtobufHelper.hpp> /// NOLINT
#include <SerializableOperator.pb.h>

namespace NES::Sources
{

SourceDescriptor::SourceDescriptor(
    Schema schema,
    std::string logicalSourceName,
    std::string sourceType,
    const int numberOfBuffersInSourceLocalBufferPool,
    ParserConfig parserConfig,
    Configurations::DescriptorConfig::Config&& config)
    : Descriptor(std::move(config))
    , schema(std::move(schema))
    , logicalSourceName(std::move(logicalSourceName))
    , sourceType(std::move(sourceType))
    , numberOfBuffersInSourceLocalBufferPool(numberOfBuffersInSourceLocalBufferPool)
    , parserConfig(std::move(parserConfig))
{
}

std::ostream& operator<<(std::ostream& out, const SourceDescriptor& sourceDescriptor)
{
    const auto schemaString = sourceDescriptor.schema.toString();
    const auto parserConfigString = fmt::format(
        "type: {}, tupleDelimiter: '{}', stringDelimiter: '{}'",
        sourceDescriptor.parserConfig.parserType,
        Util::escapeSpecialCharacters(sourceDescriptor.parserConfig.tupleDelimiter),
        Util::escapeSpecialCharacters(sourceDescriptor.parserConfig.fieldDelimiter));
    return out << fmt::format(
               "SourceDescriptor( logicalSourceName: {}, sourceType: {}, schema: {}, parserConfig: {}, config: {})",
               sourceDescriptor.logicalSourceName,
               sourceDescriptor.sourceType,
               schemaString,
               parserConfigString,
               sourceDescriptor.toStringConfig());
}

std::string SourceDescriptor::explain(ExplainVerbosity verbosity) const
{
    std::stringstream stringstream;
    if (verbosity == ExplainVerbosity::Debug)
    {
        const auto schemaString = schema.toString();
        const auto parserConfigString = fmt::format(
            "type: {}, tupleDelimiter: '{}', stringDelimiter: '{}'",
            parserConfig.parserType,
            Util::escapeSpecialCharacters(parserConfig.tupleDelimiter),
            Util::escapeSpecialCharacters(parserConfig.fieldDelimiter));
        stringstream << fmt::format(
            "SourceDescriptor( logicalSourceName: {}, sourceType: {}, schema: {}, parserConfig: {}, config: {})",
            logicalSourceName,
            sourceType,
            schemaString,
            parserConfigString,
            toStringConfig());
    }
    else if (verbosity == ExplainVerbosity::Short)
    {
        stringstream << fmt::format("{}", logicalSourceName);
    }
    return stringstream.str();
}

bool operator==(const SourceDescriptor& lhs, const SourceDescriptor& rhs)
{
    return lhs.schema == rhs.schema && lhs.sourceType == rhs.sourceType && lhs.config == rhs.config;
}

SerializableSourceDescriptor SourceDescriptor::serialize() const
{
    SerializableSourceDescriptor serializableSourceDescriptor;
    SchemaSerializationUtil::serializeSchema(schema, serializableSourceDescriptor.mutable_sourceschema());
    serializableSourceDescriptor.set_logicalsourcename(logicalSourceName);
    serializableSourceDescriptor.set_sourcetype(sourceType);
    serializableSourceDescriptor.set_numberofbuffersinsourcelocalbufferpool(numberOfBuffersInSourceLocalBufferPool);

    /// Serialize parser config.
    auto* const serializedParserConfig = NES::ParserConfig().New();
    serializedParserConfig->set_type(parserConfig.parserType);
    serializedParserConfig->set_tupledelimiter(parserConfig.tupleDelimiter);
    serializedParserConfig->set_fielddelimiter(parserConfig.fieldDelimiter);
    serializableSourceDescriptor.set_allocated_parserconfig(serializedParserConfig);

    /// Iterate over SourceDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : config)
    {
        auto* kv = serializableSourceDescriptor.mutable_config();
        kv->emplace(key, descriptorConfigTypeToProto(value));
    }
    return serializableSourceDescriptor;
}
}
