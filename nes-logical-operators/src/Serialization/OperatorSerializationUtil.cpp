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

#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/OperatorSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <Configurations/Descriptor.hpp>
#include <Operators/LogicalOperator.hpp>

namespace NES
{


LogicalOperator OperatorSerializationUtil::deserializeOperator(SerializableOperator serializedOperator)
{
    if (serializedOperator.has_source())
    {
        const auto& sourceDescriptor = serializedOperator.source();
        return deserializeSourceOperator(sourceDescriptor);
    }
    else if (serializedOperator.has_sink())
    {
        const auto& sink = serializedOperator.sink();
        return deserializeSinkOperator(sink);
    }
    else if (serializedOperator.has_operator_())
    {
        NES::Configurations::DescriptorConfig::Config config;
        for (const auto& [key, value] : serializedOperator.operator_().config())
        {
            config[key] = NES::Configurations::protoToDescriptorConfigType(value);
        }
        auto registryArgument = NES::LogicalOperatorRegistryArguments(config);

        if (auto logicalOperator = LogicalOperatorRegistry::instance().create(serializedOperator.operator_().operator_type(), registryArgument);
            logicalOperator.has_value())
        {
            return logicalOperator.value();
        }
        throw CannotDeserialize("could not de-serialize this serialized operator: {}", serializedOperator.DebugString());
    }
    throw CannotDeserialize("could not de-serialize this serialized operator: {}", serializedOperator.DebugString());
}

LogicalOperator
OperatorSerializationUtil::deserializeSourceOperator(const SerializableOperator_SourceDescriptorLogicalOperator& serializedSource)
{
    auto sourceDescriptor = deserializeSourceDescriptor(serializedSource.sourcedescriptor());
    auto logicalOperator = SourceDescriptorLogicalOperator(std::move(sourceDescriptor));
    return logicalOperator.withInputOriginIds({{OriginId(serializedSource.sourceoriginid())}});
}

LogicalOperator OperatorSerializationUtil::deserializeSinkOperator(const SerializableOperator_SinkLogicalOperator& sinkDetails)
{
    const auto& serializedSinkDescriptor = sinkDetails.sinkdescriptor();
    auto sinkDescriptor = deserializeSinkDescriptor(serializedSinkDescriptor);
    auto sinkOperator = SinkLogicalOperator();
    sinkOperator.sinkDescriptor = std::move(sinkDescriptor);
    return sinkOperator;
}

std::unique_ptr<Sources::SourceDescriptor> OperatorSerializationUtil::deserializeSourceDescriptor(
    const SerializableSourceDescriptor& sourceDescriptor)
{
    /// Declaring variables outside of SourceDescriptor for readability/debuggability.
    auto schema = SchemaSerializationUtil::deserializeSchema(sourceDescriptor.sourceschema());
    auto logicalSourceName = sourceDescriptor.logicalsourcename();
    auto sourceType = sourceDescriptor.sourcetype();

    /// Deserialize the parser config.
    const auto& serializedParserConfig = sourceDescriptor.parserconfig();
    auto deserializedParserConfig = Sources::ParserConfig{};
    deserializedParserConfig.parserType = serializedParserConfig.type();
    deserializedParserConfig.tupleDelimiter = serializedParserConfig.tupledelimiter();
    deserializedParserConfig.fieldDelimiter = serializedParserConfig.fielddelimiter();

    /// Deserialize SourceDescriptor config. Convert from protobuf variant to SourceDescriptor::ConfigType.
    NES::Configurations::DescriptorConfig::Config SourceDescriptorConfig{};
    for (const auto& [key, value] : sourceDescriptor.config())
    {
        SourceDescriptorConfig[key] = Configurations::protoToDescriptorConfigType(value);
    }

    return std::make_unique<Sources::SourceDescriptor>(
        std::move(schema),
        std::move(logicalSourceName),
        std::move(sourceType),
        std::move(deserializedParserConfig),
        std::move(SourceDescriptorConfig));
}

std::unique_ptr<Sinks::SinkDescriptor> OperatorSerializationUtil::deserializeSinkDescriptor(
    const SerializableSinkDescriptor& serializableSinkDescriptor)
{
    /// Declaring variables outside of DescriptorSource for readability/debuggability.
    auto schema = SchemaSerializationUtil::deserializeSchema(serializableSinkDescriptor.sinkschema());
    auto addTimestamp = serializableSinkDescriptor.addtimestamp();
    auto sinkType = serializableSinkDescriptor.sinktype();

    /// Deserialize DescriptorSource config. Convert from protobuf variant to DescriptorSource::ConfigType.
    NES::Configurations::DescriptorConfig::Config sinkDescriptorConfig{};
    for (const auto& kv : serializableSinkDescriptor.config())
    {
        sinkDescriptorConfig[kv.first] = Configurations::protoToDescriptorConfigType(kv.second);
    }

    auto sinkDescriptor
        = std::make_unique<Sinks::SinkDescriptor>(std::move(sinkType), std::move(sinkDescriptorConfig), std::move(addTimestamp));
    sinkDescriptor->schema = schema;
    return sinkDescriptor;
}


}
