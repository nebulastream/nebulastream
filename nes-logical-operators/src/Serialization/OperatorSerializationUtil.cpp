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

#include <vector>
#include <Operators/UnionLogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/OperatorSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>

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
        const auto& operator_ = serializedOperator.operator_();
        auto operatorArguments = NES::LogicalOperatorRegistryArguments();
        if (auto logicalOperator = LogicalOperatorRegistry::instance().create(operator_.operatortype(), operatorArguments);
            logicalOperator.has_value())
        {
            return std::move(logicalOperator.value());
        }
    }
    throw CannotDeserialize("could not de-serialize this serialized operator: {}", serializedOperator.DebugString());
}

LogicalOperator
OperatorSerializationUtil::deserializeSourceOperator(const SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails)
{
    const auto& serializedSourceDescriptor = sourceDetails.sourcedescriptor();
    auto sourceDescriptor = deserializeSourceDescriptor(serializedSourceDescriptor);
    auto logicalOperator = SourceDescriptorLogicalOperator(std::move(sourceDescriptor));
    return logicalOperator.withInputOriginIds({{OriginId(sourceDetails.sourceoriginid())}});
}

LogicalOperator OperatorSerializationUtil::deserializeSinkOperator(const SerializableOperator_SinkLogicalOperator& sinkDetails)
{
    const auto& serializedSinkDescriptor = sinkDetails.sinkdescriptor();
    auto sinkDescriptor = deserializeSinkDescriptor(serializedSinkDescriptor);
    auto sinkOperator = SinkLogicalOperator();
    sinkOperator.sinkDescriptor = std::move(sinkDescriptor);
    return sinkOperator;
}

LogicalOperator OperatorSerializationUtil::deserializeLogicalOperator(const SerializableOperator_LogicalOperator& operatorDescriptor)
{
    auto operatorType = operatorDescriptor.operatortype();

    /// Deserialize config. Convert from protobuf variant to ConfigType.
    NES::Configurations::DescriptorConfig::Config operatorDescriptorConfig{};
    for (const auto& [key, value] : operatorDescriptor.config())
    {
        operatorDescriptorConfig[key] = NES::Configurations::protoToDescriptorConfigType(value);
    }

    auto operatorArguments = LogicalOperatorRegistryArguments(operatorDescriptorConfig);
    if (auto logicalOperator = LogicalOperatorRegistry::instance().create(operatorType, operatorArguments))
    {
        return std::move(logicalOperator.value());
    }
    throw UnknownLogicalOperator();
}

std::unique_ptr<Sources::SourceDescriptor> OperatorSerializationUtil::deserializeSourceDescriptor(
    const SerializableOperator_SourceDescriptorLogicalOperator_SourceDescriptor& sourceDescriptor)
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
    const SerializableOperator_SinkLogicalOperator_SerializableSinkDescriptor& serializableSinkDescriptor)
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
