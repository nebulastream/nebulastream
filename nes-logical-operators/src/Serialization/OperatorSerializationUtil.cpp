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
#include <Serialization/OperatorSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <google/protobuf/json/json.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/Operator.hpp>

namespace NES
{

SerializableOperator OperatorSerializationUtil::serializeOperator(Operator operatorNode)
{
    auto serializedOperator = SerializableOperator();

    if (auto* sourceOp = operatorNode.get<SourceDescriptorLogicalOperator>())
    {
        // Serialize source operator
        serializeSourceOperator(*sourceOp, serializedOperator);
    }
    else if (auto* sinkOp = operatorNode.get<SinkLogicalOperator>())
    {
        // Serialize sink operator
        serializeSinkOperator(*sinkOp, serializedOperator);
    }
    return serializedOperator;
}

Operator OperatorSerializationUtil::deserializeOperator(SerializableOperator serializedOperator)
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
        if (auto logicalOperator = LogicalOperatorRegistry::instance().create(operator_.operatortype(), operatorArguments); logicalOperator.has_value())
        {
            return std::move(logicalOperator.value());
        }
    }
    throw CannotDeserialize("could not de-serialize this serialized operator: {}", serializedOperator.DebugString());
}

void OperatorSerializationUtil::serializeSourceOperator(
    const SourceDescriptorLogicalOperator& sourceOperator, SerializableOperator& serializedOperator)
{
    auto* sourceDetails = new SerializableOperator_SourceDescriptorLogicalOperator();
    const auto& sourceDescriptor = sourceOperator.getSourceDescriptor();
    serializeSourceDescriptor(sourceDescriptor, *sourceDetails);
    sourceDetails->set_sourceoriginid(sourceOperator.originIdTrait.originIds[0].getRawValue());

    serializedOperator.set_allocated_source(sourceDetails);
}

Operator
OperatorSerializationUtil::deserializeSourceOperator(const SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails)
{
    const auto& serializedSourceDescriptor = sourceDetails.sourcedescriptor();
    auto sourceDescriptor = deserializeSourceDescriptor(serializedSourceDescriptor);
    auto logicalOperator = SourceDescriptorLogicalOperator(sourceDescriptor);
    logicalOperator.originIdTrait.originIds = {OriginId(sourceDetails.sourceoriginid())};
    return logicalOperator;
}


void OperatorSerializationUtil::serializeSinkOperator(
    const SinkLogicalOperator& sinkOperator, SerializableOperator& serializedOperator)
{
    auto* sinkDetails = new SerializableOperator_SinkLogicalOperator();
    const auto& sinkDescriptor = sinkOperator.sinkDescriptor;
    serializeSinkDescriptor(sinkOperator.getOutputSchema(), sinkDescriptor, *sinkDetails);

    serializedOperator.set_allocated_sink(sinkDetails);
}

Operator OperatorSerializationUtil::deserializeSinkOperator(const SerializableOperator_SinkLogicalOperator& sinkDetails)
{
    const auto& serializedSinkDescriptor = sinkDetails.sinkdescriptor();
    auto sinkOperator = SinkLogicalOperator();
    sinkOperator.sinkDescriptor = deserializeSinkDescriptor(serializedSinkDescriptor);
    return sinkOperator;
}

void OperatorSerializationUtil::serializeSourceDescriptor(
    const Sources::SourceDescriptor& sourceDescriptor, SerializableOperator_SourceDescriptorLogicalOperator& sourceDetails)
{
    const auto serializedSourceDescriptor
        = SerializableOperator_SourceDescriptorLogicalOperator_SourceDescriptor().New(); /// cleaned up by protobuf

    SchemaSerializationUtil::serializeSchema(sourceDescriptor.schema, serializedSourceDescriptor->mutable_sourceschema());
    serializedSourceDescriptor->set_logicalsourcename(sourceDescriptor.logicalSourceName);
    serializedSourceDescriptor->set_sourcetype(sourceDescriptor.sourceType);

    /// Serialize parser config.
    auto* const serializedParserConfig = ParserConfig().New();
    serializedParserConfig->set_type(sourceDescriptor.parserConfig.parserType);
    serializedParserConfig->set_tupledelimiter(sourceDescriptor.parserConfig.tupleDelimiter);
    serializedParserConfig->set_fielddelimiter(sourceDescriptor.parserConfig.fieldDelimiter);
    serializedSourceDescriptor->set_allocated_parserconfig(serializedParserConfig);

    /// Iterate over SourceDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : sourceDescriptor.config)
    {
        auto* kv = serializedSourceDescriptor->mutable_config();
        kv->emplace(key, descriptorConfigTypeToProto(value));
    }
    sourceDetails.set_allocated_sourcedescriptor(serializedSourceDescriptor);
}

Operator OperatorSerializationUtil::deserializeLogicalOperator(
    const SerializableOperator_LogicalOperator& operatorDescriptor)
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

Sources::SourceDescriptor OperatorSerializationUtil::deserializeSourceDescriptor(
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

    return Sources::SourceDescriptor(
        std::move(schema),
        std::move(logicalSourceName),
        std::move(sourceType),
        std::move(deserializedParserConfig),
        std::move(SourceDescriptorConfig));
}

void OperatorSerializationUtil::serializeSinkDescriptor(
    const Schema& schema, const Sinks::SinkDescriptor& sinkDescriptor, SerializableOperator_SinkLogicalOperator& sinkDetails)
{
    const auto serializedSinkDescriptor
        = SerializableOperator_SinkLogicalOperator_SerializableSinkDescriptor().New(); /// cleaned up by protobuf
    SchemaSerializationUtil::serializeSchema(schema, serializedSinkDescriptor->mutable_sinkschema());
    serializedSinkDescriptor->set_sinktype(sinkDescriptor.sinkType);
    serializedSinkDescriptor->set_addtimestamp(sinkDescriptor.addTimestamp);
    /// Iterate over SinkDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : sinkDescriptor.config)
    {
        auto* kv = serializedSinkDescriptor->mutable_config();
        kv->emplace(key, descriptorConfigTypeToProto(value));
    }
    sinkDetails.set_allocated_sinkdescriptor(serializedSinkDescriptor);
}

Sinks::SinkDescriptor OperatorSerializationUtil::deserializeSinkDescriptor(
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

    auto sinkDescriptor = Sinks::SinkDescriptor(std::move(sinkType), std::move(sinkDescriptorConfig), std::move(addTimestamp));
    sinkDescriptor.schema = schema;
    return sinkDescriptor;
}

}
