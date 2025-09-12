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

#include <Serialization/OperatorSerializationUtil.hpp>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{


LogicalOperator OperatorSerializationUtil::deserializeOperator(const SerializableOperator& serializedOperator)
{
    if (serializedOperator.has_source())
    {
        const auto& serializedSource = serializedOperator.source();
        auto sourceDescriptor = deserializeSourceDescriptor(serializedSource.sourcedescriptor());
        auto sourceOperator = SourceDescriptorLogicalOperator(std::move(sourceDescriptor));
        sourceOperator.id = OperatorId(serializedOperator.operator_id());
        return sourceOperator.withOutputOriginIds({{OriginId(serializedSource.sourceoriginid())}});
    }

    if (serializedOperator.has_sink())
    {
        const auto& sink = serializedOperator.sink();
        if (not sink.has_sinkdescriptor())
        {
            throw CannotDeserialize("Sink is missing sinkdescription:\n{}", serializedOperator.DebugString());
        }
        const auto serializedSinkDescriptor = sink.has_sinkdescriptor() ? std::make_optional(sink.sinkdescriptor()) : std::nullopt;
        DescriptorConfig::Config config;
        for (const auto& [key, value] : serializedOperator.config())
        {
            config[key] = protoToDescriptorConfigType(value);
        }
        auto sinkName = config.at(SinkLogicalOperator::ConfigParameters::SINK_NAME);
        if (not std::holds_alternative<std::string>(sinkName))
        {
            throw CannotDeserialize(
                "Expected string for sinkName but got {} while deserializing\n{}", sinkName, serializedOperator.DebugString());
        }

        auto sinkOperator = SinkLogicalOperator();
        sinkOperator.id = OperatorId(serializedOperator.operator_id());
        sinkOperator.sinkName = std::get<std::string>(sinkName);

        std::vector<std::vector<OriginId>> inputIdsVec;
        for (const auto& originList : sink.input_origin_lists())
        {
            std::vector<OriginId> ids;
            for (const auto& id : originList.origin_ids())
            {
                ids.emplace_back(id);
            }
            inputIdsVec.emplace_back(std::move(ids));
        }
        sinkOperator = sinkOperator.withInputOriginIds(inputIdsVec).get<SinkLogicalOperator>();

        std::vector<OriginId> outputIds;
        for (const auto& id : sink.output_origin_ids())
        {
            outputIds.emplace_back(id);
        }
        sinkOperator = sinkOperator.withOutputOriginIds(outputIds).get<SinkLogicalOperator>();

        sinkOperator.sinkDescriptor
            = serializedSinkDescriptor.transform([](const auto& serialized) { return deserializeSinkDescriptor(serialized); });

        if (sinkOperator.getSinkDescriptor()->getSinkType() == "File" && sinkOperator.sinkDescriptor->getSchema()->getNumberOfFields() == 0)
        {
            throw CannotDeserialize("Sink must not have empty schema!");
        }

        return sinkOperator;
    }

    if (serializedOperator.has_operator_())
    {
        DescriptorConfig::Config config;
        for (const auto& [key, value] : serializedOperator.config())
        {
            config[key] = protoToDescriptorConfigType(value);
        }

        auto registryArgument = LogicalOperatorRegistryArguments{
            .id = OperatorId(serializedOperator.operator_id()),
            .inputOriginIds = {}, /// inputOriginIds - will be populated from operator_().input_origin_lists
            .outputOriginIds = {}, /// outputOriginIds - will be populated from operator_().output_origin_ids
            .inputSchemas = {}, /// inputSchemas - will be populated from operator_().input_schema
            .outputSchema = Schema(), /// outputSchema - will be populated from operator_().output_schema
            .config = config};

        for (const auto& originList : serializedOperator.operator_().input_origin_lists())
        {
            std::vector<OriginId> ids;
            for (const auto& id : originList.origin_ids())
            {
                ids.emplace_back(id);
            }
            registryArgument.inputOriginIds.push_back(ids);
        }

        std::vector<OriginId> outputIds;
        for (const auto& id : serializedOperator.operator_().output_origin_ids())
        {
            outputIds.emplace_back(id);
        }
        registryArgument.outputOriginIds = outputIds;

        for (const auto& schema : serializedOperator.operator_().input_schemas())
        {
            registryArgument.inputSchemas.push_back(SchemaSerializationUtil::deserializeSchema(schema));
        }

        if (serializedOperator.operator_().has_output_schema())
        {
            registryArgument.outputSchema = SchemaSerializationUtil::deserializeSchema(serializedOperator.operator_().output_schema());
        }

        if (auto logicalOperator
            = LogicalOperatorRegistry::instance().create(serializedOperator.operator_().operator_type(), registryArgument);
            logicalOperator.has_value())
        {
            return logicalOperator.value();
        }
    }

    throw CannotDeserialize("could not de-serialize this serialized operator:\n{}", serializedOperator.DebugString());
}

SourceDescriptor OperatorSerializationUtil::deserializeSourceDescriptor(const SerializableSourceDescriptor& sourceDescriptor)
{
    auto schema = SchemaSerializationUtil::deserializeSchema(sourceDescriptor.sourceschema());
    const LogicalSource logicalSource{sourceDescriptor.logicalsourcename(), schema};

    /// TODO #815 the serializer would also a catalog to register/create source descriptors/logical sources
    const auto physicalSourceId = PhysicalSourceId{sourceDescriptor.physicalsourceid()};
    const auto& sourceType = sourceDescriptor.sourcetype();

    /// Deserialize the parser config.
    const auto& serializedParserConfig = sourceDescriptor.parserconfig();
    auto deserializedParserConfig = ParserConfig{};
    deserializedParserConfig.parserType = serializedParserConfig.type();
    deserializedParserConfig.tupleDelimiter = serializedParserConfig.tupledelimiter();
    deserializedParserConfig.fieldDelimiter = serializedParserConfig.fielddelimiter();

    /// Deserialize SourceDescriptor config. Convert from protobuf variant to SourceDescriptor::ConfigType.
    DescriptorConfig::Config sourceDescriptorConfig{};
    for (const auto& [key, value] : sourceDescriptor.config())
    {
        sourceDescriptorConfig[key] = protoToDescriptorConfigType(value);
    }

    return SourceDescriptor{physicalSourceId, logicalSource, sourceType, std::move(sourceDescriptorConfig), deserializedParserConfig};
}

SinkDescriptor OperatorSerializationUtil::deserializeSinkDescriptor(const SerializableSinkDescriptor& serializableSinkDescriptor)
{
    /// Declaring variables outside of DescriptorSource for readability/debuggability.
    auto sinkName = serializableSinkDescriptor.sinkname();
    const auto schema = SchemaSerializationUtil::deserializeSchema(serializableSinkDescriptor.sinkschema());
    auto sinkType = serializableSinkDescriptor.sinktype();

    /// Deserialize DescriptorSource config. Convert from protobuf variant to DescriptorSource::ConfigType.
    DescriptorConfig::Config sinkDescriptorConfig{};
    for (const auto& [key, descriptor] : serializableSinkDescriptor.config())
    {
        sinkDescriptorConfig[key] = protoToDescriptorConfigType(descriptor);
    }

    return SinkDescriptor{std::move(sinkName), schema, std::move(sinkType), std::move(sinkDescriptorConfig)};
}


}
