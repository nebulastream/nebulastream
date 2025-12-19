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

#include <algorithm>
#include <cctype>
#include <cstdint>
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
#include <Serialization/TraitSetSerializationUtil.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/TraitSet.hpp>
#include <rfl/json/read.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <rfl.hpp>
#include "Serialization/SerializedUtils.hpp"

namespace NES
{


LogicalOperator OperatorSerializationUtil::deserializeOperator(const SerializableOperator& serializedOperator)
{
    std::optional<LogicalOperator> result = [&] -> std::optional<LogicalOperator>
    {
        auto serializedOpt = rfl::json::read<SerializedOperator>(serializedOperator.reflect());
        if (serializedOpt.has_value() && serializedOpt.value().type == "Source")
        {
            auto serialized = std::move(serializedOpt.value());
            auto serializedSourceDescriptor = rfl::from_generic<SerializedSourceDescriptor>(serialized.config).value();
            auto sourceDescriptor = SerializedUtils::deserializeSourceDescriptor(serializedSourceDescriptor);
            auto sourceOperator = SourceDescriptorLogicalOperator(std::move(sourceDescriptor));
            return sourceOperator;

        }
        if (serializedOpt.has_value() && serializedOpt.value().type == "Equal")
        {
            auto serialized = std::move(serializedOpt.value());
            auto registryArgument = LogicalOperatorRegistryArguments{
                .inputSchemas = {},
                .outputSchema = Schema(),
                .config = {},
                .reflec = ""};


            if (!serializedOperator.reflect().empty())
            {
                registryArgument.reflec = serializedOperator.reflect();
            }

            registryArgument.inputSchemas = SerializedUtils::deserializeSchemas(serialized.inputSchemas);
            if (serialized.outputSchema.has_value())
            {
                registryArgument.outputSchema = SerializedUtils::deserializeSchema(serialized.outputSchema.value());
            }

            return LogicalOperatorRegistry::instance().create(serializedOperator.operator_().operator_type(), registryArgument);
        }

        if (serializedOperator.has_sink())
        {
            const auto& sink = serializedOperator.sink();
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
            sinkOperator.sinkName = std::get<std::string>(sinkName);
            sinkOperator.sinkDescriptor
                = serializedSinkDescriptor.transform([](const auto& serialized) { return deserializeSinkDescriptor(serialized); });

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
                .inputSchemas = {}, /// inputSchemas - will be populated from operator_().input_schema
                .outputSchema = Schema(), /// outputSchema - will be populated from operator_().output_schema
                .config = config,
                .reflec = ""};

            if (!serializedOperator.reflect().empty())
            {
                registryArgument.reflec = serializedOperator.reflect();
            }


            for (const auto& schema : serializedOperator.operator_().input_schemas())
            {
                registryArgument.inputSchemas.push_back(SchemaSerializationUtil::deserializeSchema(schema));
            }

            if (serializedOperator.operator_().has_output_schema())
            {
                registryArgument.outputSchema = SchemaSerializationUtil::deserializeSchema(serializedOperator.operator_().output_schema());
            }
            return LogicalOperatorRegistry::instance().create(serializedOperator.operator_().operator_type(), registryArgument);
        }
        return std::nullopt;
    }();

    if (result.has_value())
    {
        TraitSet traitSet = TraitSetSerializationUtil::deserialize(&serializedOperator.trait_set());
        return result->withTraitSet(std::move(traitSet)).withOperatorId(OperatorId{serializedOperator.operator_id()});
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
    std::variant<std::string, uint64_t> sinkName;

    if (std::ranges::all_of(serializableSinkDescriptor.sinkname(), [](const char character) { return std::isdigit(character); }))
    {
        sinkName = std::stoull(serializableSinkDescriptor.sinkname());
    }
    else
    {
        sinkName = serializableSinkDescriptor.sinkname();
    }

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
