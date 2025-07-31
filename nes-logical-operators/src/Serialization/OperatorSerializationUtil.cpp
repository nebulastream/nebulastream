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
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include "Serialization/IdentifierSerializationUtil.hpp"
#include "Serialization/UnboundSchemaSerializationUtl.hpp"
#include "Util/Overloaded.hpp"

namespace NES
{


LogicalOperator
OperatorSerializationUtil::deserializeOperator(const SerializableOperator& serializedOperator, std::vector<LogicalOperator> children)
{
    std::optional<LogicalOperator> result = [&]() -> std::optional<LogicalOperator>
    {
        if (serializedOperator.has_source())
        {
            const auto& serializedSource = serializedOperator.source();
            auto sourceDescriptor = deserializeSourceDescriptor(serializedSource.sourcedescriptor());
            auto sourceOperator = TypedLogicalOperator<SourceDescriptorLogicalOperator>{std::move(sourceDescriptor)};
            return sourceOperator;
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
            auto sinkNameVariant = config.at(SinkLogicalOperator::ConfigParameters::SINK_NAME);
            if (not std::holds_alternative<Identifier>(sinkNameVariant))
            {
                throw CannotDeserialize(
                    "Expected identifier list for sinkName but got {} while deserializing\n{}",
                    sinkNameVariant,
                    serializedOperator.DebugString());
            }
            auto sinkName = std::get<Identifier>(sinkNameVariant);

            auto sinkOperator = TypedLogicalOperator<SinkLogicalOperator>{sinkName}.withChildren(std::move(children));
            serializedSinkDescriptor.transform(
                [&sinkOperator](const auto& serialized)
                { return sinkOperator = sinkOperator->withSinkDescriptor(deserializeSinkDescriptor(serialized)); });

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
                .children = std::move(children), /// children - will be populated from operator_().children
                .config = config};
            return LogicalOperatorRegistry::instance().create(serializedOperator.operator_().operator_type(), registryArgument);
        }
        return std::nullopt;
    }();

    if (result.has_value())
    {
        TraitSet traitSet = TraitSetSerializationUtil::deserialize(&serializedOperator.trait_set(), result.value());
        return result->withTraitSet(std::move(traitSet)).withOperatorId(OperatorId{serializedOperator.operator_id()});
    }

    throw CannotDeserialize("could not de-serialize this serialized operator:\n{}", serializedOperator.DebugString());
}

SourceDescriptor OperatorSerializationUtil::deserializeSourceDescriptor(const SerializableSourceDescriptor& sourceDescriptor)
{
    auto schema = UnboundSchemaSerializationUtil::deserializeUnboundSchema(sourceDescriptor.sourceschema());
    const LogicalSource logicalSource{IdentifierSerializationUtil::deserializeIdentifier(sourceDescriptor.logicalsourcename()), schema};

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
    auto sinkName = [&] -> std::variant<Identifier, uint64_t>
    {
        auto deserializedSinkName = IdentifierSerializationUtil::deserializeIdentifier(serializableSinkDescriptor.sinkname());
        if (std::ranges::all_of(deserializedSinkName.asCanonicalString(), [](const char character) { return std::isdigit(character); }))
        {
            return std::stoull(deserializedSinkName.asCanonicalString());
        }
        return deserializedSinkName;
    }();

    const auto schema = UnboundSchemaSerializationUtil::deserializeUnboundSchema(serializableSinkDescriptor.sinkschema());
    auto sinkType = serializableSinkDescriptor.sinktype();

    /// Deserialize DescriptorSource config. Convert from protobuf variant to DescriptorSource::ConfigType.
    DescriptorConfig::Config sinkDescriptorConfig{};
    for (const auto& [key, descriptor] : serializableSinkDescriptor.config())
    {
        sinkDescriptorConfig[key] = protoToDescriptorConfigType(descriptor);
    }
    const auto sinkDescriptorVariant = std::visit(
        Overloaded{
            [&](Identifier name)
            {
                return std::variant<NamedSinkDescriptor, InlineSinkDescriptor>{
                    NamedSinkDescriptor{std::move(name), std::move(schema), sinkType, sinkDescriptorConfig}};
            },
            [&](const uint64_t id)
            {
                return std::variant<NamedSinkDescriptor, InlineSinkDescriptor>{
                    InlineSinkDescriptor{id, std::move(schema), sinkType, sinkDescriptorConfig}};
            }},
        std::move(sinkName));

    return SinkDescriptor{std::move(sinkDescriptorVariant)};
}


}
