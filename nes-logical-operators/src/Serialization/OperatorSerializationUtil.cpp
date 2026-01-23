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

namespace NES
{


LogicalOperator OperatorSerializationUtil::deserializeOperator(const ReflectedOperator& serialized)
{

    std::optional<LogicalOperator> const result = [&] -> std::optional<LogicalOperator>
    {

        if (serialized.type == "Source")
        {
            return unreflect<SourceDescriptorLogicalOperator>(serialized.config);
        }

        if (serialized.type == "Sink")
        {
            return unreflect<SinkLogicalOperator>(serialized.config);
        }

        auto registryArgument
            = LogicalOperatorRegistryArguments{.inputSchemas = {}, .outputSchema = Schema(), .config = {}, .configNew = serialized.config};

        auto logicalOperatorOpt = LogicalOperatorRegistry::instance().create(serialized.type, registryArgument);

        if (!logicalOperatorOpt.has_value())
        {
            return std::nullopt;
        }

        auto logicalOperator = logicalOperatorOpt.value();

        logicalOperator = logicalOperator
            .withInferredSchema(serialized.inputSchemas);

        return std::make_optional(logicalOperator);
    }();

    if (result.has_value())
    {
        return result.value();
    }

    // throw CannotDeserialize("could not de-serialize this serialized operator:\n{}", serializedOperator.DebugString());
    throw CannotDeserialize("could not de-serialize this serialized operator {}", serialized.operatorId);
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
