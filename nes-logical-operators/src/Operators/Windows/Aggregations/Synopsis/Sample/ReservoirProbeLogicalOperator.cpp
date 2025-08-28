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

#include <Operators/Windows/Aggregations/Synopsis/Sample/ReservoirProbeLogicalOperator.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

ReservoirProbeLogicalOperator::ReservoirProbeLogicalOperator(FieldAccessLogicalFunction asField): asField(asField)
{
}

std::string_view ReservoirProbeLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool ReservoirProbeLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    const auto* other = dynamic_cast<const ReservoirProbeLogicalOperator*>(&rhs);
    return other != nullptr;
};

LogicalOperator ReservoirProbeLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    INVARIANT(inputSchemas.size() == 1, "ReservoirProbe should have one input but got {}", inputSchemas.size());

    auto copy = *this;

    copy.inputSchema = inputSchemas[0];
    copy.outputSchema = inputSchemas[0];

    return copy;
}

TraitSet ReservoirProbeLogicalOperator::getTraitSet() const
{
    return {};
}

LogicalOperator ReservoirProbeLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> ReservoirProbeLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema ReservoirProbeLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> ReservoirProbeLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> ReservoirProbeLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator ReservoirProbeLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "ReservoirProbe should have only one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator ReservoirProbeLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> ReservoirProbeLogicalOperator::getChildren() const
{
    return children;
}

std::string ReservoirProbeLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("RESERVOIR_PROBE(opId: {})", id);
    }
    return fmt::format("RESERVOIR_PROBE");
}

SerializableOperator ReservoirProbeLogicalOperator::serialize() const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);
    auto* traitSetProto = proto.mutable_trait_set();
    for (const auto& trait : getTraitSet())
    {
        *traitSetProto->add_traits() = trait.serialize();
    }

    const auto inputs = getInputSchemas();
    const auto originLists = getInputOriginIds();
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        auto* inSch = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputs[i], inSch);

        auto* olist = proto.add_input_origin_lists();
        for (auto originId : originLists[i])
        {
            olist->add_origin_ids(originId.getRawValue());
        }
    }

    for (auto outId : getOutputOriginIds())
    {
        proto.add_output_origin_ids(outId.getRawValue());
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    SerializableOperator serializableOperator;
    serializableOperator.set_operator_id(id.getRawValue());
    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

// NES::Configurations::DescriptorConfig::Config ReservoirProbeLogicalOperator::validateAndFormat(std::unordered_map<std::string, std::string> config)
// {
//     return NES::Configurations::DescriptorConfig::validateAndFormat<ConfigParameters>(std::move(config), NAME);
// }

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterReservoirProbeLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    /// TODO remove:
    (void)arguments;
    // auto functionVariant = arguments.config[ReservoirSampleLogicalOperator::ConfigParameters::MAP_FUNCTION_NAME];
    // if (std::holds_alternative<NES::FunctionList>(functionVariant))
    // {
    //     const auto functions = std::get<FunctionList>(functionVariant).functions();
    //
    //     INVARIANT(functions.size() == 1, "Expected exactly one function");
    //     auto function = FunctionSerializationUtil::deserializeFunction(functions[0]);
    //     INVARIANT(
    //         function.tryGet<FieldAssignmentLogicalFunction>(),
    //         "Expected a field assignment function, got: {}",
    //         function.explain(ExplainVerbosity::Debug));
    //
    //     auto logicalOperator = MapLogicalOperator(function.get<FieldAssignmentLogicalFunction>());
    //     if (auto& id = arguments.id)
    //     {
    //         logicalOperator.id = *id;
    //     }
    //     return logicalOperator.withInferredSchema(arguments.inputSchemas)
    //         .withInputOriginIds(arguments.inputOriginIds)
    //         .withOutputOriginIds(arguments.outputOriginIds);
    // }
    throw UnknownLogicalOperator();
}

}
