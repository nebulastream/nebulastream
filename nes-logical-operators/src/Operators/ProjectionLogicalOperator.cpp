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

#include <Operators/ProjectionLogicalOperator.hpp>

#include <cstddef>
#include <numeric>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

namespace
{
std::string explainProjection(const ProjectionLogicalOperator::Projection& projection, ExplainVerbosity verbosity)
{
    std::stringstream builder;
    builder << projection.second.explain(verbosity);
    if (projection.first)
    {
        builder << " as " << projection.first->getFieldName();
    }
    return builder.str();
}
}

ProjectionLogicalOperator::ProjectionLogicalOperator(std::vector<Projection> projections, Asterisk asterisk)
    : projections(std::move(projections)), asterisk(asterisk.value)
{
}

std::string_view ProjectionLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::vector<std::string> ProjectionLogicalOperator::getAccessedFields() const
{
    if (asterisk)
    {
        return inputSchema.getFieldNames();
    }

    return projections | std::views::values
        | std::views::transform(
               [](const LogicalFunction& function)
               {
                   return BFSRange(function)
                       | std::views::filter([](const LogicalFunction& function)
                                            { return function.tryGet<FieldAccessLogicalFunction>().has_value(); })
                       | std::views::transform([](const LogicalFunction& function)
                                               { return function.get<FieldAccessLogicalFunction>().getFieldName(); });
               })
        | std::views::join | std::ranges::to<std::vector>();
}

const std::vector<ProjectionLogicalOperator::Projection>& ProjectionLogicalOperator::getProjections() const
{
    return projections;
}

bool ProjectionLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto* const rhsOperator = dynamic_cast<const ProjectionLogicalOperator*>(&rhs))
    {
        return projections == rhsOperator->projections && getOutputSchema() == rhsOperator->getOutputSchema()
            && getInputSchemas() == rhsOperator->getInputSchemas() && getInputOriginIds() == rhsOperator->getInputOriginIds()
            && getOutputOriginIds() == rhsOperator->getOutputOriginIds() && getTraitSet() == rhsOperator->getTraitSet();
    }
    return false;
};

std::string ProjectionLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    auto explainedProjections
        = std::views::transform(projections, [&](const auto& projection) { return explainProjection(projection, verbosity); });
    std::stringstream builder;
    builder << "PROJECTION(";
    if (verbosity == ExplainVerbosity::Debug)
    {
        builder << "opId: " << id << ", ";
    }
    if (!outputSchema.getFieldNames().empty())
    {
        builder << "schema: " << outputSchema << ", ";
    }

    builder << "fields: [";
    if (asterisk)
    {
        builder << "*";
        if (!projections.empty())
        {
            builder << ", ";
        }
    }
    builder << fmt::format("{}", fmt::join(explainedProjections, ", "));
    builder << "])";
    return builder.str();
}

LogicalOperator ProjectionLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    INVARIANT(!inputSchemas.empty(), "Projection should have at least one input");

    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for Projection operator");
        }
    }

    /// Propagate the type inference to all projection functions and resolve projection names.
    auto inferredProjections = projections
        | std::views::transform(
                                   [&firstSchema](const Projection& projection)
                                   {
                                       auto inferredFunction = projection.second.withInferredDataType(firstSchema);
                                       /// If projection has a name use it.
                                       if (projection.first)
                                       {
                                           return Projection{*projection.first, inferredFunction};
                                       }
                                       /// Otherwise derive the name from the inferred function
                                       return Projection{inferredFunction.explain(ExplainVerbosity::Short), inferredFunction};
                                   });

    auto copy = *this;
    copy.projections = inferredProjections | std::ranges::to<std::vector>();
    copy.inputSchema = firstSchema;

    /// Resolve the output schema of the Projection. If an asterisk is used we propagate the entire input schema
    auto initial = Schema{copy.outputSchema.memoryLayoutType};
    if (asterisk)
    {
        initial.appendFieldsFromOtherSchema(copy.inputSchema);
    }
    copy.outputSchema = std::accumulate(
        inferredProjections.begin(),
        inferredProjections.end(),
        initial,
        [](Schema schema, const auto& projection)
        { return schema.addField(projection.first->getFieldName(), projection.second.getDataType()); });

    return copy;
}

TraitSet ProjectionLogicalOperator::getTraitSet() const
{
    return traitSet;
}

LogicalOperator ProjectionLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

LogicalOperator ProjectionLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> ProjectionLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema ProjectionLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> ProjectionLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> ProjectionLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator ProjectionLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Projection should have only one input");
    auto copy = *this;
    copy.inputOriginIds = ids.at(0);
    return copy;
}

LogicalOperator ProjectionLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> ProjectionLogicalOperator::getChildren() const
{
    return children;
}

void ProjectionLogicalOperator::serialize(SerializableOperator& serializableOperator) const
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

    serializableOperator.set_operator_id(id.getRawValue());
    for (const auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    ProjectionList projList;
    for (const auto& [name, fn] : getProjections())
    {
        auto& proj = *projList.add_projections();
        if (name)
        {
            proj.set_identifier(name->getFieldName());
        }
        proj.mutable_function()->CopyFrom(fn.serialize());
    }
    (*serializableOperator.mutable_config())[ConfigParameters::PROJECTION_FUNCTION_NAME] = descriptorConfigTypeToProto(projList);
    (*serializableOperator.mutable_config())[ConfigParameters::ASTERISK] = descriptorConfigTypeToProto(asterisk);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterProjectionLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    const auto functionVariant = arguments.config[ProjectionLogicalOperator::ConfigParameters::PROJECTION_FUNCTION_NAME];
    const auto asterisk = std::get<bool>(arguments.config[ProjectionLogicalOperator::ConfigParameters::ASTERISK]);

    if (const auto* projection = std::get_if<ProjectionList>(&functionVariant))
    {
        auto logicalOperator = ProjectionLogicalOperator(
            projection->projections()
                | std::views::transform(
                    [](const auto& serialized)
                    {
                        return ProjectionLogicalOperator::Projection{
                            FieldIdentifier(serialized.identifier()),
                            FunctionSerializationUtil::deserializeFunction(serialized.function())};
                    })
                | std::ranges::to<std::vector>(),
            ProjectionLogicalOperator::Asterisk(asterisk));

        if (auto& id = arguments.id)
        {
            logicalOperator.id = *id;
        }

        return logicalOperator.withInferredSchema(arguments.inputSchemas)
            .withInputOriginIds(arguments.inputOriginIds)
            .withOutputOriginIds(arguments.outputOriginIds);
    }
    throw UnknownLogicalOperator();
}

}
