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
#include "Schema/Field.hpp"
#include "Serialization/IdentifierSerializationUtil.hpp"

namespace NES
{

namespace
{
std::string explainProjection(const ProjectionLogicalOperator::Projection& projection, ExplainVerbosity verbosity)
{
    std::stringstream builder;
    builder << projection.second.explain(verbosity);
    builder << " as " << projection.first;
    return builder.str();
}

Schema inferOutputSchema(
    const Schema& inputSchema,
    const ProjectionLogicalOperator& projectionOperator,
    const std::vector<ProjectionLogicalOperator::Projection>& inferredProjections,
    bool asterisk)
{
    const auto fieldProjections = [&]
    {
        if (asterisk)
        {
            return inputSchema
                | std::views::transform([&projectionOperator](const Field& field)
                                        { return Field(projectionOperator, field.getLastName(), field.getDataType()); })
                | std::ranges::to<std::vector>();
        }
        return std::vector<Field>();
    }();

    const auto projections = inferredProjections
        | std::views::transform([&projectionOperator](const auto& projection)
                                { return Field(projectionOperator, projection.first, projection.second.getDataType()); })
        | std::ranges::to<std::vector>();
    auto outputSchema = Schema::tryCreateCollisionFree(std::vector{projections, fieldProjections} | std::views::join);
    if (!outputSchema.has_value())
    {
        throw CannotInferSchema("Found collisions of field names in projection: {}", Schema::createCollisionString(outputSchema.error()));
    }
    return outputSchema.value();
}
}

ProjectionLogicalOperator::ProjectionLogicalOperator(std::vector<Projection> projections, Asterisk asterisk)
    : asterisk(asterisk.value), projections(std::move(projections))
{
}

ProjectionLogicalOperator::ProjectionLogicalOperator(LogicalOperator child, DescriptorConfig::Config config)
{
    const auto functionVariant = config[ConfigParameters::PROJECTION_FUNCTION_NAME];
    const auto asteriskValue = std::get<bool>(config[ConfigParameters::ASTERISK]);

    if (const auto* projection = std::get_if<NES::ProjectionList>(&functionVariant))
    {
        this->asterisk = asteriskValue;
        this->child = std::move(child);

        const auto& childOutputSchema = this->child.getOutputSchema();

        this->projections = projection->projections()
            | std::views::transform(
                                [&childOutputSchema](const auto& serialized)
                                {
                                    return ProjectionLogicalOperator::Projection{
                                        IdentifierSerializationUtil::deserializeIdentifier(serialized.identifier()),
                                        FunctionSerializationUtil::deserializeFunction(serialized.function(), childOutputSchema)};
                                })
            | std::ranges::to<std::vector>();

        // Infer projections with input schema
        auto inferredProjections = this->projections
            | std::views::transform(
                                       [&childOutputSchema](const Projection& projection)
                                       {
                                           auto inferredFunction = projection.second.withInferredDataType(childOutputSchema);
                                           return Projection{projection.first, inferredFunction};
                                       })
            | std::ranges::to<std::vector>();

        this->projections = inferredProjections;
        this->outputSchema = inferOutputSchema(childOutputSchema, *this, inferredProjections, this->asterisk);
    }
    else
    {
        throw UnknownLogicalOperator();
    }
}

std::string_view ProjectionLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::vector<Field> ProjectionLogicalOperator::getAccessedFields() const
{
    /// TODO we might want to move this to the inferSchema/constructor as well

    auto asteriskFields = [&]
    {
        if (asterisk)
        {
            return child.getOutputSchema() | std::ranges::to<std::vector>();
        }
        return std::vector<Field>();
    }();

    auto projectedFields = projections | std::views::values
        | std::views::transform(
                               [](const LogicalFunction& function)
                               {
                                   return BFSRange(function)
                                       | std::views::filter([](const LogicalFunction& function)
                                                            { return function.tryGet<FieldAccessLogicalFunction>().has_value(); })
                                       | std::views::transform([](const LogicalFunction& function)
                                                               { return function.get<FieldAccessLogicalFunction>().getField(); });
                               })
        | std::views::join | std::ranges::to<std::vector>();
    return std::array{asteriskFields, projectedFields} | std::views::join | std::ranges::to<std::vector>();
}

const std::vector<ProjectionLogicalOperator::Projection>& ProjectionLogicalOperator::getProjections() const
{
    return projections;
}

bool ProjectionLogicalOperator::operator==(const ProjectionLogicalOperator& rhs) const
{
    return projections == rhs.projections && outputSchema == rhs.outputSchema &&  traitSet == rhs.traitSet;
};

std::string ProjectionLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    auto explainedProjections
        = std::views::transform(projections, [&](const auto& projection) { return explainProjection(projection, verbosity); });
    std::stringstream builder;
    builder << "PROJECTION(";
    if (verbosity == ExplainVerbosity::Debug)
    {
        builder << "opId: " << id << ", ";
    }
    if (outputSchema.has_value() && not std::ranges::empty(outputSchema.value()))
    {
        builder << "schema: " << outputSchema.value() << ", ";
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
    builder << fmt::format(", traitSet: {}", traitSet.explain(verbosity));
    builder << "])";
    return builder.str();
}

ProjectionLogicalOperator ProjectionLogicalOperator::withInferredSchema() const
{
    auto copy = *this;
    copy.child = copy.child.withInferredSchema();

    auto inputSchema = copy.child.getOutputSchema();

    /// Propagate the type inference to all projection functions and resolve projection names.
    auto inferredProjections = projections
        | std::views::transform(
                                   [&inputSchema](const Projection& projection)
                                   {
                                       auto inferredFunction = projection.second.withInferredDataType(inputSchema);
                                       /// If projection has a name use it.
                                       return Projection{projection.first, inferredFunction};
                                       /// Otherwise derive the name from the inferred function
                                       // return Projection{inferredFunction.explain(ExplainVerbosity::Short), inferredFunction};
                                   })
        | std::ranges::to<std::vector>();

    copy.projections = inferredProjections;
    copy.outputSchema = inferOutputSchema(inputSchema, copy, inferredProjections, copy.asterisk);
    return copy;
}

TraitSet ProjectionLogicalOperator::getTraitSet() const
{
    return traitSet;
}

ProjectionLogicalOperator ProjectionLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

ProjectionLogicalOperator ProjectionLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for projection, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

Schema ProjectionLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return outputSchema.value();
}

std::vector<LogicalOperator> ProjectionLogicalOperator::getChildren() const
{
    return {child};
}

LogicalOperator ProjectionLogicalOperator::getChild() const
{
    return child;
}

void ProjectionLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    for (const auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    ProjectionList projList;
    for (const auto& [name, fn] : getProjections())
    {
        auto& proj = *projList.add_projections();
        IdentifierSerializationUtil::serializeIdentifier(name, proj.mutable_identifier());
        proj.mutable_function()->CopyFrom(fn.serialize());
    }
    (*serializableOperator.mutable_config())[ConfigParameters::PROJECTION_FUNCTION_NAME] = descriptorConfigTypeToProto(projList);
    (*serializableOperator.mutable_config())[ConfigParameters::ASTERISK] = descriptorConfigTypeToProto(asterisk);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterProjectionLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("Expected one child for ProjectionLogicalOperator, but found {}", arguments.children.size());
    }
    return ProjectionLogicalOperator{std::move(arguments.children.at(0)), std::move(arguments.config)};
}

}
