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

#include <array>
#include <cstddef>
#include <functional>
#include <numeric>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <Schema/Binder.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Reprojecter.hpp>
#include <Schema/Field.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Util/DynamicBase.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

ProjectionLogicalOperator::ProjectionLogicalOperator(
    WeakLogicalOperator self, std::vector<UnboundProjection> projections, Asterisk asterisk)
    : self(std::move(self)), asterisk(asterisk.value), projections(std::move(projections))
{
}

ProjectionLogicalOperator::ProjectionLogicalOperator(
    WeakLogicalOperator self, LogicalOperator child, std::vector<UnboundProjection> projections, const Asterisk asterisk)
    : self(std::move(self)), child(std::move(child)), asterisk(asterisk.value), projections(std::move(projections))
{
    inferLocalSchema();
}

std::string_view ProjectionLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::vector<Field> ProjectionLogicalOperator::getAccessedFields() const
{
    PRECONDITION(child.has_value(), "Child not set when accessing accessed fields");
    auto asteriskFields = [&]
    {
        if (asterisk)
        {
            return child->getOutputSchema() | std::ranges::to<std::vector>();
        }
        return std::vector<Field>();
    }();

    auto projectedFields = getProjections() | std::views::values
        | std::views::transform(
                               [](const LogicalFunction& projectionFunction)
                               {
                                   return BFSRange(projectionFunction)
                                       | std::views::filter([](const LogicalFunction& function)
                                                            { return function.tryGetAs<FieldAccessLogicalFunction>().has_value(); })
                                       | std::views::transform([](const LogicalFunction& function)
                                                               { return function.getAs<FieldAccessLogicalFunction>()->getField(); });
                               })
        | std::views::join | std::ranges::to<std::vector>();
    return std::array{asteriskFields, projectedFields} | std::views::join | std::ranges::to<std::vector>();
}

std::vector<ProjectionLogicalOperator::Projection> ProjectionLogicalOperator::getProjections() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed projections before inferring schema");
    return projections | RangeBinder{self.lock()} | std::ranges::to<std::vector>();
}

std::unordered_map<Field, std::unordered_set<Field>> ProjectionLogicalOperator::getAccessedFieldsForOutput() const
{
    auto pairs = getProjections()
        | std::views::filter([](const Projection& projection)
                             { return !projection.second.tryGetAs<FieldAccessLogicalFunction>().has_value(); })
        | std::views::transform(
                     [](const auto& projection)
                     {
                         auto fields = BFSRange{projection.second}
                             | std::views::filter([](const LogicalFunction& function)
                                                  { return function.tryGetAs<FieldAccessLogicalFunction>().has_value(); })
                             | std::views::transform([](const LogicalFunction& function)
                                                     { return function.getAs<FieldAccessLogicalFunction>()->getField(); })
                             | std::ranges::to<std::vector>();
                         return std::pair{projection.first, std::unordered_set<Field>{fields.begin(), fields.end()}};
                     })
        | std::ranges::to<std::vector>();
    return std::unordered_map<Field, std::unordered_set<Field>>{pairs.begin(), pairs.end()};
}

bool ProjectionLogicalOperator::operator==(const ProjectionLogicalOperator& rhs) const
{
    return projections == rhs.projections && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
};

std::string ProjectionLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    auto explainedProjections = std::views::transform(
        projections,
        [&](const auto& projection)
        {
            std::stringstream builder;
            builder << projection.second.explain(verbosity);
            if (fmt::format("{}", projection.first) != projection.second.explain(verbosity))
            {
                builder << " as " << projection.first;
            }
            return builder.str();
        });
    std::stringstream builder;
    builder << "PROJECTION(";
    if (verbosity == ExplainVerbosity::Debug)
    {
        builder << "opId: " << id << ", ";
        if (outputSchema.has_value() && not std::ranges::empty(outputSchema.value()))
        {
            builder << "schema: " << outputSchema.value() << ", ";
        }
    }

    builder << "fields: [";
    if (asterisk)
    {
        builder << "*";
        if (projections.empty())
        {
            builder << ", ";
        }
    }
    builder << fmt::format("{}", fmt::join(explainedProjections, ", "));
    builder << "]";

    if (verbosity == ExplainVerbosity::Debug)
    {
        builder << fmt::format(", traitSet: {}", traitSet.explain(verbosity));
    }
    builder << ")";
    return builder.str();
}

void ProjectionLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto inputSchema = child->getOutputSchema();
    projections = projections
        | std::views::transform(
                      [&inputSchema](const UnboundProjection& projection)
                      {
                          auto inferredFunction = projection.second.withInferredDataType(inputSchema);
                          return UnboundProjection{projection.first, inferredFunction};
                      })
        | std::ranges::to<std::vector>();

    const auto fieldProjections = [&]
    {
        if (asterisk)
        {
            return unbind(inputSchema) | std::ranges::to<std::vector>();
        }
        return std::vector<UnqualifiedUnboundField>();
    }();

    const auto fieldsFromProjections = this->projections
        | std::views::transform([](const auto& projection)
                                { return UnqualifiedUnboundField{projection.first, projection.second.getDataType()}; })
        | std::ranges::to<std::vector>();
    auto outputSchemaOpt = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(
        std::vector{fieldsFromProjections, fieldProjections} | std::views::join | std::ranges::to<std::vector>());

    if (!outputSchemaOpt.has_value())
    {
        throw CannotInferSchema(
            "Found collisions of field names in projection: {}",
            Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOpt.error()));
    }
    outputSchema = std::move(outputSchemaOpt).value();
}

ProjectionLogicalOperator ProjectionLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
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

Schema<Field, Unordered> ProjectionLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bind(self.lock(), outputSchema.value() | std::ranges::to<Schema<UnqualifiedUnboundField, Unordered>>());
}

Schema<Field, Ordered> ProjectionLogicalOperator::getOrderedOutputSchema(const ChildOutputOrderProvider orderProvider) const
{
    INVARIANT(child.has_value(), "Child not set when trying to get ordered output schema");
    std::vector<UnqualifiedUnboundField> fields;
    if (asterisk)
    {
        fields = orderProvider(child.value()) | RangeUnbinder{} | std::ranges::to<std::vector>();
    }
    for (const auto& [name, function] : projections)
    {
        fields.emplace_back(name, function.getDataType());
    }
    return fields | RangeBinder{self.lock()} | std::ranges::to<Schema<Field, Ordered>>();
}

std::vector<LogicalOperator> ProjectionLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

const detail::DynamicBase* ProjectionLogicalOperator::getDynamicBase() const
{
    return static_cast<const Reprojecter*>(this);
}

LogicalOperator ProjectionLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

bool ProjectionLogicalOperator::hasAsterisk() const
{
    return asterisk;
}

Reflected
Reflector<TypedLogicalOperator<ProjectionLogicalOperator>>::operator()(const TypedLogicalOperator<ProjectionLogicalOperator>& op) const
{
    auto projections = op->getProjections()
        | std::views::transform([](const auto& projection) { return std::pair{projection.first.getLastName(), projection.second}; })
        | std::ranges::to<std::vector>();
    return reflect(
        detail::ReflectedProjectionLogicalOperator{.operatorId = op.getId(), .asterisk = op->hasAsterisk(), .projections = projections});
}

Unreflector<TypedLogicalOperator<ProjectionLogicalOperator>>::Unreflector(ContextType operatorMapping) : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<ProjectionLogicalOperator>
Unreflector<TypedLogicalOperator<ProjectionLogicalOperator>>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [id, asterisk, projections] = context.unreflect<detail::ReflectedProjectionLogicalOperator>(reflected);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("ProjectionLogicalOperator must have exactly one child, but got {}", children.size());
    }

    return TypedLogicalOperator<ProjectionLogicalOperator>{
        children.at(0),
        std::move(projections),
        ProjectionLogicalOperator::Asterisk(asterisk),
    };
}

}

std::size_t std::hash<NES::ProjectionLogicalOperator>::operator()(const NES::ProjectionLogicalOperator& projectionOperator) const noexcept
{
    /// NOLINTBEGIN(readability-magic-numbers)
    return folly::hash::commutative_hash_combine_range_generic(
        13, folly::hash::StdHasher{}, projectionOperator.projections.begin(), projectionOperator.projections.end());
    /// NOLINTEND(readability-magic-numbers)
}
