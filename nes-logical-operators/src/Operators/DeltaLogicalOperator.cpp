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

#include <Operators/DeltaLogicalOperator.hpp>

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
#include <Operators/LogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

namespace
{
std::string explainDeltaExpression(const DeltaLogicalOperator::DeltaExpression& expr, ExplainVerbosity verbosity)
{
    std::stringstream builder;
    builder << expr.second.explain(verbosity);
    builder << " as " << expr.first.getFieldName();
    return builder.str();
}
}

DeltaLogicalOperator::DeltaLogicalOperator(std::vector<DeltaExpression> deltaExpressions) : deltaExpressions(std::move(deltaExpressions))
{
}

std::string_view DeltaLogicalOperator::getName() const noexcept
{
    return NAME;
}

const std::vector<DeltaLogicalOperator::DeltaExpression>& DeltaLogicalOperator::getDeltaExpressions() const
{
    return deltaExpressions;
}

bool DeltaLogicalOperator::operator==(const DeltaLogicalOperator& rhs) const
{
    return deltaExpressions == rhs.deltaExpressions && getOutputSchema() == rhs.getOutputSchema()
        && getInputSchemas() == rhs.getInputSchemas() && getTraitSet() == rhs.getTraitSet();
}

std::string DeltaLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    auto explainedExpressions
        = std::views::transform(deltaExpressions, [&](const auto& expr) { return explainDeltaExpression(expr, verbosity); });
    std::stringstream builder;
    builder << "DELTA(";
    if (verbosity == ExplainVerbosity::Debug)
    {
        builder << "opId: " << opId << ", ";
        if (!outputSchema.getFieldNames().empty())
        {
            builder << "schema: " << outputSchema << ", ";
        }
    }
    builder << "fields: [" << fmt::format("{}", fmt::join(explainedExpressions, ", ")) << "]";
    if (verbosity == ExplainVerbosity::Debug)
    {
        builder << fmt::format(", traitSet: {}", traitSet.explain(verbosity));
    }
    builder << ")";
    return builder.str();
}

DataType DeltaLogicalOperator::toSignedType(DataType /* inputType */)
{
    /// TODO: implement type conversion for delta results.
    /// The delta of two values can be negative, so unsigned types should be
    /// converted to their signed counterparts. Floats are already signed.
    /// Non-numeric types should throw CannotInferSchema.
    throw CannotInferSchema("toSignedType not implemented");
}

DeltaLogicalOperator DeltaLogicalOperator::withInferredSchema(std::vector<Schema> /* inputSchemas */) const
{
    /// TODO: implement schema inference for the delta operator.
    /// 1. Validate that inputSchemas is non-empty and all schemas are equal.
    /// 2. Infer each delta expression's data type from the input schema.
    /// 3. Build the output schema: all input fields + delta fields (with signed types via toSignedType).
    throw CannotInferSchema("withInferredSchema not implemented");
}

TraitSet DeltaLogicalOperator::getTraitSet() const
{
    return traitSet;
}

DeltaLogicalOperator DeltaLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

DeltaLogicalOperator DeltaLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

std::vector<Schema> DeltaLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
}

Schema DeltaLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> DeltaLogicalOperator::getChildren() const
{
    return children;
}

void DeltaLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    for (auto& input : getInputSchemas())
    {
        auto* inSch = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(input, inSch);
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    for (const auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    ProjectionList exprList;
    for (const auto& [name, fn] : getDeltaExpressions())
    {
        auto& proj = *exprList.add_projections();
        proj.set_identifier(name.getFieldName());
        proj.mutable_function()->CopyFrom(fn.serialize());
    }
    (*serializableOperator.mutable_config())[ConfigParameters::DELTA_EXPRESSIONS_NAME] = descriptorConfigTypeToProto(exprList);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterDeltaLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    const auto expressionsVariant = arguments.config.at(DeltaLogicalOperator::ConfigParameters::DELTA_EXPRESSIONS_NAME);

    if (const auto* expressions = std::get_if<ProjectionList>(&expressionsVariant))
    {
        auto logicalOperator = DeltaLogicalOperator(
            expressions->projections()
            | std::views::transform(
                [](const auto& serialized)
                {
                    return DeltaLogicalOperator::DeltaExpression{
                        FieldIdentifier(serialized.identifier()), FunctionSerializationUtil::deserializeFunction(serialized.function())};
                })
            | std::ranges::to<std::vector>());

        return logicalOperator.withInferredSchema(arguments.inputSchemas);
    }
    throw UnknownLogicalOperator();
}

}
