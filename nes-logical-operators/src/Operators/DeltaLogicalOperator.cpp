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

#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

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

Reflected Reflector<DeltaLogicalOperator>::operator()(const DeltaLogicalOperator& op) const
{
    detail::ReflectedDeltaLogicalOperator reflected;
    reflected.deltaExpressions.reserve(op.deltaExpressions.size());
    for (const auto& [alias, fn] : op.deltaExpressions)
    {
        reflected.deltaExpressions.emplace_back(alias.getFieldName(), std::make_optional(fn));
    }
    return reflect(reflected);
}

DeltaLogicalOperator Unreflector<DeltaLogicalOperator>::operator()(const Reflected& reflected) const
{
    auto [exprPairs] = unreflect<detail::ReflectedDeltaLogicalOperator>(reflected);
    std::vector<DeltaLogicalOperator::DeltaExpression> deltaExpressions;
    deltaExpressions.reserve(exprPairs.size());
    for (const auto& [name, fnOpt] : exprPairs)
    {
        if (!fnOpt.has_value())
        {
            throw CannotDeserialize("DeltaLogicalOperator: delta expression function is missing");
        }
        deltaExpressions.emplace_back(FieldIdentifier(name), fnOpt.value());
    }
    if (deltaExpressions.empty())
    {
        throw CannotDeserialize("DeltaLogicalOperator requires at least one delta expression");
    }
    return DeltaLogicalOperator(std::move(deltaExpressions));
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterDeltaLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<DeltaLogicalOperator>(arguments.reflected);
    }
    PRECONDITION(false, "DeltaLogicalOperator is only built directly via parser or via reflection");
    std::unreachable();
}

}
