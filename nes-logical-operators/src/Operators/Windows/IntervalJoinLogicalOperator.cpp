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

#include <Operators/Windows/IntervalJoinLogicalOperator.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

IntervalJoinLogicalOperator::IntervalJoinLogicalOperator(
    WeakLogicalOperator self,
    LogicalFunction joinFunction,
    Windowing::TimeCharacteristic timeCharacteristic,
    int64_t lowerBound,
    int64_t upperBound,
    JoinLogicalOperator::JoinType joinType)
    : ManagedByOperator(std::move(self))
    , joinFunction(std::move(joinFunction))
    , timeCharacteristic(std::move(timeCharacteristic))
    , lowerBound(lowerBound)
    , upperBound(upperBound)
    , joinType(joinType)
{
    if (lowerBound >= upperBound)
    {
        throw InvalidQuerySyntax(
            "IntervalJoin requires lowerBound < upperBound; got lowerBound={}, upperBound={}.", lowerBound, upperBound);
    }
    /// TODO #1471: PR #1471 will add LEFT_OUTER / RIGHT_OUTER / FULL_OUTER variants.
    INVARIANT(
        joinType == JoinLogicalOperator::JoinType::INNER_JOIN,
        "IntervalJoinLogicalOperator currently supports only INNER_JOIN; PR #1471 will add the remaining variants.");
}

std::string_view IntervalJoinLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool IntervalJoinLogicalOperator::operator==(const IntervalJoinLogicalOperator& rhs) const
{
    return timeCharacteristic == rhs.timeCharacteristic and lowerBound == rhs.lowerBound and upperBound == rhs.upperBound
        and joinType == rhs.joinType and getJoinFunction() == rhs.getJoinFunction() and outputSchema == rhs.outputSchema
        and leftInputSchema == rhs.leftInputSchema and rightInputSchema == rhs.rightInputSchema and getTraitSet() == rhs.getTraitSet();
}

std::string IntervalJoinLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "IntervalJoin(opId: {}, lowerBound: {} ms, upperBound: {} ms, joinFunction: {}, windowStartField: {}, windowEndField: {}, "
            "traitSet: {})",
            id,
            lowerBound,
            upperBound,
            getJoinFunction().explain(verbosity),
            windowMetaData.windowStartFieldName,
            windowMetaData.windowEndFieldName,
            traitSet.explain(verbosity));
    }
    return fmt::format("IntervalJoin({})", getJoinFunction().explain(verbosity));
}

IntervalJoinLogicalOperator IntervalJoinLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    const auto& leftInputSchemaParam = inputSchemas[0];
    const auto& rightInputSchemaParam = inputSchemas[1];

    auto copy = *this;
    copy.outputSchema = Schema{};
    copy.leftInputSchema = leftInputSchemaParam;
    copy.rightInputSchema = rightInputSchemaParam;

    const auto newQualifierForSystemField = [](const Schema& leftSchema, const Schema& rightSchema)
    {
        const auto sourceNameLeft = leftSchema.getSourceNameQualifier();
        const auto sourceNameRight = rightSchema.getSourceNameQualifier();
        if (not(sourceNameLeft and sourceNameRight))
        {
            throw TypeInferenceException("Schemas of IntervalJoin operator must have source names.");
        }
        return sourceNameLeft.value() + sourceNameRight.value() + Schema::ATTRIBUTE_NAME_SEPARATOR;
    }(leftInputSchemaParam, rightInputSchemaParam);

    copy.windowMetaData.windowStartFieldName = newQualifierForSystemField + "START";
    copy.windowMetaData.windowEndFieldName = newQualifierForSystemField + "END";
    copy.outputSchema.addField(copy.windowMetaData.windowStartFieldName, DataType::Type::UINT64);
    copy.outputSchema.addField(copy.windowMetaData.windowEndFieldName, DataType::Type::UINT64);

    for (const auto& field : leftInputSchemaParam.getFields())
    {
        copy.outputSchema.addField(field.name, field.dataType);
    }
    for (const auto& field : rightInputSchemaParam.getFields())
    {
        copy.outputSchema.addField(field.name, field.dataType);
    }

    auto inputSchema = leftInputSchemaParam;
    inputSchema.appendFieldsFromOtherSchema(rightInputSchemaParam);
    copy.joinFunction = joinFunction.withInferredDataType(inputSchema);
    return copy;
}

IntervalJoinLogicalOperator IntervalJoinLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

TraitSet IntervalJoinLogicalOperator::getTraitSet() const
{
    return traitSet;
}

IntervalJoinLogicalOperator IntervalJoinLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> IntervalJoinLogicalOperator::getInputSchemas() const
{
    return {leftInputSchema, rightInputSchema};
}

Schema IntervalJoinLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> IntervalJoinLogicalOperator::getChildren() const
{
    return children;
}

Schema IntervalJoinLogicalOperator::getLeftSchema() const
{
    return leftInputSchema;
}

Schema IntervalJoinLogicalOperator::getRightSchema() const
{
    return rightInputSchema;
}

const Windowing::TimeCharacteristic& IntervalJoinLogicalOperator::getTimeCharacteristic() const
{
    return timeCharacteristic;
}

int64_t IntervalJoinLogicalOperator::getLowerBound() const noexcept
{
    return lowerBound;
}

int64_t IntervalJoinLogicalOperator::getUpperBound() const noexcept
{
    return upperBound;
}

JoinLogicalOperator::JoinType IntervalJoinLogicalOperator::getJoinType() const noexcept
{
    return joinType;
}

std::string IntervalJoinLogicalOperator::getWindowStartFieldName() const
{
    return windowMetaData.windowStartFieldName;
}

std::string IntervalJoinLogicalOperator::getWindowEndFieldName() const
{
    return windowMetaData.windowEndFieldName;
}

const WindowMetaData& IntervalJoinLogicalOperator::getWindowMetaData() const
{
    return windowMetaData;
}

LogicalFunction IntervalJoinLogicalOperator::getJoinFunction() const
{
    return joinFunction;
}

Reflected Reflector<TypedLogicalOperator<IntervalJoinLogicalOperator>>::operator()(const TypedLogicalOperator<IntervalJoinLogicalOperator>& op) const
{
    return reflect(detail::ReflectedIntervalJoinLogicalOperator{
        .joinFunction = op->getJoinFunction(),
        .timeCharacteristic = reflect(op->getTimeCharacteristic()),
        .lowerBound = op->getLowerBound(),
        .upperBound = op->getUpperBound(),
        .joinType = op->joinType});
}

TypedLogicalOperator<IntervalJoinLogicalOperator>
Unreflector<TypedLogicalOperator<IntervalJoinLogicalOperator>>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [joinFunction, reflectedTimeCharacteristic, lowerBound, upperBound, joinType]
        = context.unreflect<detail::ReflectedIntervalJoinLogicalOperator>(reflected);

    if (!joinFunction.has_value())
    {
        throw CannotDeserialize("Missing join function for IntervalJoinLogicalOperator");
    }

    auto timeCharacteristic = context.unreflect<Windowing::TimeCharacteristic>(reflectedTimeCharacteristic);

    return TypedLogicalOperator<IntervalJoinLogicalOperator>{
        std::move(joinFunction.value()), std::move(timeCharacteristic), lowerBound, upperBound, joinType};
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterIntervalJoinLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return ReflectionContext{}.unreflect<TypedLogicalOperator<IntervalJoinLogicalOperator>>(arguments.reflected);
    }
    PRECONDITION(false, "Operator is only build directly via parser or via reflection, not using the registry");
    std::unreachable();
}

}
