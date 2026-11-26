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
#include <AsyncDelayLogicalOperator.hpp>

#include <cstddef>
#include <functional>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/CheckedInteger.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Operators/AsyncLoweringContext.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/CheckedDeadline.hpp>
#include <Util/PlanRenderer.hpp>
#include <folly/hash/Hash.h>
#include <AsyncDelayAsyncOperator.hpp>
#include <AsyncOperatorLoweringRegistry.hpp>
#include <ErrorHandling.hpp>
#include <OperatorFunctionRegistry.hpp>

namespace NES
{
namespace
{
std::optional<RuntimeValue> constantValue(const LogicalFunction& function)
{
    const auto literal = function.tryGetAs<ConstantValueLogicalFunction>();
    if (!literal)
    {
        return std::nullopt;
    }
    auto value = RuntimeValue::fromLiteral(function.getDataType(), literal.value()->getConstantValue());
    if (!value)
    {
        throw CannotInferSchema("Invalid ASYNC_DELAY constant argument: {}", value.error().what());
    }
    return std::move(*value);
}

void requireInteger(const LogicalFunction& function, std::string_view name)
{
    const auto type = function.getDataType();
    if (!type.isInteger() || type.nullable)
    {
        throw DifferentFieldTypeExpected("ASYNC_DELAY {} must be a non-nullable integer", name);
    }
}

void validateDelay(const LogicalFunction& value, const LogicalFunction& millis)
{
    if (value.getDataType().isType(DataType::Type::UNDEFINED))
    {
        throw CannotInferSchema("ASYNC_DELAY value type is unresolved");
    }
    requireInteger(millis, "delay in milliseconds");
    if (const auto constant = constantValue(millis))
    {
        const auto duration = checkedUnsignedIntegerOption(constant->view(), "ASYNC_DELAY millis");
        if (!duration || !checkedSteadyDeadline(*duration, "ASYNC_DELAY"))
        {
            throw CannotInferSchema("ASYNC_DELAY constant delay must be a representable non-negative integer");
        }
    }
}
}

AsyncDelayLogicalOperator::AsyncDelayLogicalOperator(
    WeakLogicalOperator self, LogicalOperator child, LogicalFunction value, LogicalFunction millis)
    : ManagedByOperator(std::move(self)), child(std::move(child)), value(std::move(value)), millis(std::move(millis))
{
    inferLocalSchema();
}

TypedLogicalOperator<AsyncDelayLogicalOperator>
AsyncDelayLogicalOperator::create(LogicalOperator child, LogicalFunction value, LogicalFunction millis)
{
    return TypedLogicalOperator<AsyncDelayLogicalOperator>{std::move(child), std::move(value), std::move(millis)};
}

AsyncOperator AsyncDelayLogicalOperator::lowerToAsync(const LogicalOperator& op, AsyncLoweringContext& context)
{
    const auto& named = *op.getAs<AsyncDelayLogicalOperator>();
    return AsyncDelayAsyncOperator{context.lower(named.value), context.lower(named.millis)};
}

OperatorFunctionRegistryEntry AsyncDelayLogicalOperator::functionSyntax()
{
    return {.resultName = Identifier::parse("value"),
            .inferResultType = [](const std::vector<LogicalFunction>& args) -> DataType
            {
                if (args.size() != 2)
                {
                    throw InvalidQuerySyntax("ASYNC_DELAY takes (value, millis)");
                }
                validateDelay(args[0], args[1]);
                return args[0].getDataType();
            },
            .createOperator = [](LogicalOperator child, const std::vector<LogicalFunction>& args) -> LogicalOperator
            {
                if (args.size() != 2)
                {
                    throw InvalidQuerySyntax("ASYNC_DELAY takes (value, millis)");
                }
                return create(std::move(child), args[0], args[1]);
            },
            .getArgumentsFromOperator = [](const LogicalOperator& op) -> std::optional<std::vector<LogicalFunction>>
            {
                if (const auto invocation = op.tryGetAs<AsyncDelayLogicalOperator>())
                {
                    return std::vector<LogicalFunction>{invocation.value()->value, invocation.value()->millis};
                }
                return std::nullopt;
            }};
}

bool AsyncDelayLogicalOperator::operator==(const AsyncDelayLogicalOperator& other) const
{
    return StructuralFunctionEqual{}(value, other.value) && StructuralFunctionEqual{}(millis, other.millis)
        && outputSchema == other.outputSchema && traitSet == other.traitSet;
}

void AsyncDelayLogicalOperator::validate() const
{
    validateDelay(value, millis);
}

void AsyncDelayLogicalOperator::inferLocalSchema()
{
    const auto input = child.getOutputSchema();
    TypeInferenceContext context{input};
    value = context.inferType(value);
    millis = context.inferType(millis);
    validate();
    auto fields = input | RangeUnbinder{} | std::ranges::to<std::vector>();
    fields.emplace_back(Identifier::parse("value"), value.getDataType());
    auto schema = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(fields);
    if (!schema)
    {
        throw CannotInferSchema("AsyncDelay result fields collide; use a projection to rename fields: {}",
                                Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(schema.error()));
    }
    outputSchema = std::move(schema).value();
}

AsyncDelayLogicalOperator AsyncDelayLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "AsyncDelay requires exactly one child");
    auto copy = *this;
    copy.child = std::move(children.front());
    return copy;
}

AsyncDelayLogicalOperator AsyncDelayLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = withChildrenUnsafe(std::move(children));
    copy.inferLocalSchema();
    return copy;
}

AsyncDelayLogicalOperator AsyncDelayLogicalOperator::withInferredSchema() const
{
    return withChildren({child.withInferredSchema()});
}

AsyncDelayLogicalOperator AsyncDelayLogicalOperator::withTraitSet(TraitSet traits) const
{
    auto copy = *this;
    copy.traitSet = std::move(traits);
    return copy;
}

Schema<Field, Unordered> AsyncDelayLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "AsyncDelay schema has not been inferred");
    return bindToOperator(self.lock(), *outputSchema);
}

Schema<Field, Ordered> AsyncDelayLogicalOperator::getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const
{
    auto fields = orderProvider(child) | RangeUnbinder{} | std::ranges::to<std::vector>();
    fields.emplace_back(Identifier::parse("value"), value.getDataType());
    auto schema = Schema<UnqualifiedUnboundField, Ordered>::tryCreateCollisionFree(std::move(fields));
    if (!schema)
    {
        throw CannotInferSchema("AsyncDelay result fields collide; use a projection to rename fields");
    }
    return bindToOperator(self.lock(), std::move(schema).value());
}

std::string AsyncDelayLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId) const
{
    return "ASYNC ASYNC_DELAY(args: [" + value.explain(verbosity) + ", " + millis.explain(verbosity) + "], outputs: [VALUE])";
}

Reflected Reflector<TypedLogicalOperator<AsyncDelayLogicalOperator>>::operator()(const TypedLogicalOperator<AsyncDelayLogicalOperator>& op,
                                                                                 const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedAsyncDelayLogicalOperator{.operatorId = op.getId(), .value = op->getValue(), .millis = op->getMillis()});
}

Unreflector<TypedLogicalOperator<AsyncDelayLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<AsyncDelayLogicalOperator>
Unreflector<TypedLogicalOperator<AsyncDelayLogicalOperator>>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto value = context.unreflect<detail::ReflectedAsyncDelayLogicalOperator>(reflected);
    auto children = plan->getChildrenFor(value.operatorId, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("AsyncDelay requires exactly one child");
    }
    return AsyncDelayLogicalOperator::create(std::move(children.front()), std::move(value.value), std::move(value.millis));
}
}

size_t std::hash<NES::AsyncDelayLogicalOperator>::operator()(const NES::AsyncDelayLogicalOperator& op) const noexcept
{
    return folly::hash::hash_combine(0, NES::StructuralFunctionHash{}(op.getValue()), NES::StructuralFunctionHash{}(op.getMillis()));
}
