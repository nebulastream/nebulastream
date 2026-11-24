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
#include <Sources/AsyncInputAccessor.hpp>
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

void validateDelay(const LogicalFunction& value, const LogicalFunction& millis, const std::optional<LogicalFunction>& maxConcurrency)
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
    if (maxConcurrency)
    {
        requireInteger(*maxConcurrency, "concurrency limit");
        static_cast<void>(AsyncDelayLogicalOperator::getConcurrencyLimit(maxConcurrency));
    }
}
}

AsyncDelayLogicalOperator::AsyncDelayLogicalOperator(
    WeakLogicalOperator self,
    LogicalOperator child,
    LogicalFunction value,
    LogicalFunction millis,
    std::optional<LogicalFunction> maxConcurrency)
    : ManagedByOperator(std::move(self))
    , child(std::move(child))
    , value(std::move(value))
    , millis(std::move(millis))
    , maxConcurrency(std::move(maxConcurrency))
{
    inferLocalSchema();
}

TypedLogicalOperator<AsyncDelayLogicalOperator>
AsyncDelayLogicalOperator::create(LogicalOperator child, LogicalFunction value, LogicalFunction millis)
{
    return TypedLogicalOperator<AsyncDelayLogicalOperator>{std::move(child), std::move(value), std::move(millis), std::nullopt};
}

TypedLogicalOperator<AsyncDelayLogicalOperator> AsyncDelayLogicalOperator::create(
    LogicalOperator child, LogicalFunction value, LogicalFunction millis, std::optional<LogicalFunction> maxConcurrency)
{
    return TypedLogicalOperator<AsyncDelayLogicalOperator>{
        std::move(child), std::move(value), std::move(millis), std::move(maxConcurrency)};
}

std::optional<size_t> AsyncDelayLogicalOperator::getConcurrencyLimit(const std::optional<LogicalFunction>& maxConcurrency)
{
    if (!maxConcurrency)
    {
        return std::nullopt;
    }
    requireInteger(*maxConcurrency, "concurrency limit");
    const auto constant = constantValue(*maxConcurrency);
    if (!constant)
    {
        throw CannotInferSchema("ASYNC_DELAY requires a constant concurrency limit");
    }
    const auto limit = checkedUnsignedIntegerOption(constant->view(), "ASYNC_DELAY concurrency");
    if (!limit || *limit == 0 || !std::in_range<size_t>(*limit))
    {
        throw CannotInferSchema("ASYNC_DELAY constant concurrency must be positive and representable");
    }
    return static_cast<size_t>(*limit);
}

AsyncOperatorLowering AsyncDelayLogicalOperator::lowerToAsync(LogicalOperator op)
{
    const auto invocation = op.getAs<AsyncDelayLogicalOperator>();
    const auto& named = *invocation;
    std::vector<LogicalFunction> arguments{named.value, named.millis};
    AsyncInputAccessors accessors{{named.value.getDataType(), named.millis.getDataType()}};
    auto runtime = AsyncDelayAsyncOperator::create({.inputs = std::move(accessors)}, getConcurrencyLimit(named.maxConcurrency));
    return {.implementation = std::move(runtime), .arguments = std::move(arguments)};
}

OperatorFunctionRegistryEntry AsyncDelayLogicalOperator::functionSyntax()
{
    return {
        .resultName = Identifier::parse("value"),
        .inferResultType = [](const std::vector<LogicalFunction>& args) -> DataType
        {
            if (args.size() < 2 || args.size() > 3)
            {
                throw InvalidQuerySyntax("ASYNC_DELAY takes (value, millis[, max_concurrency])");
            }
            std::optional<LogicalFunction> concurrency;
            if (args.size() == 3)
            {
                concurrency = args[2];
            }
            validateDelay(args[0], args[1], concurrency);
            return args[0].getDataType();
        },
        .createOperator = [](LogicalOperator child, const std::vector<LogicalFunction>& args) -> LogicalOperator
        {
            if (args.size() < 2 || args.size() > 3)
            {
                throw InvalidQuerySyntax("ASYNC_DELAY takes (value, millis[, max_concurrency])");
            }
            return create(std::move(child), args[0], args[1], args.size() == 3 ? std::optional{args[2]} : std::nullopt);
        },
        .getArgumentsFromOperator = [](const LogicalOperator& op) -> std::optional<std::vector<LogicalFunction>>
        {
            if (const auto invocation = op.tryGetAs<AsyncDelayLogicalOperator>())
            {
                std::vector<LogicalFunction> args{invocation.value()->value, invocation.value()->millis};
                if (invocation.value()->maxConcurrency)
                {
                    args.push_back(*invocation.value()->maxConcurrency);
                }
                return args;
            }
            return std::nullopt;
        }};
}

bool AsyncDelayLogicalOperator::operator==(const AsyncDelayLogicalOperator& other) const
{
    return StructuralFunctionEqual{}(value, other.value) && StructuralFunctionEqual{}(millis, other.millis)
        && ((!maxConcurrency && !other.maxConcurrency)
            || (maxConcurrency && other.maxConcurrency && StructuralFunctionEqual{}(*maxConcurrency, *other.maxConcurrency)))
        && outputSchema == other.outputSchema && traitSet == other.traitSet;
}

void AsyncDelayLogicalOperator::validate() const
{
    validateDelay(value, millis, maxConcurrency);
}

void AsyncDelayLogicalOperator::inferLocalSchema()
{
    const auto input = child.getOutputSchema();
    TypeInferenceContext context{input};
    value = context.inferType(value);
    millis = context.inferType(millis);
    if (maxConcurrency)
    {
        maxConcurrency = context.inferType(*maxConcurrency);
    }
    validate();
    auto fields = input | RangeUnbinder{} | std::ranges::to<std::vector>();
    fields.emplace_back(Identifier::parse("value"), value.getDataType());
    auto schema = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(fields);
    if (!schema)
    {
        throw CannotInferSchema(
            "AsyncDelay result fields collide; use a projection to rename fields: {}",
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
    std::string result = "ASYNC ASYNC_DELAY(args: [" + value.explain(verbosity) + ", " + millis.explain(verbosity);
    if (maxConcurrency)
    {
        result += ", " + maxConcurrency->explain(verbosity);
    }
    return result + "], outputs: [VALUE])";
}

Reflected Reflector<TypedLogicalOperator<AsyncDelayLogicalOperator>>::operator()(
    const TypedLogicalOperator<AsyncDelayLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedAsyncDelayLogicalOperator{
            .operatorId = op.getId(), .value = op->getValue(), .millis = op->getMillis(), .maxConcurrency = op->getMaxConcurrency()});
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
    return AsyncDelayLogicalOperator::create(
        std::move(children.front()), std::move(value.value), std::move(value.millis), std::move(value.maxConcurrency));
}
}

size_t std::hash<NES::AsyncDelayLogicalOperator>::operator()(const NES::AsyncDelayLogicalOperator& op) const noexcept
{
    size_t result
        = folly::hash::hash_combine(0, NES::StructuralFunctionHash{}(op.getValue()), NES::StructuralFunctionHash{}(op.getMillis()));
    if (op.getMaxConcurrency())
    {
        result = folly::hash::hash_combine(result, NES::StructuralFunctionHash{}(*op.getMaxConcurrency()));
    }
    return result;
}
