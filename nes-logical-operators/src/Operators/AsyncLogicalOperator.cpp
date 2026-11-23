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

#include <Operators/AsyncLogicalOperator.hpp>

#include <algorithm>
#include <cstddef>
#include <ranges>
#include <string>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/AsyncArgumentsWithConstants.hpp>
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
#include <Sources/AsyncOperator.hpp>
#include <Sources/AsyncOperatorReflection.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <AsyncOperatorRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
AsyncLogicalOperator::AsyncLogicalOperator(
    WeakLogicalOperator self,
    LogicalOperator child,
    AsyncOperator implementation,
    std::vector<LogicalFunction> arguments,
    std::vector<Identifier> outputBindings)
    : ManagedByOperator(std::move(self))
    , child(std::move(child))
    , concreteAsyncImpl(std::move(implementation))
    , arguments(std::move(arguments))
{
    inferLocalSchema(std::move(outputBindings));
}

TypedLogicalOperator<AsyncLogicalOperator> AsyncLogicalOperator::create(
    LogicalOperator child, const std::string& operatorName, std::vector<LogicalFunction> args, std::vector<Identifier> outputBindings)
{
    std::vector<DataType> types;
    types.reserve(args.size());
    for (const auto& argument : args)
    {
        types.push_back(argument.getDataType());
    }
    auto implementation = createAsyncOperator(operatorName, std::move(types));
    return create(std::move(child), std::move(implementation), std::move(args), std::move(outputBindings));
}

TypedLogicalOperator<AsyncLogicalOperator>
AsyncLogicalOperator::create(LogicalOperator child, const std::string& operatorName, std::vector<LogicalFunction> args)
{
    return create(std::move(child), operatorName, std::move(args), {});
}

TypedLogicalOperator<AsyncLogicalOperator> AsyncLogicalOperator::create(
    LogicalOperator child, AsyncOperator implementation, std::vector<LogicalFunction> arguments, std::vector<Identifier> outputBindings)
{
    return TypedLogicalOperator<AsyncLogicalOperator>{
        std::move(child), std::move(implementation), std::move(arguments), std::move(outputBindings)};
}

TypedLogicalOperator<AsyncLogicalOperator>
AsyncLogicalOperator::create(LogicalOperator child, AsyncOperator implementation, std::vector<LogicalFunction> arguments)
{
    return create(std::move(child), std::move(implementation), std::move(arguments), {});
}

bool AsyncLogicalOperator::operator==(const AsyncLogicalOperator& rhs) const
{
    return getOperatorName() == rhs.getOperatorName() && concreteAsyncImpl.getOutputSchema() == rhs.concreteAsyncImpl.getOutputSchema()
        && concreteAsyncImpl.maxConcurrency() == rhs.concreteAsyncImpl.maxConcurrency()
        && std::ranges::equal(arguments, rhs.arguments, StructuralFunctionEqual{}) && outputs == rhs.outputs
        && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

void AsyncLogicalOperator::inferLocalSchema(std::vector<Identifier> bindings)
{
    if (bindings.empty() && !outputs.empty())
    {
        for (const auto& output : outputs)
        {
            bindings.push_back(output.binding);
        }
    }
    const auto input = child.getOutputSchema();
    TypeInferenceContext context{input};
    for (auto& argument : arguments)
    {
        argument = context.inferType(argument);
    }
    concreteAsyncImpl = concreteAsyncImpl.withInferredArguments(makeAsyncArgumentsWithConstants(arguments));
    const auto asyncOutputSchema = concreteAsyncImpl.getOutputSchema() | std::ranges::to<std::vector>();
    if (asyncOutputSchema.empty())
    {
        throw InvalidQuerySyntax("Async operator '{}' must declare at least one output", getOperatorName());
    }
    if (std::ranges::any_of(asyncOutputSchema, [](const auto& field) { return field.getDataType().type == DataType::Type::UNDEFINED; }))
    {
        throw CannotInferSchema("Async operator '{}' has unresolved output types after inference", getOperatorName());
    }
    if (bindings.empty())
    {
        for (const auto& output : asyncOutputSchema)
        {
            bindings.push_back(*output.getFullyQualifiedName().begin());
        }
    }
    if (bindings.size() != asyncOutputSchema.size())
    {
        throw InvalidQuerySyntax(
            "Async operator '{}' requires {} output bindings, got {}", getOperatorName(), asyncOutputSchema.size(), bindings.size());
    }
    outputs.clear();
    auto fields = input | RangeUnbinder{} | std::ranges::to<std::vector>();
    /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) binding and signature sizes match above
    for (size_t i = 0; i < asyncOutputSchema.size(); ++i)
    {
        outputs.push_back(BoundAsyncOutput{.field = asyncOutputSchema[i], .binding = bindings[i]});
        fields.emplace_back(bindings[i], asyncOutputSchema[i].getDataType());
    }
    /// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    auto schema = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(fields);
    if (!schema)
    {
        throw CannotInferSchema(
            "Async output bindings collide with child fields or each other: {}",
            Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(schema.error()));
    }
    outputSchema = std::move(schema).value();
}

AsyncLogicalOperator AsyncLogicalOperator::withArguments(std::vector<LogicalFunction> args) const
{
    auto copy = *this;
    copy.arguments = std::move(args);
    copy.inferLocalSchema();
    return copy;
}

AsyncLogicalOperator AsyncLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Async requires exactly one child");
    auto copy = *this;
    copy.child = std::move(children.front());
    return copy;
}

AsyncLogicalOperator AsyncLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = withChildrenUnsafe(std::move(children));
    copy.inferLocalSchema();
    return copy;
}

AsyncLogicalOperator AsyncLogicalOperator::withInferredSchema() const
{
    return withChildren({child.withInferredSchema()});
}

AsyncLogicalOperator AsyncLogicalOperator::withTraitSet(TraitSet traits) const
{
    auto copy = *this;
    copy.traitSet = std::move(traits);
    return copy;
}

Schema<Field, Unordered> AsyncLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Async schema has not been inferred");
    return bindToOperator(self.lock(), *outputSchema);
}

Schema<Field, Ordered> AsyncLogicalOperator::getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const
{
    auto fields = orderProvider(child) | RangeUnbinder{} | std::ranges::to<std::vector>();
    for (const auto& output : outputs)
    {
        fields.emplace_back(output.binding, output.field.getDataType());
    }
    auto schema = Schema<UnqualifiedUnboundField, Ordered>::tryCreateCollisionFree(std::move(fields));
    if (!schema)
    {
        throw CannotInferSchema(
            "Async output bindings collide: {}", Schema<UnqualifiedUnboundField, Ordered>::createCollisionString(schema.error()));
    }
    return bindToOperator(self.lock(), std::move(schema).value());
}

std::string AsyncLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId) const
{
    const auto arguments = getArguments();
    std::string result = "ASYNC " + getOperatorName() + "(args: [";
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        if (i != 0U)
        {
            result += ", ";
        }
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) i is bounded by arguments.size()
        result += arguments[i].explain(verbosity);
    }
    result += "], outputs: [";
    for (size_t i = 0; i < outputs.size(); ++i)
    {
        if (i != 0U)
        {
            result += ", ";
        }
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) i is bounded by outputs.size()
        result += fmt::format("{} as {}", outputs[i].field.getFullyQualifiedName(), outputs[i].binding);
    }
    return result + "])";
}

Reflected Reflector<TypedLogicalOperator<AsyncLogicalOperator>>::operator()(
    const TypedLogicalOperator<AsyncLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedAsyncLogicalOperator{
            .operatorId = op.getId(),
            .implementation = op->getAsyncOperator(),
            .arguments = op->getArguments(),
            .outputs = op->getOutputBindings()});
}

Unreflector<TypedLogicalOperator<AsyncLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<AsyncLogicalOperator>
Unreflector<TypedLogicalOperator<AsyncLogicalOperator>>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto value = context.unreflect<detail::ReflectedAsyncLogicalOperator>(reflected);
    auto children = plan->getChildrenFor(value.operatorId, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("Async requires exactly one child");
    }
    std::vector<Identifier> bindings;
    bindings.reserve(value.outputs.size());
    for (const auto& output : value.outputs)
    {
        bindings.push_back(output.binding);
    }
    auto result = AsyncLogicalOperator::create(
        std::move(children.front()), std::move(value.implementation), std::move(value.arguments), std::move(bindings));
    if (result->getOutputBindings() != value.outputs)
    {
        throw CannotDeserialize("Async payload does not match the serialized output signature");
    }
    return result;
}
}

size_t std::hash<NES::AsyncLogicalOperator>::operator()(const NES::AsyncLogicalOperator& op) const noexcept
{
    size_t seed = std::hash<std::string>{}(op.getOperatorName());
    for (const auto& arg : op.getArguments())
    {
        seed = folly::hash::hash_combine(seed, NES::StructuralFunctionHash{}(arg));
    }
    for (const auto& output : op.getOutputBindings())
    {
        seed = folly::hash::hash_combine(seed, output.binding, output.field);
    }
    return seed;
}
