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

#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/UnboundField.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/AsynchronousOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Reorderer.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>

namespace NES
{
struct AsyncOperatorLowering;

/// Actual unary async invocation. Instance identity is pointer equality via TypedLogicalOperator.
/// NOLINTNEXTLINE(misc-multiple-inheritance) logical operators combine ordering and managed-identity mixins
class AsyncLogicalOperator final : public Reorderer, public ManagedByOperator, public AsynchronousOperator
{
public:
    AsyncLogicalOperator(
        WeakLogicalOperator self, LogicalOperator child, AsyncOperator implementation, std::vector<LogicalFunction> arguments);
    [[nodiscard]] static TypedLogicalOperator<AsyncLogicalOperator>
    create(LogicalOperator child, const std::string& operatorName, std::vector<LogicalFunction> args);

    [[nodiscard]] static TypedLogicalOperator<AsyncLogicalOperator>
    create(LogicalOperator child, AsyncOperator implementation, std::vector<LogicalFunction> arguments);

    [[nodiscard]] static AsyncOperatorLowering lowerToAsync(LogicalOperator op);

    /// Disambiguate the DynamicBase view used by the type-erased logical operator model.
    [[nodiscard]] const DynamicBase* getDynamicBase() const
    {
        return static_cast<const DynamicBase*>(static_cast<const Castable<AsynchronousOperator>*>(this));
    }

    [[nodiscard]] const AsyncOperator& getAsyncOperator() const { return concreteAsyncImpl; }

    [[nodiscard]] LogicalOperator getChild() const { return child; }

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const { return {child}; }

    [[nodiscard]] const std::vector<LogicalFunction>& getArguments() const { return arguments; }

    // TODO remove just call getAsyncOperator getType
    [[nodiscard]] std::string getOperatorName() const { return concreteAsyncImpl.getType(); }

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) Required by the logical-operator interface.
    [[nodiscard]] std::string_view getName() const noexcept { return "Async"; }

    [[nodiscard]] bool operator==(const AsyncLogicalOperator& rhs) const;

    [[nodiscard]] AsyncLogicalOperator withArguments(std::vector<LogicalFunction> args) const;

    [[nodiscard]] AsyncLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] AsyncLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] AsyncLogicalOperator withInferredSchema() const;
    [[nodiscard]] AsyncLogicalOperator withTraitSet(TraitSet traits) const;

    [[nodiscard]] TraitSet getTraitSet() const { return traitSet; }

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;
    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;

private:
    void inferLocalSchema();

    LogicalOperator child;
    AsyncOperator concreteAsyncImpl;
    std::vector<LogicalFunction> arguments;
    TraitSet traitSet;
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
};

namespace detail
{
struct ReflectedAsyncLogicalOperator
{
    OperatorId operatorId;
    AsyncOperator implementation;
    std::vector<LogicalFunction> arguments;
};

[[nodiscard]] AsyncLogicalOperator
restoreAsyncLogicalOperator(const ReflectedAsyncLogicalOperator& value, std::vector<LogicalOperator> children);
}

template <>
struct Reflector<TypedLogicalOperator<AsyncLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<AsyncLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<AsyncLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<AsyncLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<AsyncLogicalOperator>);
}

template <>
struct std::hash<NES::AsyncLogicalOperator>
{
    size_t operator()(const NES::AsyncLogicalOperator& op) const noexcept;
};
