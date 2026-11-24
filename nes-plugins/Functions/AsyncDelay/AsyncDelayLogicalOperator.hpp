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
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/AsynchronousOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Reorderer.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>

namespace NES
{
struct AsyncOperatorLowering;
struct OperatorFunctionRegistryEntry;

class AsyncDelayLogicalOperator final : public Reorderer, public ManagedByOperator, public AsynchronousOperator
{
public:
    AsyncDelayLogicalOperator(
        WeakLogicalOperator self,
        LogicalOperator child,
        LogicalFunction value,
        LogicalFunction millis,
        std::optional<LogicalFunction> maxConcurrency);
    [[nodiscard]] static TypedLogicalOperator<AsyncDelayLogicalOperator>
    create(LogicalOperator child, LogicalFunction value, LogicalFunction millis);
    [[nodiscard]] static TypedLogicalOperator<AsyncDelayLogicalOperator>
    create(LogicalOperator child, LogicalFunction value, LogicalFunction millis, std::optional<LogicalFunction> maxConcurrency);
    [[nodiscard]] static AsyncOperatorLowering lowerToAsync(LogicalOperator op);
    [[nodiscard]] static OperatorFunctionRegistryEntry functionSyntax();
    [[nodiscard]] static std::optional<size_t> getConcurrencyLimit(const std::optional<LogicalFunction>& maxConcurrency);

    [[nodiscard]] std::optional<size_t> getConcurrencyLimit() const { return getConcurrencyLimit(maxConcurrency); }

    [[nodiscard]] std::string_view getName() const noexcept { return "AsyncDelay"; }

    [[nodiscard]] const detail::DynamicBase* getDynamicBase() const
    {
        return static_cast<const detail::DynamicBase*>(static_cast<const Castable<AsynchronousOperator>*>(this));
    }

    [[nodiscard]] const LogicalFunction& getValue() const { return value; }

    [[nodiscard]] const LogicalFunction& getMillis() const { return millis; }

    [[nodiscard]] const std::optional<LogicalFunction>& getMaxConcurrency() const { return maxConcurrency; }

    [[nodiscard]] LogicalOperator getChild() const { return child; }

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const { return {child}; }

    [[nodiscard]] TraitSet getTraitSet() const { return traitSet; }

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;
    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    [[nodiscard]] bool operator==(const AsyncDelayLogicalOperator& other) const;
    [[nodiscard]] AsyncDelayLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] AsyncDelayLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] AsyncDelayLogicalOperator withInferredSchema() const;
    [[nodiscard]] AsyncDelayLogicalOperator withTraitSet(TraitSet traits) const;

private:
    void inferLocalSchema();
    void validate() const;
    LogicalOperator child;
    LogicalFunction value;
    LogicalFunction millis;
    std::optional<LogicalFunction> maxConcurrency;
    TraitSet traitSet;
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
};

namespace detail
{
struct ReflectedAsyncDelayLogicalOperator
{
    OperatorId operatorId;
    LogicalFunction value;
    LogicalFunction millis;
    std::optional<LogicalFunction> maxConcurrency;
};
}

template <>
struct Reflector<TypedLogicalOperator<AsyncDelayLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<AsyncDelayLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<AsyncDelayLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<AsyncDelayLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<AsyncDelayLogicalOperator>);
}

template <>
struct std::hash<NES::AsyncDelayLogicalOperator>
{
    size_t operator()(const NES::AsyncDelayLogicalOperator& op) const noexcept;
};
