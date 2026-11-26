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

#include <optional>
#include <string_view>
#include <vector>
#include <Functions/LogicalFunction.hpp>
#include <Operators/AsynchronousOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Reorderer.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>

namespace NES
{
class AsyncLoweringContext;
struct OperatorFunctionRegistryEntry;

class HttpCallLogicalOperator final : public Reorderer, public ManagedByOperator, public AsynchronousOperator
{
public:
    HttpCallLogicalOperator(WeakLogicalOperator self,
                            LogicalOperator child,
                            LogicalFunction body,
                            LogicalFunction url,
                            std::optional<LogicalFunction> timeoutMillis,
                            std::optional<LogicalFunction> retries);
    [[nodiscard]] static TypedLogicalOperator<HttpCallLogicalOperator>
    create(LogicalOperator child, LogicalFunction body, LogicalFunction url);
    [[nodiscard]] static TypedLogicalOperator<HttpCallLogicalOperator> create(LogicalOperator child,
                                                                              LogicalFunction body,
                                                                              LogicalFunction url,
                                                                              std::optional<LogicalFunction> timeoutMillis,
                                                                              std::optional<LogicalFunction> retries);
    [[nodiscard]] static AsyncOperator lowerToAsync(const LogicalOperator& op, AsyncLoweringContext& context);
    [[nodiscard]] static OperatorFunctionRegistryEntry functionSyntax();

    [[nodiscard]] std::string_view getName() const noexcept { return "HttpCall"; }

    [[nodiscard]] const detail::DynamicBase* getDynamicBase() const
    {
        return static_cast<const detail::DynamicBase*>(static_cast<const Castable<AsynchronousOperator>*>(this));
    }

    [[nodiscard]] const LogicalFunction& getBody() const { return body; }

    [[nodiscard]] const LogicalFunction& getUrl() const { return url; }

    [[nodiscard]] const std::optional<LogicalFunction>& getTimeoutMillis() const { return timeoutMillis; }

    [[nodiscard]] const std::optional<LogicalFunction>& getRetries() const { return retries; }

    [[nodiscard]] LogicalOperator getChild() const { return child; }

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const { return {child}; }

    [[nodiscard]] TraitSet getTraitSet() const { return traitSet; }

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;
    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    [[nodiscard]] bool operator==(const HttpCallLogicalOperator& other) const;
    [[nodiscard]] HttpCallLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] HttpCallLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] HttpCallLogicalOperator withInferredSchema() const;
    [[nodiscard]] HttpCallLogicalOperator withTraitSet(TraitSet traits) const;

private:
    void inferLocalSchema();
    [[nodiscard]] std::vector<LogicalFunction> functionArguments() const;
    LogicalOperator child;
    LogicalFunction body;
    LogicalFunction url;
    std::optional<LogicalFunction> timeoutMillis;
    std::optional<LogicalFunction> retries;
    TraitSet traitSet;
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
};

namespace detail
{
struct ReflectedHttpCallLogicalOperator
{
    OperatorId operatorId;
    LogicalFunction body;
    LogicalFunction url;
    std::optional<LogicalFunction> timeoutMillis;
    std::optional<LogicalFunction> retries;
};
}

template <>
struct Reflector<TypedLogicalOperator<HttpCallLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<HttpCallLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<HttpCallLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<HttpCallLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<HttpCallLogicalOperator>);
}

template <>
struct std::hash<NES::HttpCallLogicalOperator>
{
    size_t operator()(const NES::HttpCallLogicalOperator& op) const noexcept;
};
