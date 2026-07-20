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

#include <array>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Evaluates an equality-ANY quantified subquery for every tuple of the left
/// child and appends its nullable Boolean result. The right child is the
/// relational subquery. Empty correlationFields identify the uncorrelated form
/// that can be lowered to the growing-table physical implementation.
class MarkApplyLogicalOperator final : public OriginIdAssigner, public ManagedByOperator
{
public:
    explicit MarkApplyLogicalOperator(
        WeakLogicalOperator self,
        std::vector<LogicalFunction> probeValues,
        Identifier markField,
        std::vector<Identifier> correlationFields);

    explicit MarkApplyLogicalOperator(
        WeakLogicalOperator self,
        std::array<LogicalOperator, 2> children,
        std::vector<LogicalFunction> probeValues,
        Identifier markField,
        std::vector<Identifier> correlationFields);

    static TypedLogicalOperator<MarkApplyLogicalOperator> create(
        std::vector<LogicalFunction> probeValues,
        Identifier markField,
        std::vector<Identifier> correlationFields = {});

    static TypedLogicalOperator<MarkApplyLogicalOperator> create(
        std::array<LogicalOperator, 2> children,
        std::vector<LogicalFunction> probeValues,
        Identifier markField,
        std::vector<Identifier> correlationFields = {});

    [[nodiscard]] const std::vector<LogicalFunction>& getProbeValues() const;
    [[nodiscard]] LogicalFunction getComparisonFunction() const;
    [[nodiscard]] Identifier getMarkField() const;
    [[nodiscard]] const std::vector<Identifier>& getCorrelationFields() const;
    [[nodiscard]] std::array<LogicalOperator, 2> getBothChildren() const;

    [[nodiscard]] bool operator==(const MarkApplyLogicalOperator& rhs) const;
    [[nodiscard]] MarkApplyLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;
    [[nodiscard]] MarkApplyLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] MarkApplyLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] MarkApplyLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "MarkApply";

    void inferLocalSchema();

    std::optional<std::array<LogicalOperator, 2>> children;
    std::vector<LogicalFunction> probeValues;
    Identifier markField;
    std::vector<Identifier> correlationFields;
    std::vector<Identifier> tableFieldNames;
    std::optional<LogicalFunction> comparisonFunction;
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
    TraitSet traitSet;

    friend struct std::hash<MarkApplyLogicalOperator>;
    friend struct Reflector<TypedLogicalOperator<MarkApplyLogicalOperator>>;
};

static_assert(LogicalOperatorConcept<MarkApplyLogicalOperator>);

template <>
struct Reflector<TypedLogicalOperator<MarkApplyLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<MarkApplyLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<MarkApplyLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<MarkApplyLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;

private:
    ContextType plan;
};

namespace detail
{
struct ReflectedMarkApplyLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    std::vector<LogicalFunction> probeValues;
    Identifier markField;
    std::vector<Identifier> correlationFields;
};
}

}

template <>
struct std::hash<NES::MarkApplyLogicalOperator>
{
    std::size_t operator()(const NES::MarkApplyLogicalOperator& op) const noexcept;
};
