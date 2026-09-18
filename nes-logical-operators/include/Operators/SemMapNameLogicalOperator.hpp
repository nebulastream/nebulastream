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
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Reorderer.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Name-based SEM_MAP operator that holds a model name and the call-site input fields for
/// deferred model resolution. Schema inference requires the actual model; attempting
/// withInferredSchema/getOutputSchema/getOrderedOutputSchema trips a PRECONDITION. withChildren
/// stays functional so generic plan-rewriting rules can traverse past the operator before
/// SemMapResolutionRule resolves the model.
class SemMapNameLogicalOperator : public Reorderer, public ManagedByOperator
{
public:
    SemMapNameLogicalOperator(
        WeakLogicalOperator self,
        std::string modelName,
        std::vector<UnqualifiedUnboundField> callSiteInputs,
        std::optional<Identifier> outputAlias = std::nullopt);
    SemMapNameLogicalOperator(
        WeakLogicalOperator self,
        std::string modelName,
        std::vector<UnqualifiedUnboundField> callSiteInputs,
        LogicalOperator child,
        std::optional<Identifier> outputAlias = std::nullopt);

    [[nodiscard]] std::string getModelName() const;
    [[nodiscard]] const std::vector<UnqualifiedUnboundField>& getCallSiteInputs() const;
    [[nodiscard]] const std::optional<Identifier>& getOutputAlias() const;

    [[nodiscard]] bool operator==(const SemMapNameLogicalOperator& rhs) const;

    [[nodiscard]] SemMapNameLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] SemMapNameLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] SemMapNameLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] std::string_view getName() const noexcept;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] SemMapNameLogicalOperator withInferredSchema() const;

    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;

private:
    static constexpr std::string_view NAME = "SemMapName";
    std::string modelName;
    std::vector<UnqualifiedUnboundField> callSiteInputs;
    std::optional<Identifier> outputAlias;

    std::optional<LogicalOperator> child;
    TraitSet traitSet;
    /// Schema inference for this placeholder always trips a PRECONDITION — stored as unbound for
    /// layout parity with SemMapLogicalOperator.
    Schema<UnqualifiedUnboundField, Unordered> outputSchema;
};

template <>
struct Reflector<TypedLogicalOperator<SemMapNameLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<SemMapNameLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<SemMapNameLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<SemMapNameLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<SemMapNameLogicalOperator>);

}

namespace NES::detail
{
struct ReflectedSemMapNameLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    std::optional<std::string> modelName;
    std::optional<std::vector<UnqualifiedUnboundField>> callSiteInputs;
    std::optional<Identifier> outputAlias;
};
}

template <>
struct std::hash<NES::SemMapNameLogicalOperator>
{
    size_t operator()(const NES::SemMapNameLogicalOperator& op) const noexcept;
};
