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
#include <SemanticModelCatalog.hpp>

namespace NES
{

/// Semantic filter operator: asks an LLM whether each record satisfies the model's
/// condition(s) and drops the records it does not affirm. The child's schema passes
/// through unchanged; only a fused operator (`SemanticFusionRule`) whose step list also
/// holds MAP steps appends one column per MAP step, exactly as SEM_MAP would.
///
/// Like `SemanticMapLogicalOperator` it holds a `RegisteredSemanticModel`; the API key
/// is resolved worker-side during lowering (`LowerToPhysicalSemanticFilter`). It does
/// not assign origin ids: dropping records keeps the stream's identity.
class SemanticFilterLogicalOperator : public Reorderer, public ManagedByOperator
{
public:
    SemanticFilterLogicalOperator(WeakLogicalOperator self, RegisteredSemanticModel model);
    SemanticFilterLogicalOperator(WeakLogicalOperator self, RegisteredSemanticModel model, LogicalOperator child);

    [[nodiscard]] const RegisteredSemanticModel& getModel() const;

    [[nodiscard]] bool operator==(const SemanticFilterLogicalOperator& rhs) const;

    [[nodiscard]] SemanticFilterLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] SemanticFilterLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] SemanticFilterLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] std::string_view getName() const noexcept;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] SemanticFilterLogicalOperator withInferredSchema() const;

    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;

private:
    void inferLocalSchema();

    static constexpr std::string_view NAME = "SemanticFilter";
    RegisteredSemanticModel model;

    std::optional<LogicalOperator> child;
    TraitSet traitSet;
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
};

template <>
struct Reflector<TypedLogicalOperator<SemanticFilterLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<SemanticFilterLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<SemanticFilterLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<SemanticFilterLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<SemanticFilterLogicalOperator>);

}

namespace NES::detail
{
struct ReflectedSemanticFilterLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    Reflected model;
};
}

template <>
struct std::hash<NES::SemanticFilterLogicalOperator>
{
    size_t operator()(const NES::SemanticFilterLogicalOperator& op) const noexcept;
};
