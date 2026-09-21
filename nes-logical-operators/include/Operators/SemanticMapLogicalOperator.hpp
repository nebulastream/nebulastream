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

/// Semantic map operator: asks an LLM to transform the child's matching output fields
/// and appends one column per semantic step to the output schema. Holds a
/// `RegisteredSemanticModel`, which bundles the endpoint configuration, the prompt
/// steps and the validated field schema so the three cannot drift apart. The API key
/// is not part of the entry; it is resolved worker-side during lowering
/// (`LowerToPhysicalSemanticMap`).
class SemanticMapLogicalOperator : public Reorderer, public ManagedByOperator
{
public:
    SemanticMapLogicalOperator(WeakLogicalOperator self, RegisteredSemanticModel model);
    SemanticMapLogicalOperator(WeakLogicalOperator self, RegisteredSemanticModel model, LogicalOperator child);

    [[nodiscard]] const RegisteredSemanticModel& getModel() const;

    [[nodiscard]] bool operator==(const SemanticMapLogicalOperator& rhs) const;

    [[nodiscard]] SemanticMapLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] SemanticMapLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] SemanticMapLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] std::string_view getName() const noexcept;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] SemanticMapLogicalOperator withInferredSchema() const;

    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;

private:
    void inferLocalSchema();

    static constexpr std::string_view NAME = "SemanticMap";
    RegisteredSemanticModel model;

    std::optional<LogicalOperator> child;
    TraitSet traitSet;
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
};

template <>
struct Reflector<TypedLogicalOperator<SemanticMapLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<SemanticMapLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<SemanticMapLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<SemanticMapLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<SemanticMapLogicalOperator>);

}

namespace NES::detail
{
struct ReflectedSemanticMapLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    Reflected model;
};
}

template <>
struct std::hash<NES::SemanticMapLogicalOperator>
{
    size_t operator()(const NES::SemanticMapLogicalOperator& op) const noexcept;
};
