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
#include <ModelCatalog.hpp>

namespace NES
{

/// ML inference operator that evaluates a model over the child's matching output
/// fields and appends the model's output fields to the output schema. Holds a
/// `RegisteredModel` — which bundles the imported model and the validated
/// field schema so the two cannot drift. Compilation to executable bytecode is
/// deferred to worker-side lowering (`LowerToPhysicalInferModel`).
class InferModelLogicalOperator : public Reorderer, public ManagedByOperator
{
public:
    InferModelLogicalOperator(WeakLogicalOperator self, RegisteredModel model);
    InferModelLogicalOperator(WeakLogicalOperator self, RegisteredModel model, LogicalOperator child);

    [[nodiscard]] const RegisteredModel& getModel() const;

    [[nodiscard]] bool operator==(const InferModelLogicalOperator& rhs) const;

    [[nodiscard]] InferModelLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] InferModelLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] InferModelLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] std::string_view getName() const noexcept;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] InferModelLogicalOperator withInferredSchema() const;

    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;

private:
    void inferLocalSchema();

    static constexpr std::string_view NAME = "InferModel";
    RegisteredModel model;

    std::optional<LogicalOperator> child;
    TraitSet traitSet;
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
};

template <>
struct Reflector<TypedLogicalOperator<InferModelLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<InferModelLogicalOperator>& op) const;
};

template <>
struct Unreflector<TypedLogicalOperator<InferModelLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<InferModelLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<InferModelLogicalOperator>);

}

namespace NES::detail
{
struct ReflectedInferModelLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    Reflected model;
};
}

template <>
struct std::hash<NES::InferModelLogicalOperator>
{
    size_t operator()(const NES::InferModelLogicalOperator& op) const noexcept;
};
