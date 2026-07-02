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

namespace NES
{

/// Name-based ML inference operator that holds a model name string for deferred model resolution.
/// Schema inference requires the actual model; attempting withInferredSchema throws CannotInferSchema.
class InferModelNameLogicalOperator : public Reorderer, public ManagedByOperator
{
public:
    explicit InferModelNameLogicalOperator(WeakLogicalOperator self, std::string modelName);
    InferModelNameLogicalOperator(WeakLogicalOperator self, std::string modelName, LogicalOperator child);

    [[nodiscard]] std::string getModelName() const;

    [[nodiscard]] bool operator==(const InferModelNameLogicalOperator& rhs) const;

    [[nodiscard]] InferModelNameLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] InferModelNameLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] InferModelNameLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] std::string_view getName() const noexcept;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] InferModelNameLogicalOperator withInferredSchema() const;

    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;

private:
    static constexpr std::string_view NAME = "InferModelName";
    std::string modelName;

    std::optional<LogicalOperator> child;
    TraitSet traitSet;
    /// Schema inference for this placeholder always throws — stored as unbound for layout parity with InferModelLogicalOperator.
    Schema<UnqualifiedUnboundField, Unordered> outputSchema;
};

template <>
struct Reflector<TypedLogicalOperator<InferModelNameLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<InferModelNameLogicalOperator>& op) const;
};

template <>
struct Unreflector<TypedLogicalOperator<InferModelNameLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<InferModelNameLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<InferModelNameLogicalOperator>);

}

namespace NES::detail
{
struct ReflectedInferModelNameLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    std::optional<std::string> modelName;
};
}

template <>
struct std::hash<NES::InferModelNameLogicalOperator>
{
    size_t operator()(const NES::InferModelNameLogicalOperator& op) const noexcept;
};
