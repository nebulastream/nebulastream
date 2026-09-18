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
#include <SemanticModelCatalog.hpp>

namespace NES
{

/// Minimal, data-carrier SEM_MAP operator (plan §M1). Unlike InferModel, the INPUT fields come
/// from the call site (`callSiteInputs`), not the catalog. `SemMapNameLogicalOperator` carries a
/// model name plus these call-site inputs for deferred resolution; `SemMapResolutionRule`
/// (plan §M2) swaps it for this operator once the name is looked up in `SemanticModelCatalog`.
class SemMapLogicalOperator : public Reorderer, public ManagedByOperator
{
public:
    SemMapLogicalOperator(
        WeakLogicalOperator self,
        RegisteredSemanticModel model,
        std::vector<UnqualifiedUnboundField> callSiteInputs,
        std::optional<Identifier> outputAlias = std::nullopt);
    SemMapLogicalOperator(
        WeakLogicalOperator self,
        RegisteredSemanticModel model,
        std::vector<UnqualifiedUnboundField> callSiteInputs,
        LogicalOperator child,
        std::optional<Identifier> outputAlias = std::nullopt);

    [[nodiscard]] const RegisteredSemanticModel& getModel() const;
    [[nodiscard]] const std::vector<UnqualifiedUnboundField>& getCallSiteInputs() const;
    [[nodiscard]] const std::optional<Identifier>& getOutputAlias() const;

    /// Returns the model's declared OUTPUT field(s), renamed to `outputAlias` when set. Throws
    /// CannotInferSchema if an alias is given but the model declares more than one OUTPUT field.
    /// Pure accessor; public so lowering can use the catalog OUTPUT names (prompt keys) and the
    /// alias (record field to write) as the distinct things they are (plan §M4).
    [[nodiscard]] std::vector<UnqualifiedUnboundField> resolvedModelOutputFields() const;

    [[nodiscard]] bool operator==(const SemMapLogicalOperator& rhs) const;

    [[nodiscard]] SemMapLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] SemMapLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] SemMapLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] std::string_view getName() const noexcept;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] SemMapLogicalOperator withInferredSchema() const;

    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;

private:
    void inferLocalSchema();

    static constexpr std::string_view NAME = "SemMap";
    RegisteredSemanticModel model;
    std::vector<UnqualifiedUnboundField> callSiteInputs;
    std::optional<Identifier> outputAlias;

    std::optional<LogicalOperator> child;
    TraitSet traitSet;
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
};

template <>
struct Reflector<TypedLogicalOperator<SemMapLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<SemMapLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<SemMapLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<SemMapLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<SemMapLogicalOperator>);

}

namespace NES::detail
{
struct ReflectedSemMapLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    Reflected model;
    std::optional<std::vector<UnqualifiedUnboundField>> callSiteInputs;
    std::optional<Identifier> outputAlias;
};
}

template <>
struct std::hash<NES::SemMapLogicalOperator>
{
    size_t operator()(const NES::SemMapLogicalOperator& op) const noexcept;
};
