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

/// A 1→N operator: every input row whose VARSIZED field `input` holds a JSON array of N elements becomes N output rows,
/// each with the child's fields copied and one element in the new VARSIZED field `output`. A string element is its
/// unescaped text, any other element its compact JSON text. An empty array yields no row; anything that is not a JSON
/// array fails the query at runtime. The physical operator calls its child once per element, and the emit spills into
/// as many output buffers as needed, so no pipelining change is involved.
///
/// SQL writes it as `SELECT …, UNNEST(arr) AS item FROM …`; `UnnestTranslationRule` turns the placeholder function into
/// this operator below the projection.
class UnnestLogicalOperator : public Reorderer, public ManagedByOperator
{
public:
    UnnestLogicalOperator(WeakLogicalOperator self, Identifier input, Identifier output);
    UnnestLogicalOperator(WeakLogicalOperator self, LogicalOperator child, Identifier input, Identifier output);

    static TypedLogicalOperator<UnnestLogicalOperator> create(LogicalOperator child, Identifier input, Identifier output);

    [[nodiscard]] const Identifier& getInput() const;
    [[nodiscard]] const Identifier& getOutput() const;

    [[nodiscard]] bool operator==(const UnnestLogicalOperator& rhs) const;

    [[nodiscard]] UnnestLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] UnnestLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] UnnestLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] UnnestLogicalOperator withInferredSchema() const;
    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;

private:
    void inferLocalSchema();

    static constexpr std::string_view NAME = "Unnest";
    Identifier input;
    Identifier output;

    std::optional<LogicalOperator> child;
    TraitSet traitSet;
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
};

template <>
struct Reflector<TypedLogicalOperator<UnnestLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<UnnestLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<UnnestLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<UnnestLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<UnnestLogicalOperator>);

}

namespace NES::detail
{
struct ReflectedUnnestLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    Reflected input;
    Reflected output;
};
}

template <>
struct std::hash<NES::UnnestLogicalOperator>
{
    size_t operator()(const NES::UnnestLogicalOperator& op) const noexcept;
};
