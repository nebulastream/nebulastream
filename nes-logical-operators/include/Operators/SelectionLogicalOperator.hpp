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

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Selection operator, which contains an function as a predicate.
class SelectionLogicalOperator : public ManagedByOperator
{
public:
    explicit SelectionLogicalOperator(WeakLogicalOperator self, LogicalFunction predicate);
    SelectionLogicalOperator(WeakLogicalOperator self, LogicalOperator child, LogicalFunction predicate);

    [[nodiscard]] LogicalFunction getPredicate() const;

    [[nodiscard]] bool operator==(const SelectionLogicalOperator& rhs) const;

    [[nodiscard]] SelectionLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] SelectionLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] SelectionLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "Selection";
    std::optional<LogicalOperator> child;
    LogicalFunction predicate;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;
    friend struct std::hash<SelectionLogicalOperator>;
};

namespace detail
{
struct ReflectedSelectionLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    LogicalFunction predicate;
};
}

template <>
struct Reflector<TypedLogicalOperator<SelectionLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<SelectionLogicalOperator>& op) const;
};

template <>
struct Unreflector<TypedLogicalOperator<SelectionLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<SelectionLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<SelectionLogicalOperator>);
}

template <>
struct std::hash<NES::SelectionLogicalOperator>
{
    uint64_t operator()(const NES::SelectionLogicalOperator& op) const noexcept;
};
