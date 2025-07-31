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
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Reorderer.hpp>
#include <Schema/Field.hpp>
#include <Util/DynamicBase.hpp>

namespace NES
{

class UnionLogicalOperator : public Reorderer
{
public:
    explicit UnionLogicalOperator(WeakLogicalOperator self);
    explicit UnionLogicalOperator(WeakLogicalOperator self, std::vector<LogicalOperator> children);

    [[nodiscard]] bool operator==(const UnionLogicalOperator& rhs) const;

    [[nodiscard]] UnionLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] UnionLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    [[nodiscard]] UnionLogicalOperator withInferredSchema() const;
    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider childOrderProvider) const override;
    [[nodiscard]] const DynamicBase* getDynamicBase() const;

    WeakLogicalOperator self;

private:
    static constexpr std::string_view NAME = "Union";

    std::vector<LogicalOperator> children;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;
    friend struct std::hash<UnionLogicalOperator>;
};

template <>
struct Reflector<TypedLogicalOperator<UnionLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<UnionLogicalOperator>&) const;
};

template <>
struct Unreflector<TypedLogicalOperator<UnionLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<UnionLogicalOperator> operator()(const Reflected&, const ReflectionContext&) const;
};

static_assert(LogicalOperatorConcept<UnionLogicalOperator>);

}

template <>
struct std::hash<NES::UnionLogicalOperator>
{
    uint64_t operator()(const NES::UnionLogicalOperator& op) const noexcept;
};
