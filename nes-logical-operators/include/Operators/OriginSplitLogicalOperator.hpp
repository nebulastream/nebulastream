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

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Heads one branch of a fan-out point and gives it an identity of its own: the branch stamps the records it forwards
/// with origin ids that no other branch carries. Operators that merge branches again, and network channels relaying
/// one shared operator, therefore never see two streams under one origin id.
///
/// The operator only marks where the identity changes; which ids the branch stamps is decided by the origin id
/// inference rule and carried in the OriginMappingTrait, like every other decision the optimizer records.
class OriginSplitLogicalOperator : public ManagedByOperator
{
public:
    OriginSplitLogicalOperator(WeakLogicalOperator self, LogicalOperator child);

    static TypedLogicalOperator<OriginSplitLogicalOperator> create(LogicalOperator child);

    [[nodiscard]] bool operator==(const OriginSplitLogicalOperator& rhs) const;

    [[nodiscard]] OriginSplitLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] OriginSplitLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] OriginSplitLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] OriginSplitLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "OriginSplit";
    std::optional<LogicalOperator> child;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;
    friend struct std::hash<OriginSplitLogicalOperator>;
};

namespace detail
{
struct ReflectedOriginSplitLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
};
}

template <>
struct Reflector<TypedLogicalOperator<OriginSplitLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<OriginSplitLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<OriginSplitLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<OriginSplitLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<OriginSplitLogicalOperator>);
}

template <>
struct std::hash<NES::OriginSplitLogicalOperator>
{
    uint64_t operator()(const NES::OriginSplitLogicalOperator& op) const noexcept;
};
