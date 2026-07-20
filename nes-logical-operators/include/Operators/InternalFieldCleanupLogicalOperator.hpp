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

#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Reorderer.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Removes optimizer-generated fields while preserving every other field and
/// its relative order. It is deliberately independent of the operator that
/// introduced or consumed those fields.
class InternalFieldCleanupLogicalOperator final : public Reorderer, public ManagedByOperator
{
public:
    explicit InternalFieldCleanupLogicalOperator(WeakLogicalOperator self, std::vector<Identifier> internalFields);
    explicit InternalFieldCleanupLogicalOperator(
        WeakLogicalOperator self, LogicalOperator child, std::vector<Identifier> internalFields);

    static TypedLogicalOperator<InternalFieldCleanupLogicalOperator> create(std::vector<Identifier> internalFields);
    static TypedLogicalOperator<InternalFieldCleanupLogicalOperator>
    create(LogicalOperator child, std::vector<Identifier> internalFields);

    [[nodiscard]] const std::vector<Identifier>& getInternalFields() const;
    [[nodiscard]] LogicalOperator getChild() const;
    [[nodiscard]] bool operator==(const InternalFieldCleanupLogicalOperator& rhs) const;
    [[nodiscard]] InternalFieldCleanupLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;
    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;
    [[nodiscard]] InternalFieldCleanupLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] InternalFieldCleanupLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] InternalFieldCleanupLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "InternalFieldCleanup";
    void inferLocalSchema();

    std::optional<LogicalOperator> child;
    std::vector<Identifier> internalFields;
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
    TraitSet traitSet;

    friend struct std::hash<InternalFieldCleanupLogicalOperator>;
    friend struct Reflector<TypedLogicalOperator<InternalFieldCleanupLogicalOperator>>;
};

static_assert(LogicalOperatorConcept<InternalFieldCleanupLogicalOperator>);

template <>
struct Reflector<TypedLogicalOperator<InternalFieldCleanupLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<InternalFieldCleanupLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<InternalFieldCleanupLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<InternalFieldCleanupLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;

private:
    ContextType plan;
};

namespace detail
{
struct ReflectedInternalFieldCleanupLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    std::vector<Identifier> internalFields;
};
}

}

template <>
struct std::hash<NES::InternalFieldCleanupLogicalOperator>
{
    std::size_t operator()(const NES::InternalFieldCleanupLogicalOperator& op) const noexcept;
};
