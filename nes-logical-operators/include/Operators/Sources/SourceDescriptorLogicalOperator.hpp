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

#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Reorderer.hpp>
#include <Schema/Field.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/DynamicBase.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Is constructed when we apply the LogicalSourceExpansionRule. Stores the Descriptor of a (physical) source as a member.
/// During SQL parsing, we register (physical) source descriptors in the source catalog. Each currently must name exactly one logical source.
/// The logical source is then used as key to a multimap, with all descriptors that name the logical source as values.
/// In the LogicalSourceExpansionRule, we take the keys from SourceNameLogicalOperator operators, get all corresponding (physical) source
/// descriptors from the catalog, construct SourceDescriptorLogicalOperators from the descriptors and attach them to the query plan.
class SourceDescriptorLogicalOperator final : public OriginIdAssigner, public Reorderer
{
public:
    explicit SourceDescriptorLogicalOperator(WeakLogicalOperator self, SourceDescriptor sourceDescriptor);

    [[nodiscard]] SourceDescriptor getSourceDescriptor() const;

    [[nodiscard]] bool operator==(const SourceDescriptorLogicalOperator& rhs) const;

    [[nodiscard]] SourceDescriptorLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] SourceDescriptorLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] SourceDescriptorLogicalOperator withInferredSchema() const;
    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;
    [[nodiscard]] const DynamicBase* getDynamicBase() const;

    WeakLogicalOperator self;

private:
    static constexpr std::string_view NAME = "SourceDescriptor";

    std::vector<LogicalOperator> children;
    SourceDescriptor sourceDescriptor;

    void inferLocalSchema();
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;
    friend struct std::hash<SourceDescriptorLogicalOperator>;
};

namespace detail
{
struct ReflectedSourceDescriptorLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    SourceDescriptor sourceDescriptor;
};
}

template <>
struct Reflector<TypedLogicalOperator<SourceDescriptorLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<SourceDescriptorLogicalOperator>& op) const;
};

template <>
struct Unreflector<TypedLogicalOperator<SourceDescriptorLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<SourceDescriptorLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<SourceDescriptorLogicalOperator>);


}

template <>
struct std::hash<NES::SourceDescriptorLogicalOperator>
{
    std::size_t operator()(const NES::SourceDescriptorLogicalOperator& sourceDescriptorLogicalOperator) const;
};
