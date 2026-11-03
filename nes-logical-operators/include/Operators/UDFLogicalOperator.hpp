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
#include <utility>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Functions/LogicalFunction.hpp>
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
#include <UdfDescriptor.hpp>

namespace NES
{

/// Runs UDFs that do not execute inside the query pipeline and appends their results to the child's fields. Created by
/// UDFLiftingRule. All calls share one execution and one bridge library, so that they can run as one unit per row. Their
/// arguments are fields of the child or constants, and they do not read each other's results.
class UDFLogicalOperator : public Reorderer, public ManagedByOperator
{
public:
    /// The name of the result field and the resolved UDF call computing it.
    using NamedCall = std::pair<Identifier, LogicalFunction>;

    UDFLogicalOperator(WeakLogicalOperator self, LogicalOperator child, std::vector<NamedCall> calls);

    static TypedLogicalOperator<UDFLogicalOperator> create(LogicalOperator child, std::vector<NamedCall> calls);

    [[nodiscard]] const std::vector<NamedCall>& getCalls() const;
    [[nodiscard]] UdfExecution getExecution() const;

    [[nodiscard]] bool operator==(const UDFLogicalOperator& rhs) const;

    [[nodiscard]] UDFLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] UDFLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] UDFLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId opId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] UDFLogicalOperator withInferredSchema() const;

    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;

private:
    void inferLocalSchema();

    static constexpr std::string_view NAME = "UDF";

    LogicalOperator child;
    std::vector<NamedCall> calls;
    TraitSet traitSet;
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    friend struct std::hash<UDFLogicalOperator>;
};

template <>
struct Reflector<TypedLogicalOperator<UDFLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<UDFLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<UDFLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<UDFLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<UDFLogicalOperator>);

}

namespace NES::detail
{
struct ReflectedUDFLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    std::vector<UDFLogicalOperator::NamedCall> calls;
};
}

template <>
struct std::hash<NES::UDFLogicalOperator>
{
    size_t operator()(const NES::UDFLogicalOperator& op) const noexcept;
};
