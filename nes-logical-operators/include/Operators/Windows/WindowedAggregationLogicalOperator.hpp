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

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Reorderer.hpp>
#include <Operators/Reprojecter.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Serialization/WindowAggregationLogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/DynamicBase.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <Util/Variant.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{


class WindowedAggregationLogicalOperator final : public OriginIdAssigner, public Reprojecter, public Reorderer, public ManagedByOperator
{
public:
    struct ProjectedAggregation
    {
        WindowAggregationLogicalFunction function;
        Identifier name;

        friend bool operator==(const ProjectedAggregation& lhs, const ProjectedAggregation& rhs);
    };

    using GroupingKeyType = NES::VariantContainer<
        std::vector,
        std::pair<TypedLogicalFunction<UnboundFieldAccessLogicalFunction>, std::optional<Identifier>>,
        std::pair<TypedLogicalFunction<FieldAccessLogicalFunction>, std::optional<Identifier>>>;

    WindowedAggregationLogicalOperator(
        WeakLogicalOperator self,
        GroupingKeyType groupingKey,
        std::vector<ProjectedAggregation> aggregationFunctions,
        Windowing::TimeBasedWindowType windowType,
        Windowing::TimeCharacteristic timeCharacteristic);

    WindowedAggregationLogicalOperator(
        WeakLogicalOperator self,
        LogicalOperator child,
        GroupingKeyType groupingKey,
        std::vector<ProjectedAggregation> aggregationFunctions,
        Windowing::TimeBasedWindowType windowType,
        Windowing::TimeCharacteristic timeCharacteristic);

    [[nodiscard]] bool isKeyed() const;


    [[nodiscard]] std::vector<ProjectedAggregation> getWindowAggregation() const;

    [[nodiscard]] NES::VariantContainer<
        std::vector,
        TypedLogicalFunction<UnboundFieldAccessLogicalFunction>,
        TypedLogicalFunction<FieldAccessLogicalFunction>>
    getGroupingKeys() const;
    [[nodiscard]] std::unordered_map<Field, std::unordered_set<Field>> getAccessedFieldsForOutput() const override;
    [[nodiscard]] const GroupingKeyType& getGroupingKeysWithName() const;

    [[nodiscard]] Windowing::TimeBasedWindowType getWindowType() const;
    [[nodiscard]] const UnqualifiedUnboundField& getWindowStartField() const;
    [[nodiscard]] const UnqualifiedUnboundField& getWindowEndField() const;
    [[nodiscard]] std::variant<Windowing::UnboundTimeCharacteristic, Windowing::BoundTimeCharacteristic> getCharacteristic() const;


    [[nodiscard]] bool operator==(const WindowedAggregationLogicalOperator& rhs) const;

    [[nodiscard]] WindowedAggregationLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] WindowedAggregationLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] WindowedAggregationLogicalOperator withInferredSchema() const;
    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;
    [[nodiscard]] const DynamicBase* getDynamicBase() const;

private:
    static constexpr std::string_view NAME = "WindowedAggregation";

    std::optional<LogicalOperator> child;
    Windowing::TimeBasedWindowType windowType;
    GroupingKeyType groupingKeys;
    std::vector<ProjectedAggregation> aggregationFunctions;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
    Windowing::TimeCharacteristic timestampField;

    std::array<UnqualifiedUnboundField, 2> startEndFields = std::array{
        UnqualifiedUnboundField{Identifier::parse("start"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("end"), DataType::Type::UINT64}};

    TraitSet traitSet;

    friend struct std::hash<WindowedAggregationLogicalOperator>;

    friend Reflector<TypedLogicalOperator<WindowedAggregationLogicalOperator>>;
};

template <>
struct Reflector<TypedLogicalOperator<WindowedAggregationLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<WindowedAggregationLogicalOperator>& op) const;
};

template <>
struct Unreflector<TypedLogicalOperator<WindowedAggregationLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<WindowedAggregationLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<WindowedAggregationLogicalOperator>);

}

template <>
struct std::hash<NES::WindowedAggregationLogicalOperator>
{
    std::size_t operator()(const NES::WindowedAggregationLogicalOperator& windowedAggregationLogicalOperator) const noexcept;
};

template <>
struct std::hash<NES::WindowedAggregationLogicalOperator::ProjectedAggregation>
{
    std::size_t operator()(const NES::WindowedAggregationLogicalOperator::ProjectedAggregation& aggregation) const noexcept;
};

namespace NES::detail
{
struct ReflectedWindowAggregationLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    Windowing::TimeBasedWindowType windowType;
    WindowedAggregationLogicalOperator::GroupingKeyType groupingKeys;
    std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation> aggregations;
    Windowing::TimeCharacteristic timestampField;
};
}
