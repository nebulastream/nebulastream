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
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <Util/Variant.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>

namespace NES
{

namespace detail
{
template <typename T>
using AsOfArrayOfTwo = std::array<T, 2>;
}

using AsOfJoinTimeCharacteristics = NES::VariantContainerFrom<NES::detail::AsOfArrayOfTwo, Windowing::TimeCharacteristic>;

/// A directional temporal join. The left side produces output and selects the
/// matching right tuple with the greatest timestamp not after the left tuple.
class AsOfJoinLogicalOperator final : public OriginIdAssigner, public ManagedByOperator
{
public:
    explicit AsOfJoinLogicalOperator(
        WeakLogicalOperator self, LogicalFunction joinFunction, AsOfJoinTimeCharacteristics timeCharacteristics, bool rightIsTable);

    explicit AsOfJoinLogicalOperator(
        WeakLogicalOperator self,
        std::array<LogicalOperator, 2> children,
        LogicalFunction joinFunction,
        AsOfJoinTimeCharacteristics timeCharacteristics,
        bool rightIsTable);

    static TypedLogicalOperator<AsOfJoinLogicalOperator>
    create(LogicalFunction joinFunction, AsOfJoinTimeCharacteristics timeCharacteristics, bool rightIsTable);

    static TypedLogicalOperator<AsOfJoinLogicalOperator> create(
        std::array<LogicalOperator, 2> children,
        LogicalFunction joinFunction,
        AsOfJoinTimeCharacteristics timeCharacteristics,
        bool rightIsTable);

    [[nodiscard]] LogicalFunction getJoinFunction() const;
    [[nodiscard]] AsOfJoinTimeCharacteristics getTimeCharacteristics() const;
    [[nodiscard]] bool isRightTable() const;
    [[nodiscard]] std::array<LogicalOperator, 2> getBothChildren() const;

    [[nodiscard]] bool operator==(const AsOfJoinLogicalOperator& rhs) const;
    [[nodiscard]] AsOfJoinLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;
    [[nodiscard]] AsOfJoinLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] AsOfJoinLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId id) const;
    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] AsOfJoinLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "AsOfJoin";

    void inferLocalSchema();

    std::optional<std::array<LogicalOperator, 2>> children;
    LogicalFunction joinFunction;
    AsOfJoinTimeCharacteristics timeCharacteristics;
    bool rightIsTable;
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
    TraitSet traitSet;

    friend struct std::hash<AsOfJoinLogicalOperator>;
    friend struct Reflector<TypedLogicalOperator<AsOfJoinLogicalOperator>>;
};

static_assert(LogicalOperatorConcept<AsOfJoinLogicalOperator>);

template <>
struct Reflector<TypedLogicalOperator<AsOfJoinLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<AsOfJoinLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<AsOfJoinLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<AsOfJoinLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;

private:
    ContextType plan;
};

namespace detail
{
struct ReflectedAsOfJoinLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    LogicalFunction joinFunction;
    AsOfJoinTimeCharacteristics timeCharacteristics;
    bool rightIsTable{false};
};
}

}

template <>
struct std::hash<NES::AsOfJoinLogicalOperator>
{
    std::size_t operator()(const NES::AsOfJoinLogicalOperator& op) const noexcept;
};
