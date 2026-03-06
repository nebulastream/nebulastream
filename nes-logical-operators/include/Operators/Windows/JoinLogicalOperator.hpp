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

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Util/Variant.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include "WindowTypes/Types/TimeBasedWindowType.hpp"

namespace NES
{
class SerializableOperator;

namespace detail
{
template <typename T>
using ArrayOfTwo = std::array<T, 2>;
}

using JoinTimeCharacteristic = NES::VariantContainerFrom<NES::detail::ArrayOfTwo, Windowing::TimeCharacteristic>;

class JoinLogicalOperator final : public OriginIdAssigner
{
public:
    enum class JoinType : uint8_t
    {
        INNER_JOIN,
        CARTESIAN_PRODUCT
    };

    explicit JoinLogicalOperator(
        WeakLogicalOperator self,
        LogicalFunction joinFunction,
        Windowing::TimeBasedWindowType windowType,
        JoinType joinType,
        JoinTimeCharacteristic timeCharacteristics);

    explicit JoinLogicalOperator(
        WeakLogicalOperator self,
        std::array<LogicalOperator, 2> children,
        LogicalFunction joinFunction,
        Windowing::TimeBasedWindowType windowType,
        JoinType joinType,
        JoinTimeCharacteristic timeCharacteristics);

    static std::optional<JoinTimeCharacteristic> createJoinTimeCharacteristic(
        std::array<std::variant<Windowing::UnboundTimeCharacteristic, Windowing::BoundTimeCharacteristic>, 2> timestampFields);

    [[nodiscard]] LogicalFunction getJoinFunction() const;
    [[nodiscard]] Windowing::TimeBasedWindowType getWindowType() const;
    [[nodiscard]] const UnqualifiedUnboundField& getStartField() const;
    [[nodiscard]] const UnqualifiedUnboundField& getEndField() const;
    [[nodiscard]] JoinTimeCharacteristic getJoinTimeCharacteristics() const;
    [[nodiscard]] JoinType getJoinType() const;


    [[nodiscard]] bool operator==(const JoinLogicalOperator& rhs) const;

    [[nodiscard]] JoinLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;
    [[nodiscard]] JoinLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] std::array<LogicalOperator, 2> getBothChildren() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] JoinLogicalOperator withInferredSchema() const;

    WeakLogicalOperator self;
private:
    // template <LogicalOperatorConcept OperatorType>
    friend struct detail::ErasedLogicalOperator;

    static constexpr std::string_view NAME = "Join";

    Windowing::TimeBasedWindowType windowType;
    JoinType joinType;
    std::optional<std::array<LogicalOperator, 2>> children;
    LogicalFunction joinFunction;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
    JoinTimeCharacteristic timestampFields;

    std::array<UnqualifiedUnboundField, 2> startEndFields = std::array{
        UnqualifiedUnboundField{Identifier::parse("start"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("end"), DataType::Type::UINT64}};

    TraitSet traitSet;
    friend struct std::hash<JoinLogicalOperator>;
};

static_assert(LogicalOperatorConcept<JoinLogicalOperator>);

template <>
struct Reflector<TypedLogicalOperator<JoinLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<JoinLogicalOperator>& op) const;
};

template <>
struct Unreflector<TypedLogicalOperator<JoinLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<JoinLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

namespace detail
{
struct ReflectedJoinLogicalOperator
{
    OperatorId operatorId;
    LogicalFunction joinFunction;
    Windowing::TimeBasedWindowType windowType;
    JoinTimeCharacteristic timestampFields;
    JoinLogicalOperator::JoinType joinType = JoinLogicalOperator::JoinType::INNER_JOIN;
};
}
}

template <>
struct std::hash<NES::JoinLogicalOperator>
{
    std::size_t operator()(const NES::JoinLogicalOperator& joinLogicalOperator) const noexcept;
};

template <>
struct std::hash<NES::JoinTimeCharacteristic>
{
    std::size_t operator()(const NES::JoinTimeCharacteristic& joinTimeCharacteristic) const noexcept;
};
