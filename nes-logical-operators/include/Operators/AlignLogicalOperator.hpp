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
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>

namespace NES
{

using AlignTimeCharacteristic = std::variant<std::array<Windowing::UnboundTimeCharacteristic, 2>, std::array<Windowing::BoundTimeCharacteristic, 2>>;

class AlignLogicalOperator final : public OriginIdAssigner, public ManagedByOperator
{
public:
    enum class AlignStrategy : uint8_t
    {
        LE,
        EagerLE,
        NN,
        FullMatch
    };

    explicit AlignLogicalOperator(WeakLogicalOperator self, AlignStrategy strategy, AlignTimeCharacteristic timestampFields);

    explicit AlignLogicalOperator(
        WeakLogicalOperator self, std::array<LogicalOperator, 2> children, AlignStrategy strategy, AlignTimeCharacteristic timestampFields);

    static TypedLogicalOperator<AlignLogicalOperator> create(AlignStrategy strategy, AlignTimeCharacteristic timestampFields);

    static TypedLogicalOperator<AlignLogicalOperator>
    create(std::array<LogicalOperator, 2> children, AlignStrategy strategy, AlignTimeCharacteristic timestampFields);

    [[nodiscard]] AlignStrategy getAlignStrategy() const;
    [[nodiscard]] AlignTimeCharacteristic getTimestampFields() const;

    [[nodiscard]] bool operator==(const AlignLogicalOperator& rhs) const;

    [[nodiscard]] AlignLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;
    [[nodiscard]] AlignLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] AlignLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] std::array<LogicalOperator, 2> getBothChildren() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] AlignLogicalOperator withInferredSchema() const;

private:
    friend struct detail::ErasedLogicalOperator;

    static constexpr std::string_view NAME = "Align";

    AlignStrategy strategy;
    std::optional<std::array<LogicalOperator, 2>> children;
    AlignTimeCharacteristic timestampFields;

    void inferLocalSchema();
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;
    friend struct std::hash<AlignLogicalOperator>;
};

static_assert(LogicalOperatorConcept<AlignLogicalOperator>);

template <>
struct Reflector<TypedLogicalOperator<AlignLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<AlignLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<AlignLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<AlignLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

namespace detail
{
struct ReflectedAlignLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    AlignLogicalOperator::AlignStrategy strategy = AlignLogicalOperator::AlignStrategy::NN;
    AlignTimeCharacteristic timestampFields;
};
}
}

template <>
struct std::hash<NES::AlignLogicalOperator>
{
    std::size_t operator()(const NES::AlignLogicalOperator& alignLogicalOperator) const noexcept;
};
