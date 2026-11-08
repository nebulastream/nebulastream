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
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Reorderer.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/DynamicBase.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>

namespace NES
{

/// Keeps the first `limit` rows per partition key in the order of the sort keys, for SQL's
/// `ORDER BY sortKeys LIMIT limit [BY partitionKeys] WINDOW window`. Rows with equal sort keys keep their arrival order.
/// With a tumbling or sliding window, the operator emits the top rows of every key per window, with the window's start and end;
/// the output is append-only. With an UNBOUNDED window, the top rows change over time, so the operator emits a retract changelog
/// (see RowKind.hpp): +I for a row that enters the top rows of its key and -D for a row that leaves them, with the end of the slice
/// that emitted the change. Every output row carries all input fields.
class TopKLogicalOperator final : public OriginIdAssigner, public Reorderer, public ManagedByOperator
{
public:
    struct SortKey
    {
        LogicalFunction field;
        bool descending;

        friend bool operator==(const SortKey& lhs, const SortKey& rhs) = default;
    };

    TopKLogicalOperator(
        WeakLogicalOperator self,
        std::vector<LogicalFunction> partitionKeys,
        std::vector<SortKey> sortKeys,
        uint64_t limit,
        Windowing::TimeBasedWindowType windowType,
        Windowing::TimeCharacteristic timeCharacteristic);

    TopKLogicalOperator(
        WeakLogicalOperator self,
        LogicalOperator child,
        std::vector<LogicalFunction> partitionKeys,
        std::vector<SortKey> sortKeys,
        uint64_t limit,
        Windowing::TimeBasedWindowType windowType,
        Windowing::TimeCharacteristic timeCharacteristic);

    static TypedLogicalOperator<TopKLogicalOperator> create(
        std::vector<LogicalFunction> partitionKeys,
        std::vector<SortKey> sortKeys,
        uint64_t limit,
        Windowing::TimeBasedWindowType windowType,
        Windowing::TimeCharacteristic timeCharacteristic);

    static TypedLogicalOperator<TopKLogicalOperator> create(
        LogicalOperator child,
        std::vector<LogicalFunction> partitionKeys,
        std::vector<SortKey> sortKeys,
        uint64_t limit,
        Windowing::TimeBasedWindowType windowType,
        Windowing::TimeCharacteristic timeCharacteristic);

    [[nodiscard]] const std::vector<LogicalFunction>& getPartitionKeys() const;
    [[nodiscard]] const std::vector<SortKey>& getSortKeys() const;
    [[nodiscard]] uint64_t getLimit() const;
    [[nodiscard]] Windowing::TimeBasedWindowType getWindowType() const;
    [[nodiscard]] Windowing::TimeCharacteristic getCharacteristic() const;
    [[nodiscard]] const UnqualifiedUnboundField& getWindowStartField() const;
    [[nodiscard]] const UnqualifiedUnboundField& getWindowEndField() const;

    [[nodiscard]] bool operator==(const TopKLogicalOperator& rhs) const;

    [[nodiscard]] TopKLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] TopKLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] TopKLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;
    [[nodiscard]] Schema<Field, Ordered> getOrderedOutputSchema(ChildOutputOrderProvider orderProvider) const override;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] TopKLogicalOperator withInferredSchema() const;
    [[nodiscard]] const detail::DynamicBase* getDynamicBase() const;

    static constexpr std::string_view NAME = "TopK";

private:
    /// The fields the operator adds to the input fields: start and end of the window, or, for an UNBOUNDED window, the end of the
    /// emitting slice and the row kind.
    [[nodiscard]] std::vector<UnqualifiedUnboundField> getAddedFields() const;
    void inferLocalSchema();

    std::optional<LogicalOperator> child;
    std::vector<LogicalFunction> partitionKeys;
    std::vector<SortKey> sortKeys;
    uint64_t limit;
    Windowing::TimeBasedWindowType windowType;
    Windowing::TimeCharacteristic timestampField;
    UnqualifiedUnboundField startField{Identifier::parse("start"), DataType::Type::UINT64};
    UnqualifiedUnboundField endField{Identifier::parse("end"), DataType::Type::UINT64};
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;
    TraitSet traitSet;

    friend struct std::hash<TopKLogicalOperator>;
};

namespace detail
{
struct ReflectedTopKLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    std::vector<LogicalFunction> partitionKeys;
    std::vector<LogicalFunction> sortFields;
    std::vector<bool> descending;
    uint64_t limit{0};
    Windowing::TimeBasedWindowType windowType;
    Windowing::TimeCharacteristic timestampField;
};
}

template <>
struct Reflector<TypedLogicalOperator<TopKLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<TopKLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<TopKLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType plan);
    TypedLogicalOperator<TopKLogicalOperator> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<TopKLogicalOperator>);
}

template <>
struct std::hash<NES::TopKLogicalOperator>
{
    std::size_t operator()(const NES::TopKLogicalOperator& op) const noexcept;
};
